"""The one HTTP client used for every SEC request (doc section 11).

Returns raw bytes with no parsing. Rate limiting, retries and per-attempt logging live here so no
other module needs to think about them.
"""

from __future__ import annotations

import hashlib
import json
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_RETRY_AFTER_S = 60.0


class EdgarError(Exception):
    pass


class MissingUserAgent(EdgarError):
    """SEC requires a descriptive User-Agent; we refuse to send requests without one."""


class TerminalHTTPError(EdgarError):
    """A non-retryable client error (404 and friends). The caller decides how to continue."""

    def __init__(self, url: str, status: int) -> None:
        super().__init__(f"HTTP {status} for {url}")
        self.url = url
        self.status = status


class HaltRun(EdgarError):
    """403: bad User-Agent or rate violation. Continuing makes it worse, so stop everything."""


class RetriesExhausted(EdgarError):
    def __init__(self, url: str, attempts: int, last: str) -> None:
        super().__init__(f"gave up on {url} after {attempts} attempts: {last}")
        self.url = url
        self.attempts = attempts


@dataclass(frozen=True)
class Response:
    url: str
    status: int
    content: bytes
    headers: dict[str, str]
    sha256: str


class TokenBucket:
    """Spaces requests at `rate` per second. One instance must be shared per process."""

    def __init__(
        self,
        rate: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._interval = 1.0 / rate
        self._clock = clock
        self._sleep = sleep
        self._next_free = clock()

    def acquire(self) -> None:
        now = self._clock()
        wait = self._next_free - now
        if wait > 0:
            self._sleep(wait)
            now = self._next_free
        self._next_free = now + self._interval


def _utc_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class EdgarClient:
    def __init__(
        self,
        *,
        user_agent: str | None,
        run_id: str,
        rate_limit_per_sec: float = 5,
        max_attempts: int = 3,
        connect_timeout_s: float = 10,
        read_timeout_s: float = 60,
        log_sink: Callable[[str], None] | None = None,
        transport: httpx.BaseTransport | None = None,
        bucket: TokenBucket | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        if not user_agent or not user_agent.strip():
            raise MissingUserAgent("SEC_USER_AGENT is not set (use 'Name email@example.com')")
        self.run_id = run_id
        self.max_attempts = max_attempts
        self.retries = 0  # attempts beyond the first, for the run summary
        self.halted = False
        self._log_sink = log_sink
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._bucket = bucket or TokenBucket(rate_limit_per_sec, sleep=sleep)
        self._http = httpx.Client(
            headers={"User-Agent": user_agent.strip(), "Accept-Encoding": "gzip"},
            timeout=httpx.Timeout(read_timeout_s, connect=connect_timeout_s),
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> EdgarClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _log(
        self,
        url: str,
        attempt: int,
        status: int | None,
        latency_ms: int,
        nbytes: int,
        error_type: str | None,
    ) -> None:
        if self._log_sink is None:
            return
        self._log_sink(
            json.dumps(
                {
                    "run_id": self.run_id,
                    "url": url,
                    "attempt": attempt,
                    "status": status,
                    "latency_ms": latency_ms,
                    "bytes": nbytes,
                    "error_type": error_type,
                    "ts": _utc_iso(),
                },
                separators=(",", ":"),
            )
        )

    def _backoff(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(max(float(retry_after), 0.0), MAX_RETRY_AFTER_S)
            except ValueError:
                pass  # HTTP-date form: fall back to computed backoff
        return float(2 ** (attempt - 1)) * self._rng.uniform(0.5, 1.0)

    def get(self, url: str) -> Response:
        if self.halted:
            raise HaltRun("run halted by an earlier 403")
        last = "no attempts made"
        for attempt in range(1, self.max_attempts + 1):
            self._bucket.acquire()
            started = time.monotonic()
            retry_after: str | None = None
            try:
                resp = self._http.get(url)
            except (httpx.TimeoutException, httpx.TransportError) as e:
                latency = int((time.monotonic() - started) * 1000)
                self._log(url, attempt, None, latency, 0, type(e).__name__)
                last = type(e).__name__
            else:
                latency = int((time.monotonic() - started) * 1000)
                status = resp.status_code
                content = resp.content
                retryable = status in RETRYABLE_STATUS
                self._log(
                    url,
                    attempt,
                    status,
                    latency,
                    len(content),
                    f"http_{status}" if status != 200 else None,
                )
                if status == 200:
                    return Response(
                        url=url,
                        status=status,
                        content=content,
                        headers=dict(resp.headers),
                        sha256=hashlib.sha256(content).hexdigest(),
                    )
                if status == 403:
                    self.halted = True
                    raise HaltRun(f"HTTP 403 for {url}; check SEC_USER_AGENT and request rate")
                if not retryable:
                    raise TerminalHTTPError(url, status)
                last = f"HTTP {status}"
                retry_after = resp.headers.get("Retry-After")
            if attempt < self.max_attempts:
                self.retries += 1
                self._sleep(self._backoff(attempt, retry_after))
        raise RetriesExhausted(url, self.max_attempts, last)

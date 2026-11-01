import gzip
import hashlib
import json
import random

import httpx
import pytest

from sec_ingest.edgar_client import (
    EdgarClient,
    HaltRun,
    MissingUserAgent,
    RetriesExhausted,
    TerminalHTTPError,
    TokenBucket,
)

URL = "https://data.sec.gov/submissions/CIK0000320193.json"


class Harness:
    def __init__(self, responses: list[httpx.Response | Exception]) -> None:
        self._responses = list(responses)
        self.requests: list[httpx.Request] = []
        self.sleeps: list[float] = []
        self.log: list[dict[str, object]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def client(self, **kw: object) -> EdgarClient:
        return EdgarClient(
            user_agent="Test Suite test@example.com",
            run_id="run1",
            transport=httpx.MockTransport(self.handler),
            sleep=self.sleeps.append,
            rng=random.Random(0),
            log_sink=lambda line: self.log.append(json.loads(line)),
            bucket=TokenBucket(1000, sleep=lambda _: None),
            **kw,  # type: ignore[arg-type]
        )


def test_requires_user_agent() -> None:
    with pytest.raises(MissingUserAgent):
        EdgarClient(user_agent=None, run_id="r")
    with pytest.raises(MissingUserAgent):
        EdgarClient(user_agent="   ", run_id="r")


def test_success_returns_raw_bytes_hash_and_sends_user_agent() -> None:
    h = Harness([httpx.Response(200, content=b"abc")])
    r = h.client().get(URL)
    assert r.content == b"abc"
    assert r.sha256 == hashlib.sha256(b"abc").hexdigest()
    assert h.requests[0].headers["User-Agent"] == "Test Suite test@example.com"
    assert "gzip" in h.requests[0].headers["Accept-Encoding"]
    assert h.log[0]["status"] == 200 and h.log[0]["attempt"] == 1


def test_gzip_body_is_decoded_to_document_bytes() -> None:
    body = b'{"a":1}'
    h = Harness(
        [httpx.Response(200, content=gzip.compress(body), headers={"Content-Encoding": "gzip"})]
    )
    assert h.client().get(URL).content == body


def test_retries_5xx_then_succeeds() -> None:
    h = Harness([httpx.Response(503), httpx.Response(502), httpx.Response(200, content=b"ok")])
    c = h.client()
    assert c.get(URL).content == b"ok"
    assert len(h.requests) == 3
    assert len(h.sleeps) == 2
    assert 0.5 <= h.sleeps[0] <= 1.0  # attempt 1: 1s * jitter(0.5..1)
    assert 1.0 <= h.sleeps[1] <= 2.0  # attempt 2: 2s * jitter
    assert c.retries == 2
    assert [e["attempt"] for e in h.log] == [1, 2, 3]


def test_retries_exhausted() -> None:
    h = Harness([httpx.Response(500)] * 3)
    with pytest.raises(RetriesExhausted) as ei:
        h.client(max_attempts=3).get(URL)
    assert ei.value.attempts == 3
    assert len(h.requests) == 3
    assert len(h.sleeps) == 2  # no sleep after the final attempt


def test_retries_timeouts_and_connection_errors() -> None:
    h = Harness(
        [httpx.ReadTimeout("slow"), httpx.ConnectError("down"), httpx.Response(200, content=b"x")]
    )
    assert h.client().get(URL).content == b"x"
    assert [e["error_type"] for e in h.log] == ["ReadTimeout", "ConnectError", None]


def test_404_is_terminal_without_retry() -> None:
    h = Harness([httpx.Response(404)])
    with pytest.raises(TerminalHTTPError) as ei:
        h.client().get(URL)
    assert ei.value.status == 404
    assert len(h.requests) == 1
    assert h.sleeps == []


def test_403_halts_the_whole_client() -> None:
    h = Harness([httpx.Response(403)])
    c = h.client()
    with pytest.raises(HaltRun):
        c.get(URL)
    with pytest.raises(HaltRun):  # later calls refuse without touching the network
        c.get(URL)
    assert len(h.requests) == 1


def test_429_honors_retry_after_capped() -> None:
    h = Harness(
        [
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(429, headers={"Retry-After": "9999"}),
            httpx.Response(200, content=b"x"),
        ]
    )
    h.client().get(URL)
    assert h.sleeps == [7.0, 60.0]


def test_token_bucket_spaces_requests() -> None:
    now = [0.0]
    sleeps: list[float] = []

    def sleep(s: float) -> None:
        sleeps.append(s)
        now[0] += s

    b = TokenBucket(5, clock=lambda: now[0], sleep=sleep)
    for _ in range(4):
        b.acquire()
    assert sleeps == pytest.approx([0.2, 0.2, 0.2])  # first is free, then 5 req/s
    now[0] += 10  # idle time does not bank unlimited burst
    sleeps.clear()
    b.acquire()
    b.acquire()
    assert sleeps == pytest.approx([0.2])

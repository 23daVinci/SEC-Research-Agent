"""Shared test helpers: a routed fake EDGAR and submissions builders."""

from __future__ import annotations

import json
from typing import Any

import httpx

from sec_ingest.edgar_client import EdgarClient, TokenBucket


class FakeEdgar:
    """Serves canned bodies by URL and records every request."""

    def __init__(self, routes: dict[str, bytes | int | BaseException] | None = None) -> None:
        self.routes: dict[str, bytes | int | BaseException] = routes or {}
        self.requested: list[str] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.requested.append(url)
        item = self.routes.get(url, 404)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, int):
            return httpx.Response(item)
        return httpx.Response(200, content=item)

    def client(self, run_id: str = "run1", max_attempts: int = 3) -> EdgarClient:
        return EdgarClient(
            user_agent="Test Suite test@example.com",
            run_id=run_id,
            max_attempts=max_attempts,
            transport=httpx.MockTransport(self.handler),
            sleep=lambda _: None,
            bucket=TokenBucket(1000, sleep=lambda _: None),
        )


def ticker_map(*entries: tuple[int, str, str]) -> bytes:
    return json.dumps(
        {
            str(i): {"cik_str": cik, "ticker": t, "title": title}
            for i, (cik, t, title) in enumerate(entries)
        }
    ).encode()


def filings_block(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    keys = ("accessionNumber", "form", "filingDate", "reportDate", "primaryDocument", "items")
    return {k: [r.get(k, "") for r in rows] for k in keys}


def submissions(rows: list[dict[str, Any]], files: list[dict[str, Any]] | None = None) -> bytes:
    return json.dumps({"filings": {"recent": filings_block(rows), "files": files or []}}).encode()


def filing(acc: str, form: str, filed: str, doc: str = "d.htm", **kw: Any) -> dict[str, Any]:
    return {
        "accessionNumber": acc,
        "form": form,
        "filingDate": filed,
        "reportDate": kw.pop("reportDate", ""),
        "primaryDocument": doc,
        **kw,
    }

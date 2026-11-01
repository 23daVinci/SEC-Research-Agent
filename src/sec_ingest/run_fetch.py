"""Execute a written plan: filings, companyfacts, summary (doc section 9)."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from . import layout
from .blobstore import BlobStore
from .edgar_client import EdgarClient, EdgarError
from .exhibits import ExhibitSelector, select_ex99
from .fetch_filing import FilingOutcome, fetch_filing
from .ledger import Ledger


def fetch_filings(
    *,
    client: EdgarClient,
    store: BlobStore,
    ledger: Ledger,
    plan_lines: Sequence[Mapping[str, Any]],
    run_id: str,
    exhibit_selector: ExhibitSelector = select_ex99,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[FilingOutcome]:
    """Process `fetch` decisions sequentially. One process means one rate limiter (doc section 9).

    Per-filing failures are collected, not raised; the caller fails the task afterwards.
    """
    return [
        fetch_filing(
            client=client,
            store=store,
            ledger=ledger,
            line=line,
            run_id=run_id,
            exhibit_selector=exhibit_selector,
            clock=clock,
        )
        for line in plan_lines
        if line["decision"] == "fetch"
    ]


def fetch_companyfacts(
    *,
    client: EdgarClient,
    store: BlobStore,
    ledger: Ledger,
    ciks: Sequence[str],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, str | None]:
    """Snapshot companyfacts per CIK. Returns {cik: error or None}."""
    results: dict[str, str | None] = {}
    for cik in ciks:
        try:
            resp = client.get(layout.companyfacts_url(cik))
            json.loads(resp.content)  # must parse, but we store the raw bytes untouched
        except (EdgarError, ValueError) as e:
            results[cik] = f"{type(e).__name__}: {e}"
            continue
        moment = clock()
        key = layout.companyfacts_key(cik, moment)
        store.put_bytes(key, resp.content)
        ledger.record_companyfacts_snapshot(cik, fetched_at=moment, path=key, sha256=resp.sha256)
        results[cik] = None
    return results


def write_fetch_summary(
    store: BlobStore,
    *,
    run_id: str,
    outcomes: Sequence[FilingOutcome],
    companyfacts: Mapping[str, str | None],
    retries: int,
    started_monotonic: float,
    note: str | None = None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "committed": sum(o.state == "committed" for o in outcomes),
        "skipped": sum(o.state == "skipped" for o in outcomes),
        "failed": sum(o.state == "failed" for o in outcomes),
        "bytes_written": sum(o.bytes_written for o in outcomes),
        "retries": retries,
        "wall_time_s": round(time.monotonic() - started_monotonic, 2),
        "companyfacts_ok": sorted(c for c, e in companyfacts.items() if e is None),
        "failures": [
            {"accession": o.accession, "error": o.error} for o in outcomes if o.state == "failed"
        ]
        + [{"companyfacts_cik": c, "error": e} for c, e in companyfacts.items() if e is not None],
        "note": note,
    }
    store.put_bytes(
        layout.fetch_summary_key(run_id),
        (json.dumps(summary, indent=2) + "\n").encode(),
        overwrite=True,
    )
    return summary

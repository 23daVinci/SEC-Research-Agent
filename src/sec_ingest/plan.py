"""Build, write and read the immutable fetch plan (doc section 8).

Planning only reads the ledger. Rows are marked `planned` by the fetch step.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from . import layout
from .blobstore import BlobStore
from .edgar_client import EdgarClient
from .ledger import Ledger
from .selection import (
    REASON_ALREADY_COMMITTED,
    REASON_AMENDMENT,
    REASON_LATEST_10K,
    REASON_LATEST_10Q,
    FilingRow,
    MalformedSubmissions,
    PlanDecision,
    diff_against_ledger,
    needs_history,
    select_candidates,
    zip_filings,
)

SCHEMA_VERSION = 1


class TickerResolutionError(Exception):
    pass


class PlanNotFound(Exception):
    """No complete plan exists for this run_id (the plan job did not finish)."""


def new_run_id(now: datetime) -> str:
    return layout.ts_for_key(now)


def resolve_ciks(ticker_map: Mapping[str, Any], tickers: Sequence[str]) -> dict[str, str]:
    """Map each configured ticker to exactly one CIK, or fail listing every problem."""
    found: dict[str, set[str]] = {}
    for entry in ticker_map.values():
        found.setdefault(str(entry["ticker"]).upper(), set()).add(layout.cik10(entry["cik_str"]))
    problems = [
        f"{t}: {'not found' if t not in found else f'ambiguous {sorted(found[t])}'}"
        for t in tickers
        if len(found.get(t, ())) != 1
    ]
    if problems:
        raise TickerResolutionError("; ".join(problems))
    return {t: next(iter(found[t])) for t in tickers}


def company_names(ticker_map: Mapping[str, Any]) -> dict[str, str]:
    return {layout.cik10(e["cik_str"]): str(e["title"]) for e in ticker_map.values()}


@dataclass(frozen=True)
class PlanResult:
    run_id: str
    decisions: list[PlanDecision]
    summary: dict[str, Any]


def _parse_submissions(raw: bytes) -> tuple[list[FilingRow], list[dict[str, Any]]]:
    try:
        data = json.loads(raw)
        filings = data["filings"]
        return zip_filings(filings["recent"]), list(filings.get("files", []))
    except (KeyError, TypeError, json.JSONDecodeError) as e:
        raise MalformedSubmissions(f"unexpected submissions shape: {e!r}") from e


def build_plan(
    *,
    client: EdgarClient,
    store: BlobStore,
    ledger: Ledger,
    tickers: Sequence[str],
    run_id: str,
    as_of: date,
    window_months: int,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PlanResult:
    # 1-2. snapshot the ticker map, resolve every ticker (all-or-nothing)
    tm = client.get(layout.TICKER_MAP_URL)
    store.put_bytes(layout.ticker_map_key(clock()), tm.content)
    ticker_map = json.loads(tm.content)
    cik_by_ticker = resolve_ciks(ticker_map, tickers)
    names = company_names(ticker_map)

    committed = ledger.committed_accessions()
    decisions: list[PlanDecision] = []
    coverage: dict[str, int] = {}
    exceptions: dict[str, list[str]] = {}
    row_extras: dict[str, dict[str, Any]] = {}

    for ticker, cik in cik_by_ticker.items():
        # 3. submissions snapshot, then 4. the gap guard: follow history until covered
        sub = client.get(layout.submissions_url(cik))
        store.put_bytes(layout.submissions_key(cik, clock()), sub.content)
        rows, files = _parse_submissions(sub.content)
        remaining = sorted(files, key=lambda f: f.get("filingTo", ""), reverse=True)
        followed = 0
        while needs_history(
            rows, as_of=as_of, window_months=window_months, has_history_files=bool(remaining)
        ):
            name = remaining.pop(0)["name"]
            hist = client.get(layout.submissions_history_url(name))
            store.put_bytes(layout.submissions_history_key(cik, clock(), name), hist.content)
            try:
                rows += zip_filings(json.loads(hist.content))
            except (TypeError, json.JSONDecodeError) as e:
                raise MalformedSubmissions(f"{name}: {e!r}") from e
            followed += 1
        coverage[cik] = followed

        missing = [f for f in ("10-K", "10-Q") if not any(r.form == f for r in rows)]
        if missing:
            exceptions[cik] = [f"no_{f.lower().replace('-', '')}" for f in missing]

        # 5-6. select, then diff against the ledger
        picked = select_candidates(cik, ticker, rows, as_of=as_of, window_months=window_months)
        decisions += diff_against_ledger(picked, committed)
        row_extras[cik] = {"company_name": names.get(cik)}

    refresh = sorted(
        {
            d.cik
            for d in decisions
            if d.decision == "fetch" and d.reason in (REASON_LATEST_10K, REASON_LATEST_10Q)
        }
        | {c for c in cik_by_ticker.values() if ledger.companyfacts_snapshot_count(c) == 0}
    )
    to_fetch = sum(d.decision == "fetch" for d in decisions)
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "as_of": as_of.isoformat(),
        "tickers": list(cik_by_ticker),
        "ciks_planned": len(cik_by_ticker),
        "to_fetch": to_fetch,
        "skipped_already_committed": sum(d.reason == REASON_ALREADY_COMMITTED for d in decisions),
        "rejected": sum(d.reason == REASON_AMENDMENT for d in decisions),
        "companyfacts_refresh": refresh,
        "coverage_history_files_followed": coverage,
        "exceptions": exceptions,
        "empty": to_fetch == 0 and not refresh,
    }
    write_plan(store, run_id, decisions, summary, row_extras)
    return PlanResult(run_id, decisions, summary)


def _line(run_id: str, as_of: str, d: PlanDecision, extras: Mapping[str, Mapping[str, Any]]) -> str:
    r = d.row
    return json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "as_of": as_of,
            "cik": d.cik,
            "ticker": d.ticker,
            "company_name": extras.get(d.cik, {}).get("company_name"),
            "accession": r.accession,
            "form": r.form,
            "filing_date": r.filing_date.isoformat(),
            "report_date": r.report_date.isoformat() if r.report_date else None,
            "primary_document": r.primary_document,
            "items": r.items,
            "accepted_at": r.accepted_at,
            "is_inline_xbrl": r.is_inline_xbrl,
            "decision": d.decision,
            "reason": d.reason,
        },
        separators=(",", ":"),
    )


def write_plan(
    store: BlobStore,
    run_id: str,
    decisions: Sequence[PlanDecision],
    summary: Mapping[str, Any],
    extras: Mapping[str, Mapping[str, Any]],
) -> None:
    """Always writes both files, even for an empty plan.

    The summary goes last: its presence marks the plan complete.
    """
    body = "".join(_line(run_id, summary["as_of"], d, extras) + "\n" for d in decisions)
    store.put_bytes(layout.plan_key(run_id), body.encode())
    store.put_bytes(
        layout.plan_summary_key(run_id), (json.dumps(summary, indent=2) + "\n").encode()
    )


def read_plan(store: BlobStore, run_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not (
        store.exists(layout.plan_key(run_id)) and store.exists(layout.plan_summary_key(run_id))
    ):
        raise PlanNotFound(f"no complete plan for run_id={run_id}")
    summary = json.loads(store.get_bytes(layout.plan_summary_key(run_id)))
    lines = store.get_bytes(layout.plan_key(run_id)).decode().splitlines()
    return [json.loads(line) for line in lines if line.strip()], summary

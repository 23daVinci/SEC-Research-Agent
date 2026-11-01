"""Pure functions: zip submissions arrays, apply selection rules, diff against the ledger.

No I/O, no clock. Everything takes its inputs as arguments so it is trivially testable.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Literal

from dateutil.relativedelta import relativedelta

Decision = Literal["fetch", "skip", "reject"]

REASON_LATEST_10K = "latest_10k"
REASON_LATEST_10Q = "latest_10q"
REASON_8K_WINDOW = "8k_window"
REASON_AMENDMENT = "amendment"
REASON_ALREADY_COMMITTED = "already_committed"

TRACKED_FORMS = ("10-K", "10-Q", "8-K")
AMENDMENT_FORMS = frozenset(f"{f}/A" for f in TRACKED_FORMS)

_ARRAY_KEYS = ("accessionNumber", "form", "filingDate", "reportDate", "primaryDocument")


class MalformedSubmissions(ValueError):
    pass


@dataclass(frozen=True)
class FilingRow:
    accession: str
    form: str
    filing_date: date
    report_date: date | None
    primary_document: str
    items: str | None = None
    accepted_at: str | None = None
    is_inline_xbrl: bool | None = None


@dataclass(frozen=True)
class PlanDecision:
    cik: str
    ticker: str
    row: FilingRow
    decision: Decision
    reason: str


def _opt_date(value: Any) -> date | None:
    return date.fromisoformat(value) if value else None


def zip_filings(block: Mapping[str, Sequence[Any]]) -> list[FilingRow]:
    """Turn EDGAR's parallel arrays into row objects.

    Works for `filings.recent` and for the paginated history files, which share the shape.
    """
    missing = [k for k in _ARRAY_KEYS if k not in block]
    if missing:
        raise MalformedSubmissions(f"missing arrays: {missing}")
    n = len(block["accessionNumber"])
    lengths = {k: len(v) for k, v in block.items() if isinstance(v, list)}
    if any(length != n for length in lengths.values()):
        raise MalformedSubmissions(f"parallel arrays differ in length: {lengths}")

    def col(key: str, i: int) -> Any:
        values = block.get(key)
        return values[i] if values is not None else None

    rows = []
    for i in range(n):
        inline = col("isInlineXBRL", i)
        rows.append(
            FilingRow(
                accession=block["accessionNumber"][i],
                form=block["form"][i],
                filing_date=date.fromisoformat(block["filingDate"][i]),
                report_date=_opt_date(block["reportDate"][i]),
                primary_document=block["primaryDocument"][i],
                items=col("items", i) or None,
                accepted_at=col("acceptanceDateTime", i) or None,
                is_inline_xbrl=None if inline is None else bool(inline),
            )
        )
    return rows


def window_start(as_of: date, months: int) -> date:
    return as_of - relativedelta(months=months)


def _sort_key(row: FilingRow) -> tuple[date, str, str]:
    # Deterministic tie-break when two filings share a date.
    return (row.filing_date, row.accepted_at or "", row.accession)


def select_candidates(
    cik: str, ticker: str, rows: Sequence[FilingRow], *, as_of: date, window_months: int
) -> list[PlanDecision]:
    """Apply the selection rules (doc section 8). Returns `fetch` and `reject` decisions."""
    start = window_start(as_of, window_months)
    out: list[PlanDecision] = []

    def decide(row: FilingRow, decision: Decision, reason: str) -> None:
        out.append(PlanDecision(cik, ticker, row, decision, reason))

    for form, reason in (("10-K", REASON_LATEST_10K), ("10-Q", REASON_LATEST_10Q)):
        matching = [r for r in rows if r.form == form]
        if matching:
            decide(max(matching, key=_sort_key), "fetch", reason)

    for row in sorted(rows, key=_sort_key, reverse=True):
        if row.form == "8-K" and start <= row.filing_date <= as_of:
            decide(row, "fetch", REASON_8K_WINDOW)
        elif row.form in AMENDMENT_FORMS and row.filing_date >= start:
            # Only recent amendments are logged; older ones would just be noise in the plan.
            decide(row, "reject", REASON_AMENDMENT)
    return out


def needs_history(
    rows: Sequence[FilingRow], *, as_of: date, window_months: int, has_history_files: bool
) -> bool:
    """True when `recent` may not cover everything we need (the gap guard, doc section 8).

    If the submissions response references no older files, `recent` is the whole history and
    there is no gap. Otherwise we need older files when `recent` does not reach back past the
    8-K window start, or when a 10-K / 10-Q is missing from it.
    """
    if not has_history_files:
        return False
    if not rows:
        return True
    oldest = min(r.filing_date for r in rows)
    forms = {r.form for r in rows}
    return oldest > window_start(as_of, window_months) or not {"10-K", "10-Q"} <= forms


def diff_against_ledger(
    decisions: Sequence[PlanDecision], committed: Collection[str]
) -> list[PlanDecision]:
    """Turn already-committed fetches into skips. `planned` and `failed` stay `fetch` (retried)."""
    return [
        replace(d, decision="skip", reason=REASON_ALREADY_COMMITTED)
        if d.decision == "fetch" and d.row.accession in committed
        else d
        for d in decisions
    ]

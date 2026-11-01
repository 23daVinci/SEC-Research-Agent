from datetime import date

import pytest

from sec_ingest import selection as sel
from sec_ingest.selection import FilingRow, PlanDecision

CIK, TICKER = "0000320193", "AAPL"
AS_OF = date(2026, 10, 30)


def row(acc: str, form: str, filed: str, **kw: object) -> FilingRow:
    return FilingRow(
        accession=acc,
        form=form,
        filing_date=date.fromisoformat(filed),
        report_date=None,
        primary_document="doc.htm",
        **kw,  # type: ignore[arg-type]
    )


def select(rows: list[FilingRow]) -> list[PlanDecision]:
    return sel.select_candidates(CIK, TICKER, rows, as_of=AS_OF, window_months=6)


def by_reason(ds: list[PlanDecision], reason: str) -> list[str]:
    return [d.row.accession for d in ds if d.reason == reason]


def test_zip_filings_builds_rows_and_normalizes_blanks() -> None:
    block = {
        "accessionNumber": ["a-1", "a-2"],
        "form": ["10-K", "8-K"],
        "filingDate": ["2026-01-02", "2026-02-03"],
        "reportDate": ["2025-12-31", ""],
        "primaryDocument": ["k.htm", "e.htm"],
        "items": ["", "2.02,9.01"],
        "isInlineXBRL": [1, 0],
        "acceptanceDateTime": ["2026-01-02T16:30:42.000Z", ""],
    }
    rows = sel.zip_filings(block)
    assert rows[0].report_date == date(2025, 12, 31)
    assert rows[0].items is None and rows[1].items == "2.02,9.01"
    assert rows[0].is_inline_xbrl is True and rows[1].is_inline_xbrl is False
    assert rows[1].report_date is None and rows[1].accepted_at is None


def test_zip_filings_empty_recent() -> None:
    empty = {k: [] for k in sel._ARRAY_KEYS}
    assert sel.zip_filings(empty) == []


def test_zip_filings_rejects_ragged_or_missing_arrays() -> None:
    ragged = {k: ["x"] for k in sel._ARRAY_KEYS} | {"form": []}
    with pytest.raises(sel.MalformedSubmissions, match="differ"):
        sel.zip_filings(ragged)
    with pytest.raises(sel.MalformedSubmissions, match="missing"):
        sel.zip_filings({"accessionNumber": []})


def test_latest_10k_and_10q_by_filing_date_not_list_order() -> None:
    rows = [
        row("k-old", "10-K", "2025-11-01"),
        row("k-new", "10-K", "2026-10-01"),
        row("q-old", "10-Q", "2026-02-01"),
        row("q-new", "10-Q", "2026-08-01"),
    ]
    ds = select(rows)
    assert by_reason(ds, sel.REASON_LATEST_10K) == ["k-new"]
    assert by_reason(ds, sel.REASON_LATEST_10Q) == ["q-new"]


def test_amendments_excluded_and_recorded_as_rejects() -> None:
    rows = [
        row("k", "10-K", "2026-01-10"),
        row("ka", "10-K/A", "2026-09-01"),  # newer than the 10-K, must not win
        row("qa", "10-Q/A", "2026-09-02"),
    ]
    ds = select(rows)
    assert by_reason(ds, sel.REASON_LATEST_10K) == ["k"]
    assert by_reason(ds, sel.REASON_LATEST_10Q) == []
    assert {d.row.accession for d in ds if d.decision == "reject"} == {"ka", "qa"}
    assert all(d.reason == sel.REASON_AMENDMENT for d in ds if d.decision == "reject")


def test_8k_window_boundary_is_exactly_six_calendar_months() -> None:
    rows = [
        row("on-start", "8-K", "2026-04-30"),  # as_of - 6 months: included
        row("day-before", "8-K", "2026-04-29"),  # excluded
        row("today", "8-K", "2026-10-30"),
    ]
    assert sorted(by_reason(select(rows), sel.REASON_8K_WINDOW)) == ["on-start", "today"]


def test_window_start_clamps_month_end() -> None:
    assert sel.window_start(date(2026, 8, 31), 6) == date(2026, 2, 28)


def test_company_with_no_10q_yet() -> None:
    ds = select([row("k", "10-K", "2026-01-10")])
    assert [d.reason for d in ds] == [sel.REASON_LATEST_10K]


def test_empty_recent_yields_no_decisions() -> None:
    assert select([]) == []


def test_same_date_tie_is_deterministic() -> None:
    rows = [row("a-1", "10-K", "2026-01-10"), row("a-2", "10-K", "2026-01-10")]
    assert by_reason(select(rows), sel.REASON_LATEST_10K) == ["a-2"]
    assert by_reason(select(rows[::-1]), sel.REASON_LATEST_10K) == ["a-2"]


def test_other_forms_ignored() -> None:
    assert select([row("x", "4", "2026-10-01"), row("y", "10-KT", "2026-10-01")]) == []


def test_diff_skips_committed_but_retries_planned_and_failed() -> None:
    ds = select(
        [
            row("k", "10-K", "2026-01-10"),
            row("q", "10-Q", "2026-08-01"),
            row("e", "8-K", "2026-09-01"),
        ]
    )
    out = sel.diff_against_ledger(ds, committed={"k"})
    got = {d.row.accession: (d.decision, d.reason) for d in out}
    assert got["k"] == ("skip", sel.REASON_ALREADY_COMMITTED)
    assert got["q"][0] == "fetch" and got["e"][0] == "fetch"


def test_diff_leaves_rejects_alone() -> None:
    ds = select([row("ka", "10-K/A", "2026-09-01")])
    assert sel.diff_against_ledger(ds, committed={"ka"}) == ds


@pytest.mark.parametrize(
    ("rows", "has_files", "expected"),
    [
        ([], False, False),  # recent is everything and it is empty: nothing to follow
        ([], True, True),
        (
            [row("k", "10-K", "2025-01-01"), row("q", "10-Q", "2025-02-01")],
            True,
            False,
        ),  # reaches past window
        (
            [row("k", "10-K", "2026-09-01"), row("q", "10-Q", "2026-09-02")],
            True,
            True,
        ),  # window may continue
        (
            [row("k", "10-K", "2026-09-01"), row("q", "10-Q", "2026-09-02")],
            False,
            False,
        ),  # no older files
        ([row("k", "10-K", "2025-01-01")], True, True),  # reaches back but no 10-Q in recent
    ],
)
def test_needs_history(rows: list[FilingRow], has_files: bool, expected: bool) -> None:
    assert (
        sel.needs_history(rows, as_of=AS_OF, window_months=6, has_history_files=has_files)
        is expected
    )


def test_real_submissions_response_shape() -> None:
    """Real AAPL submissions JSON saved by spike 1: parallel arrays incl. optional columns."""
    import json
    from pathlib import Path

    raw = json.loads((Path(__file__).parent / "fixtures/spike1/submissions_AAPL.json").read_bytes())
    recent = raw["filings"]["recent"]
    rows = sel.zip_filings(recent)
    assert len(rows) == len(recent["accessionNumber"]) > 100
    assert any(r.is_inline_xbrl for r in rows) and any(r.accepted_at for r in rows)

    ds = select(rows)
    fetched = {d.reason: d.row for d in ds if d.decision == "fetch" and d.reason != "8k_window"}
    assert fetched["latest_10k"].accession == "0000320193-25-000079"
    assert fetched["latest_10q"].accession == "0000320193-26-000020"
    window = [d.row for d in ds if d.reason == "8k_window"]
    assert window and all(r.form == "8-K" and r.filing_date >= date(2026, 4, 30) for r in window)

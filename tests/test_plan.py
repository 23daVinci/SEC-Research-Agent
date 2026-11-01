import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from sec_ingest import layout, plan
from sec_ingest.blobstore import ImmutabilityViolation, LocalBlobStore
from sec_ingest.edgar_client import TerminalHTTPError
from sec_ingest.ledger import Ledger
from sec_ingest.selection import FilingRow

from .helpers import FakeEdgar, filing, submissions, ticker_map

NOW = datetime(2026, 10, 30, 14, 0, 0, tzinfo=UTC)
AS_OF = date(2026, 10, 30)
RUN = plan.new_run_id(NOW)
AAPL = "0000320193"


@pytest.fixture
def store(tmp_path: Path) -> LocalBlobStore:
    return LocalBlobStore(tmp_path)


@pytest.fixture
def ledger(tmp_path: Path) -> Ledger:
    return Ledger(tmp_path / "ledger.sqlite")


def run_plan(
    edgar: FakeEdgar, store: LocalBlobStore, ledger: Ledger, tickers: list[str]
) -> plan.PlanResult:
    return plan.build_plan(
        client=edgar.client(RUN),
        store=store,
        ledger=ledger,
        tickers=tickers,
        run_id=RUN,
        as_of=AS_OF,
        window_months=6,
        clock=lambda: NOW,
    )


def aapl_edgar(
    rows: list[dict[str, object]], files: list[dict[str, object]] | None = None
) -> FakeEdgar:
    return FakeEdgar(
        {
            layout.TICKER_MAP_URL: ticker_map((320193, "AAPL", "Apple Inc.")),
            layout.submissions_url(AAPL): submissions(rows, files),  # type: ignore[arg-type]
        }
    )


BASE_ROWS = [
    filing("0000320193-25-000079", "10-K", "2025-10-31"),
    filing("0000320193-26-000020", "10-Q", "2026-07-31", reportDate="2026-06-27"),
    filing("0000320193-26-000030", "8-K", "2026-09-01", items="2.02"),
    filing("0000320193-24-000123", "10-K/A", "2026-08-15"),
]


def test_resolve_ciks_ok_missing_and_ambiguous() -> None:
    tm = json.loads(
        ticker_map((1, "AAA", "A"), (2, "BBB", "B"), (3, "BBB", "B again"), (1, "AAA", "A dup"))
    )
    assert plan.resolve_ciks(tm, ["AAA"]) == {"AAA": "0000000001"}  # same-CIK duplicates are fine
    with pytest.raises(plan.TickerResolutionError) as ei:
        plan.resolve_ciks(tm, ["AAA", "BBB", "ZZZ"])
    assert "BBB: ambiguous" in str(ei.value) and "ZZZ: not found" in str(ei.value)


def test_plan_writes_snapshots_plan_and_summary(store: LocalBlobStore, ledger: Ledger) -> None:
    result = run_plan(aapl_edgar(BASE_ROWS), store, ledger, ["AAPL"])

    assert store.exists(layout.ticker_map_key(NOW))
    assert store.exists(layout.submissions_key(AAPL, NOW))
    lines, summary = plan.read_plan(store, RUN)
    by_acc = {ln["accession"]: ln for ln in lines}
    assert by_acc["0000320193-25-000079"]["reason"] == "latest_10k"
    assert by_acc["0000320193-26-000020"]["reason"] == "latest_10q"
    assert by_acc["0000320193-26-000030"]["reason"] == "8k_window"
    assert by_acc["0000320193-24-000123"]["decision"] == "reject"
    assert by_acc["0000320193-26-000020"]["company_name"] == "Apple Inc."
    assert summary["to_fetch"] == 3 and summary["rejected"] == 1 and summary["empty"] is False
    assert summary["companyfacts_refresh"] == [AAPL]
    assert result.summary == summary


def test_already_committed_is_skipped_and_empty_plan_is_explicit(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    for acc, form, filed in [
        ("0000320193-25-000079", "10-K", "2025-10-31"),
        ("0000320193-26-000020", "10-Q", "2026-07-31"),
        ("0000320193-26-000030", "8-K", "2026-09-01"),
    ]:
        r = FilingRow(acc, form, date.fromisoformat(filed), None, "d.htm")
        ledger.mark_planned(r, cik=AAPL, ticker="AAPL", run_id="old")
        ledger.mark_committed(acc, run_id="old", at=NOW)
    ledger.record_companyfacts_snapshot(AAPL, fetched_at=NOW, path="p", sha256="h")

    run_plan(aapl_edgar(BASE_ROWS), store, ledger, ["AAPL"])
    lines, summary = plan.read_plan(store, RUN)
    assert summary["to_fetch"] == 0 and summary["skipped_already_committed"] == 3
    assert summary["companyfacts_refresh"] == []
    assert summary["empty"] is True  # explicit, and plan files exist
    assert any(ln["decision"] == "skip" for ln in lines)


def test_failed_filing_is_retried_in_next_plan(store: LocalBlobStore, ledger: Ledger) -> None:
    r = FilingRow("0000320193-25-000079", "10-K", date(2025, 10, 31), None, "d.htm")
    ledger.mark_planned(r, cik=AAPL, ticker="AAPL", run_id="old")
    ledger.mark_failed(r.accession, "boom")
    result = run_plan(aapl_edgar(BASE_ROWS), store, ledger, ["AAPL"])
    d = next(d for d in result.decisions if d.row.accession == r.accession)
    assert d.decision == "fetch"


def test_empty_recent_is_still_written_and_flags_missing_forms(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    run_plan(aapl_edgar([]), store, ledger, ["AAPL"])
    lines, summary = plan.read_plan(store, RUN)
    assert lines == []
    assert summary["exceptions"] == {AAPL: ["no_10k", "no_10q"]}
    assert summary["to_fetch"] == 0
    assert summary["companyfacts_refresh"] == [AAPL]  # no snapshot yet
    assert summary["empty"] is False


def test_gap_guard_follows_history_files(store: LocalBlobStore, ledger: Ledger) -> None:
    recent = [
        filing("a-10q", "10-Q", "2026-09-30"),
        filing("a-10k", "10-K", "2026-09-01"),
        filing("a-8k-new", "8-K", "2026-09-15"),
    ]
    older = [filing("a-8k-mid", "8-K", "2026-06-01"), filing("a-8k-old", "8-K", "2026-03-01")]
    files = [{"name": "CIK0000320193-submissions-001.json", "filingTo": "2026-08-30"}]
    edgar = aapl_edgar(recent, files)
    edgar.routes[layout.submissions_history_url("CIK0000320193-submissions-001.json")] = json.dumps(
        {
            k: [r.get(k, "") for r in older]
            for k in ("accessionNumber", "form", "filingDate", "reportDate", "primaryDocument")
        }
    ).encode()

    result = run_plan(edgar, store, ledger, ["AAPL"])

    window = {d.row.accession for d in result.decisions if d.reason == "8k_window"}
    assert window == {"a-8k-new", "a-8k-mid"}  # old one is outside the 6 month window
    assert result.summary["coverage_history_files_followed"] == {AAPL: 1}
    assert any("submissions-001.json" in k for k in store.list("bronze/submissions"))


def test_gap_guard_fails_when_history_file_unavailable(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    recent = [filing("a-10q", "10-Q", "2026-09-30"), filing("a-10k", "10-K", "2026-09-01")]
    edgar = aapl_edgar(recent, [{"name": "missing.json", "filingTo": "2026-08-30"}])
    with pytest.raises(TerminalHTTPError):
        run_plan(edgar, store, ledger, ["AAPL"])
    with pytest.raises(plan.PlanNotFound):  # no plan written, so DAG 2 cannot run
        plan.read_plan(store, RUN)


def test_unresolvable_ticker_fails_before_any_plan(store: LocalBlobStore, ledger: Ledger) -> None:
    with pytest.raises(plan.TickerResolutionError):
        run_plan(aapl_edgar(BASE_ROWS), store, ledger, ["AAPL", "NOPE"])
    with pytest.raises(plan.PlanNotFound):
        plan.read_plan(store, RUN)


def test_plan_is_immutable_for_a_run_id(store: LocalBlobStore, ledger: Ledger) -> None:
    run_plan(aapl_edgar(BASE_ROWS), store, ledger, ["AAPL"])
    run_plan(aapl_edgar(BASE_ROWS), store, ledger, ["AAPL"])  # identical re-run is a no-op
    changed = [*BASE_ROWS, filing("0000320193-26-000031", "8-K", "2026-09-02")]
    with pytest.raises(ImmutabilityViolation):
        run_plan(aapl_edgar(changed), store, ledger, ["AAPL"])


def test_read_plan_requires_both_files(store: LocalBlobStore) -> None:
    store.put_bytes(layout.plan_key(RUN), b"")
    with pytest.raises(plan.PlanNotFound):  # summary is the completion marker
        plan.read_plan(store, RUN)

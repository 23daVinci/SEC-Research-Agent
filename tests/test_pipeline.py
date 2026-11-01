import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sec_ingest import layout, pipeline, plan
from sec_ingest.config import Config
from sec_ingest.pipeline import FetchFailed, Runtime, open_runtime

from .helpers import FakeEdgar, filing, submissions, ticker_map

CIK = "0000320193"
K, Q, E = "0000320193-25-000079", "0000320193-26-000020", "0000320193-26-000030"


def make_edgar() -> FakeEdgar:
    routes: dict[str, bytes | int | BaseException] = {
        layout.TICKER_MAP_URL: ticker_map((320193, "AAPL", "Apple Inc.")),
        layout.submissions_url(CIK): submissions(
            [filing(K, "10-K", "2025-10-31", "k.htm"), filing(Q, "10-Q", "2026-07-31", "q.htm")]
        ),
        layout.companyfacts_url(CIK): json.dumps(
            {"facts": {"us-gaap": {"R": {"units": {"USD": [{"accn": K}, {"accn": Q}]}}}}}
        ).encode(),
    }
    for acc, doc in ((K, "k.htm"), (Q, "q.htm")):
        base = layout.filing_base_url(CIK, acc)
        routes[f"{base}/index.json"] = b"{}"
        routes[f"{base}/{doc}"] = b"<html/>"
    return FakeEdgar(routes)


@pytest.fixture
def rt(tmp_path: Path) -> Runtime:
    cfg = Config(("AAPL",), 6, 5, 3, 10, 60, tmp_path, "Test test@example.com")
    return open_runtime(cfg)


def wire(rt: Runtime, edgar: FakeEdgar, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Runtime, "client", lambda self, run_id: edgar.client(run_id))


def test_plan_fetch_validate_then_second_run_is_empty(
    rt: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    edgar = make_edgar()
    wire(rt, edgar, monkeypatch)
    t1 = datetime(2026, 10, 30, 14, 0, 0, tzinfo=UTC)

    result = pipeline.run_plan_stage(rt, clock=lambda: t1)
    summary, report = pipeline.run_all_fetch(rt, result.run_id)
    assert report.ok, report.errors
    assert (summary["committed"], summary["failed"], summary["skipped"]) == (2, 0, 0)
    assert summary["companyfacts_ok"] == [CIK] and summary["bytes_written"] > 0

    # Re-plan an hour later: nothing new, plan is explicitly empty, fetch makes no document calls.
    t2 = t1 + timedelta(hours=1)
    edgar.requested.clear()
    second = pipeline.run_plan_stage(rt, clock=lambda: t2)
    assert second.summary["empty"] is True and second.summary["skipped_already_committed"] == 2
    edgar.requested.clear()
    summary2, report2 = pipeline.run_all_fetch(rt, second.run_id)
    assert edgar.requested == []
    assert report2.ok and summary2["committed"] == 0 and "plan was empty" in summary2["note"]


def test_failure_still_writes_summary_and_raises(
    rt: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    edgar = make_edgar()
    edgar.routes[f"{layout.filing_base_url(CIK, Q)}/q.htm"] = 404
    wire(rt, edgar, monkeypatch)
    run_id = pipeline.run_plan_stage(rt, clock=lambda: datetime(2026, 10, 30, tzinfo=UTC)).run_id

    with pytest.raises(FetchFailed):
        pipeline.run_all_fetch(rt, run_id)

    summary = json.loads(rt.store.get_bytes(layout.fetch_summary_key(run_id)))
    assert summary["committed"] == 1 and summary["failed"] == 1
    assert summary["failures"][0]["accession"] == Q


def test_fetch_without_a_plan_refuses(rt: Runtime) -> None:
    with pytest.raises(plan.PlanNotFound):
        pipeline.fetch_filings_stage(rt, "2026-01-01T00-00-00Z")


def test_limit_plans_only_first_n_companies(rt: Runtime, monkeypatch: pytest.MonkeyPatch) -> None:
    edgar = make_edgar()
    edgar.routes[layout.TICKER_MAP_URL] = ticker_map(
        (320193, "AAPL", "Apple"), (789019, "MSFT", "Microsoft")
    )
    wire(rt, edgar, monkeypatch)
    result = pipeline.run_plan_stage(
        rt, tickers=["AAPL", "MSFT"], limit=1, clock=lambda: datetime(2026, 10, 30, tzinfo=UTC)
    )
    assert result.summary["tickers"] == ["AAPL"] and result.summary["ciks_planned"] == 1


def test_progress_is_logged_per_company_and_filing(
    rt: Runtime, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    wire(rt, make_edgar(), monkeypatch)
    with caplog.at_level("INFO", logger="sec_ingest"):
        run_id = pipeline.run_plan_stage(
            rt, clock=lambda: datetime(2026, 10, 30, tzinfo=UTC)
        ).run_id
        pipeline.fetch_filings_stage(rt, run_id)
    planned = [r for r in caplog.records if r.getMessage() == "planned company"]
    done = [r for r in caplog.records if r.getMessage() == "filing processed"]
    assert [(r.n, r.total) for r in planned] == [(1, 1)]  # type: ignore[attr-defined]
    assert [(r.n, r.total, r.state) for r in done] == [(1, 2, "committed"), (2, 2, "committed")]  # type: ignore[attr-defined]


def test_sp500_universe_plans_from_membership_with_provenance(
    rt: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sec_ingest.plan import Company

    wire(rt, make_edgar(), monkeypatch)
    members = [Company("AAPL", CIK), Company("AAPL2", CIK), Company("ZZZ", "0000000999")]
    info = {"name": "sp500", "snapshot": "bronze/reference/sp500_membership/x.csv", "rows": 3}
    monkeypatch.setattr(Runtime, "membership", lambda self, clock: (members, info))
    now = datetime(2026, 10, 30, tzinfo=UTC)

    full = pipeline.run_plan_stage(rt, universe="sp500", clock=lambda: now)
    assert full.summary["tickers"] == ["AAPL", "ZZZ"]  # AAPL2 dropped: same CIK as AAPL
    assert full.summary["duplicate_tickers"] == {"AAPL2": "AAPL"}
    assert full.summary["universe"]["snapshot"].endswith("x.csv")
    assert full.summary["universe"]["limit"] is None
    assert list(full.summary["unplannable"]) == ["0000000999"]  # ZZZ 404s but AAPL proceeds

    limited = pipeline.run_plan_stage(
        rt, universe="sp500", limit=1, run_id="2026-10-30T01-00-00Z", clock=lambda: now
    )
    assert limited.summary["tickers"] == ["AAPL"] and limited.summary["universe"]["limit"] == 1


def test_unknown_universe_is_rejected(rt: Runtime) -> None:
    with pytest.raises(ValueError, match="unknown universe"):
        pipeline.run_plan_stage(rt, universe="nasdaq")

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

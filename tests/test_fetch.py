import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from sec_ingest import layout
from sec_ingest.blobstore import LocalBlobStore
from sec_ingest.edgar_client import HaltRun
from sec_ingest.fetch_filing import fetch_filing
from sec_ingest.ledger import Ledger, rebuild_ledger
from sec_ingest.run_fetch import fetch_companyfacts, fetch_filings, write_fetch_summary

from .helpers import FakeEdgar

NOW = datetime(2026, 10, 30, 14, 3, 12, tzinfo=UTC)
CIK = "0000320193"
ACC = "0000320193-26-000020"
BASE = layout.filing_base_url(CIK, ACC)
INDEX = b'{"directory":{"item":[]}}'
PRIMARY = b"<html>10-Q body</html>"


def plan_line(
    acc: str = ACC, form: str = "10-Q", doc: str = "aapl-20260627.htm", **kw: Any
) -> dict[str, Any]:
    return {
        "run_id": "r1",
        "as_of": "2026-10-30",
        "cik": CIK,
        "ticker": "AAPL",
        "company_name": "Apple Inc.",
        "accession": acc,
        "form": form,
        "filing_date": "2026-07-31",
        "report_date": "2026-06-27",
        "primary_document": doc,
        "items": None,
        "accepted_at": "2026-07-31T16:30:42Z",
        "is_inline_xbrl": True,
        "decision": "fetch",
        "reason": "latest_10q",
        **kw,
    }


def routes_for(
    acc: str = ACC, doc: str = "aapl-20260627.htm"
) -> dict[str, bytes | int | BaseException]:
    base = layout.filing_base_url(CIK, acc)
    return {f"{base}/index.json": INDEX, f"{base}/{doc}": PRIMARY}


@pytest.fixture
def store(tmp_path: Path) -> LocalBlobStore:
    return LocalBlobStore(tmp_path)


@pytest.fixture
def ledger(tmp_path: Path) -> Ledger:
    return Ledger(tmp_path / "ledger.sqlite")


def run(edgar: FakeEdgar, store: LocalBlobStore, ledger: Ledger, line: dict[str, Any], **kw: Any):  # type: ignore[no-untyped-def]
    return fetch_filing(
        client=edgar.client(),
        store=store,
        ledger=ledger,
        line=line,
        run_id="r1",
        clock=lambda: NOW,
        **kw,
    )


def test_commit_writes_documents_meta_last_and_ledger(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    out = run(FakeEdgar(routes_for()), store, ledger, plan_line())
    assert out.state == "committed" and out.bytes_written == len(INDEX) + len(PRIMARY)

    prefix = layout.filing_prefix(CIK, ACC)
    meta = json.loads(store.get_bytes(f"{prefix}/_meta.json"))
    assert meta["schema_version"] == 1 and meta["amends_accession"] is None
    assert meta["selection_reason"] == "latest_10q" and meta["company_name"] == "Apple Inc."
    roles = {d["role"]: d for d in meta["documents"]}
    assert roles["primary"]["filename"] == "primary__aapl-20260627.htm"
    assert roles["primary"]["sha256"] == hashlib.sha256(PRIMARY).hexdigest()
    assert roles["primary"]["source_url"] == f"{BASE}/aapl-20260627.htm"
    # bytes on disk are exactly what was received
    assert store.get_bytes(f"{prefix}/primary__aapl-20260627.htm") == PRIMARY
    row = ledger.get(ACC)
    assert row and row.state == "committed" and row.committed_run == "r1"


def test_second_run_makes_zero_http_calls(store: LocalBlobStore, ledger: Ledger) -> None:
    run(FakeEdgar(routes_for()), store, ledger, plan_line())
    again = FakeEdgar(routes_for())
    out = run(again, store, ledger, plan_line())
    assert out.state == "skipped"
    assert again.requested == []


def test_skip_repairs_a_lagging_ledger(store: LocalBlobStore, ledger: Ledger) -> None:
    run(FakeEdgar(routes_for()), store, ledger, plan_line())
    ledger.clear()
    out = run(FakeEdgar(), store, ledger, plan_line())
    assert out.state == "skipped"
    assert ledger.get(ACC).state == "committed"  # type: ignore[union-attr]


def test_crash_mid_filing_then_rerun_completes_without_duplicates(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    class Crash(BaseException):
        pass

    flaky = FakeEdgar({**routes_for(), f"{BASE}/aapl-20260627.htm": Crash()})
    with pytest.raises(Crash):
        run(flaky, store, ledger, plan_line())

    prefix = layout.filing_prefix(CIK, ACC)
    assert store.exists(f"{prefix}/index.json")  # partial attempt on disk...
    assert not store.exists(f"{prefix}/_meta.json")  # ...but not committed
    assert ledger.get(ACC).state == "planned"  # type: ignore[union-attr]

    assert run(FakeEdgar(routes_for()), store, ledger, plan_line()).state == "committed"
    assert len(ledger.all_rows()) == 1
    assert sorted(store.list(prefix)) == [
        f"{prefix}/_meta.json",
        f"{prefix}/index.json",
        f"{prefix}/primary__aapl-20260627.htm",
    ]


def test_failure_is_recorded_and_other_filings_continue(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    good, bad = "0000320193-26-000021", "0000320193-26-000022"
    routes = {**routes_for(good), **routes_for(bad)}
    routes[f"{layout.filing_base_url(CIK, bad)}/aapl-20260627.htm"] = 404
    lines = [plan_line(bad), plan_line(good)]  # the failing one goes first

    outcomes = fetch_filings(
        client=FakeEdgar(routes).client(),
        store=store,
        ledger=ledger,
        plan_lines=lines,
        run_id="r1",
        clock=lambda: NOW,
    )
    assert [(o.accession, o.state) for o in outcomes] == [(bad, "failed"), (good, "committed")]
    failed = ledger.get(bad)
    assert failed and failed.state == "failed" and "404" in (failed.last_error or "")

    summary = write_fetch_summary(
        store,
        run_id="r1",
        outcomes=outcomes,
        companyfacts={},
        retries=0,
        started_monotonic=time.monotonic(),
    )
    assert summary["committed"] == 1 and summary["failed"] == 1
    assert summary["failures"][0]["accession"] == bad


def test_403_halts_the_run(store: LocalBlobStore, ledger: Ledger) -> None:
    routes = routes_for()
    routes[f"{BASE}/index.json"] = 403
    with pytest.raises(HaltRun):
        run(FakeEdgar(routes), store, ledger, plan_line())
    assert not store.exists(layout.meta_key(CIK, ACC))


def test_empty_document_is_a_failure_not_a_commit(store: LocalBlobStore, ledger: Ledger) -> None:
    routes = routes_for()
    routes[f"{BASE}/aapl-20260627.htm"] = b""
    out = run(FakeEdgar(routes), store, ledger, plan_line())
    assert out.state == "failed" and "EmptyDocument" in (out.error or "")
    assert not store.exists(layout.meta_key(CIK, ACC))


FIXTURES = Path(__file__).parent / "fixtures" / "spike1"
AAPL_8K = "0000320193-26-000018"  # real filing: 8-K plus EX-99.1 plus a graphic


def real_8k_routes(page_fixture: str, acc: str, extra: dict[str, bytes]) -> FakeEdgar:
    base = layout.filing_base_url(CIK, acc)
    routes: dict[str, bytes | int | BaseException] = {
        f"{base}/index.json": INDEX,
        f"{base}/e.htm": b"<html>cover page</html>",
        f"{base}/{acc}-index.htm": (FIXTURES / page_fixture).read_bytes(),
    }
    routes.update({f"{base}/{name}": body for name, body in extra.items()})
    return FakeEdgar(routes)


def test_8k_fetches_ex99_from_index_page_using_real_fixture(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    edgar = real_8k_routes(
        f"AAPL_8-K_{AAPL_8K}_{AAPL_8K}-index.htm",
        AAPL_8K,
        {"a8-kex991q3202606272026.htm": b"press release"},
    )
    line = plan_line(AAPL_8K, "8-K", "e.htm", reason="8k_window", items="2.02")
    out = run(edgar, store, ledger, line)

    assert out.state == "committed", out.error
    prefix = layout.filing_prefix(CIK, AAPL_8K)
    assert (
        store.get_bytes(f"{prefix}/exhibits/ex99-1__a8-kex991q3202606272026.htm")
        == b"press release"
    )
    assert store.exists(f"{prefix}/{AAPL_8K}-index.htm")
    meta = json.loads(store.get_bytes(f"{prefix}/_meta.json"))
    assert [d["role"] for d in meta["documents"]] == ["index", "primary", "index_html", "exhibit"]
    assert meta["items"] == "2.02"
    assert not any("jpg" in url for url in edgar.requested)  # graphics are never fetched


def test_8k_without_ex99_commits_primary_only(store: LocalBlobStore, ledger: Ledger) -> None:
    acc = "0001193125-26-426704"  # real JPM 8-K whose only exhibit is EX-5.1
    edgar = real_8k_routes(f"JPM_8-K_{acc}_{acc}-index.htm", acc, {})
    line = plan_line(acc, "8-K", "e.htm", reason="8k_window", items="8.01,9.01")
    assert run(edgar, store, ledger, line).state == "committed"
    meta = json.loads(store.get_bytes(layout.meta_key(CIK, acc)))
    assert "exhibit" not in [d["role"] for d in meta["documents"]]


def test_8k_with_unparseable_index_page_fails_instead_of_committing(
    store: LocalBlobStore, ledger: Ledger
) -> None:
    acc = "0000320193-26-000030"
    routes = routes_for(acc, "e.htm")
    routes[f"{layout.filing_base_url(CIK, acc)}/{acc}-index.htm"] = b"<html>blocked</html>"
    line = plan_line(acc, "8-K", "e.htm", reason="8k_window")
    out = run(FakeEdgar(routes), store, ledger, line)
    assert out.state == "failed" and "ExhibitParseError" in (out.error or "")
    assert not store.exists(layout.meta_key(CIK, acc))


def test_only_fetch_decisions_are_processed(store: LocalBlobStore, ledger: Ledger) -> None:
    lines = [plan_line(decision="skip"), plan_line("x-1", decision="reject")]
    edgar = FakeEdgar()
    assert (
        fetch_filings(
            client=edgar.client(), store=store, ledger=ledger, plan_lines=lines, run_id="r1"
        )
        == []
    )
    assert edgar.requested == []


def test_companyfacts_snapshot_and_error_isolation(store: LocalBlobStore, ledger: Ledger) -> None:
    other = "0000789019"
    edgar = FakeEdgar(
        {layout.companyfacts_url(CIK): b'{"facts":{}}', layout.companyfacts_url(other): b"not json"}
    )
    res = fetch_companyfacts(
        client=edgar.client(), store=store, ledger=ledger, ciks=[other, CIK], clock=lambda: NOW
    )
    assert res[CIK] is None and res[other] is not None
    assert store.exists(layout.companyfacts_key(CIK, NOW))
    assert not store.exists(layout.companyfacts_key(other, NOW))  # invalid payload is not kept
    assert ledger.companyfacts_snapshot_count(CIK) == 1


def test_rebuild_after_fetch_matches_ledger(store: LocalBlobStore, ledger: Ledger) -> None:
    run(FakeEdgar(routes_for()), store, ledger, plan_line())
    before = ledger.all_rows()
    assert rebuild_ledger(store, ledger) == (1, 0)
    after = ledger.all_rows()
    assert [(r.accession, r.state, r.primary_document) for r in after] == [
        (r.accession, r.state, r.primary_document) for r in before
    ]

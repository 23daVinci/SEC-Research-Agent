import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from sec_ingest import layout, plan
from sec_ingest.blobstore import LocalBlobStore
from sec_ingest.ledger import Ledger
from sec_ingest.run_fetch import fetch_companyfacts, fetch_filings
from sec_ingest.validate import validate_run

from .helpers import FakeEdgar, filing, submissions, ticker_map

NOW = datetime(2026, 10, 30, 14, 0, 0, tzinfo=UTC)
RUN = plan.new_run_id(NOW)
CIK = "0000320193"
K, Q = "0000320193-25-000079", "0000320193-26-000020"


def facts(*accns: str) -> bytes:
    items = [{"accn": a, "val": 1} for a in accns]
    return json.dumps({"facts": {"us-gaap": {"Revenues": {"units": {"USD": items}}}}}).encode()


class Env:
    def __init__(self, tmp_path: Path, facts_body: bytes) -> None:
        self.store = LocalBlobStore(tmp_path)
        self.ledger = Ledger(tmp_path / "ledger.sqlite")
        routes: dict[str, bytes | int | BaseException] = {
            layout.TICKER_MAP_URL: ticker_map((320193, "AAPL", "Apple Inc.")),
            layout.submissions_url(CIK): submissions(
                [filing(K, "10-K", "2025-10-31", "k.htm"), filing(Q, "10-Q", "2026-07-31", "q.htm")]
            ),
            layout.companyfacts_url(CIK): facts_body,
        }
        for acc, doc in ((K, "k.htm"), (Q, "q.htm")):
            base = layout.filing_base_url(CIK, acc)
            routes[f"{base}/index.json"] = b"{}"
            routes[f"{base}/{doc}"] = f"<html>{acc}</html>".encode()
        self.edgar = FakeEdgar(routes)

    def run(self, tickers: list[str] | None = None) -> None:
        client = self.edgar.client(RUN)
        plan.build_plan(
            client=client,
            store=self.store,
            ledger=self.ledger,
            tickers=["AAPL"],
            run_id=RUN,
            as_of=date(2026, 10, 30),
            window_months=6,
            clock=lambda: NOW,
        )
        lines, summary = plan.read_plan(self.store, RUN)
        fetch_filings(
            client=client,
            store=self.store,
            ledger=self.ledger,
            plan_lines=lines,
            run_id=RUN,
            clock=lambda: NOW,
        )
        fetch_companyfacts(
            client=client,
            store=self.store,
            ledger=self.ledger,
            ciks=summary["companyfacts_refresh"],
            clock=lambda: NOW,
        )

    def validate(self, tickers: list[str] | None = None):  # type: ignore[no-untyped-def]
        lines, summary = plan.read_plan(self.store, RUN)
        return validate_run(
            store=self.store,
            ledger=self.ledger,
            run_id=RUN,
            configured_tickers=tickers or ["AAPL"],
            plan_lines=lines,
            plan_summary=summary,
        )


@pytest.fixture
def env(tmp_path: Path) -> Env:
    e = Env(tmp_path, facts(K, Q))
    e.run()
    return e


def test_clean_run_passes_with_no_warnings(env: Env) -> None:
    rep = env.validate()
    assert rep.ok, rep.errors
    assert rep.warnings == []


def test_cross_layer_miss_is_only_a_warning(tmp_path: Path) -> None:
    e = Env(tmp_path, facts(K))  # facts lag: the new 10-Q is missing
    e.run()
    rep = e.validate()
    assert rep.ok
    assert len(rep.warnings) == 1 and Q in rep.warnings[0] and rep.warnings[0].startswith("[8]")


def test_hash_mismatch_is_detected(env: Env, tmp_path: Path) -> None:
    key = f"{layout.filing_prefix(CIK, Q)}/primary__q.htm"
    env.store.put_bytes(key, b"tampered", overwrite=True)
    rep = env.validate()
    assert any(e.startswith("[4]") and "hash mismatch" in e for e in rep.errors)


def test_missing_and_empty_documents_detected(env: Env) -> None:
    prefix = layout.filing_prefix(CIK, Q)
    (Path(env.store.root) / f"{prefix}/index.json").unlink()
    env.store.put_bytes(f"{prefix}/primary__q.htm", b"", overwrite=True)
    errors = env.validate().errors
    assert any("missing index.json" in e for e in errors)
    assert any("empty primary__q.htm" in e for e in errors)


def test_wrong_ticker_count_fails_check_1(env: Env) -> None:
    assert any(e.startswith("[1]") for e in env.validate(["AAPL", "MSFT"]).errors)


def test_unrun_fetch_decision_fails_check_6(tmp_path: Path) -> None:
    e = Env(tmp_path, facts(K, Q))
    e.run()
    e.ledger.clear()  # pretend nothing was recorded
    errors = e.validate().errors
    assert any(x.startswith("[6]") for x in errors)


def test_stale_or_missing_companyfacts_fails_check_7(env: Env) -> None:
    env.ledger.clear()
    env.ledger.record_companyfacts_snapshot(
        CIK,
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
        path=layout.companyfacts_key(CIK, datetime(2026, 1, 1, tzinfo=UTC)),
        sha256="x",
    )
    assert any(e.startswith("[7]") and "predates" in e for e in env.validate().errors)


def test_missing_10q_without_exception_fails_check_2(env: Env) -> None:
    env.ledger.clear()
    errors = env.validate().errors
    assert any(e.startswith("[2]") for e in errors)


def test_incremental_validation_skips_older_runs_but_full_audit_catches_them(env: Env) -> None:
    """Tampering with a filing committed in an earlier run: only --full notices."""
    key = f"{layout.filing_prefix(CIK, Q)}/primary__q.htm"
    env.store.put_bytes(key, b"tampered", overwrite=True)
    lines, summary = plan.read_plan(env.store, RUN)

    def run(run_id: str, full: bool):  # type: ignore[no-untyped-def]
        return validate_run(
            store=env.store,
            ledger=env.ledger,
            run_id=run_id,
            configured_tickers=["AAPL"],
            plan_lines=lines,
            plan_summary=summary,
            full_audit=full,
        )

    later = "2026-11-01T00-00-00Z"  # the filings above were committed by RUN, not this run
    incremental = run(later, full=False)
    assert incremental.filings_audited == 0
    assert not any(e.startswith("[4]") for e in incremental.errors)

    full = run(later, full=True)
    assert full.filings_audited == 2 and full.full_audit
    assert any("hash mismatch" in e for e in full.errors)


def test_unplannable_company_is_a_loud_validation_error(env: Env) -> None:
    lines, summary = plan.read_plan(env.store, RUN)
    summary = {**summary, "unplannable": {"0000000999": "GONE: HTTP 404"}}
    rep = validate_run(
        store=env.store,
        ledger=env.ledger,
        run_id=RUN,
        configured_tickers=["AAPL"],
        plan_lines=lines,
        plan_summary=summary,
    )
    assert any(e.startswith("[1]") and "0000000999" in e for e in rep.errors)

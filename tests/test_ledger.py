import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from sec_ingest import layout
from sec_ingest.blobstore import LocalBlobStore
from sec_ingest.ledger import Ledger, rebuild_ledger
from sec_ingest.selection import FilingRow

CIK = "0000320193"
T0 = datetime(2026, 10, 30, 14, 3, 12, tzinfo=UTC)


def row(acc: str, form: str, filed: str) -> FilingRow:
    return FilingRow(acc, form, date.fromisoformat(filed), None, f"{acc}.htm")


@pytest.fixture
def ledger(tmp_path: Path) -> Ledger:
    return Ledger(tmp_path / "ledger.sqlite")


def commit(ledger: Ledger, r: FilingRow, run: str = "r1") -> None:
    ledger.mark_planned(r, cik=CIK, ticker="AAPL", run_id=run)
    ledger.mark_committed(r.accession, run_id=run, at=T0)


def test_state_transitions(ledger: Ledger) -> None:
    r = row("a-1", "10-K", "2026-01-01")
    ledger.mark_planned(r, cik="320193", ticker="AAPL", run_id="r1")
    got = ledger.get("a-1")
    assert got and got.state == "planned" and got.cik == CIK  # padded to 10 digits

    ledger.mark_failed("a-1", "HTTP 404")
    assert ledger.get("a-1").state == "failed"  # type: ignore[union-attr]

    ledger.mark_planned(r, cik=CIK, ticker="AAPL", run_id="r2")  # retry flips failed -> planned
    assert ledger.get("a-1").state == "planned"  # type: ignore[union-attr]
    assert ledger.get("a-1").first_planned_run == "r1"  # type: ignore[union-attr]

    ledger.mark_committed("a-1", run_id="r2", at=T0)
    ledger.mark_failed("a-1", "late error")  # a committed filing can never regress
    final = ledger.get("a-1")
    assert final and final.state == "committed" and final.last_error is None
    assert ledger.committed_accessions() == {"a-1"}


def test_commit_unknown_accession_raises(ledger: Ledger) -> None:
    with pytest.raises(KeyError):
        ledger.mark_committed("nope", run_id="r", at=T0)


def test_replanning_committed_filing_is_noop(ledger: Ledger) -> None:
    r = row("a-1", "10-K", "2026-01-01")
    commit(ledger, r)
    ledger.mark_planned(r, cik=CIK, ticker="AAPL", run_id="r2")
    assert ledger.get("a-1").state == "committed"  # type: ignore[union-attr]
    assert len(ledger.all_rows()) == 1


def test_current_periodic_is_latest_committed_only(ledger: Ledger) -> None:
    commit(ledger, row("k-old", "10-K", "2025-01-01"))
    commit(ledger, row("k-new", "10-K", "2026-01-01"))
    commit(ledger, row("q-new", "10-Q", "2026-08-01"))
    ledger.mark_planned(row("k-newest", "10-K", "2026-09-01"), cik=CIK, ticker=None, run_id="r1")
    commit(ledger, row("e-1", "8-K", "2026-09-01"))
    assert [r.accession for r in ledger.current_periodic()] == ["k-new", "q-new"]


def test_eight_k_window_query(ledger: Ledger) -> None:
    commit(ledger, row("e-in", "8-K", "2026-04-30"))
    commit(ledger, row("e-out", "8-K", "2026-04-29"))
    commit(ledger, row("k", "10-K", "2026-09-01"))
    got = ledger.eight_k_window(date(2026, 10, 30), 6)
    assert [r.accession for r in got] == ["e-in"]


def test_companyfacts_snapshots(ledger: Ledger) -> None:
    assert ledger.latest_companyfacts(CIK) is None
    ledger.record_companyfacts_snapshot(CIK, fetched_at=T0, path="p1", sha256="h1")
    ledger.record_companyfacts_snapshot(CIK, fetched_at=T0, path="p1", sha256="h1")  # idempotent
    later = datetime(2026, 11, 1, tzinfo=UTC)
    ledger.record_companyfacts_snapshot("320193", fetched_at=later, path="p2", sha256="h2")
    assert ledger.latest_companyfacts(CIK) == ("p2", "h2")
    assert ledger.companyfacts_snapshot_count(CIK) == 2


def write_committed_filing(store: LocalBlobStore, r: FilingRow) -> None:
    meta = {
        "run_id": "r1",
        "cik": CIK,
        "ticker_at_plan": "AAPL",
        "accession": r.accession,
        "form": r.form,
        "filing_date": r.filing_date.isoformat(),
        "report_date": None,
        "items": None,
        "fetched_at": T0.isoformat(),
        "documents": [
            {"role": "index", "filename": "index.json"},
            {"role": "primary", "filename": f"primary__{r.primary_document}"},
        ],
    }
    store.put_bytes(layout.meta_key(CIK, r.accession), json.dumps(meta).encode())


def test_rebuild_from_blob_store_matches_original(tmp_path: Path, ledger: Ledger) -> None:
    store = LocalBlobStore(tmp_path / "blobs")
    rows = [row("k", "10-K", "2026-01-01"), row("q", "10-Q", "2026-08-01")]
    for r in rows:
        commit(ledger, r)
        write_committed_filing(store, r)
    ledger.mark_planned(row("planned-only", "8-K", "2026-09-01"), cik=CIK, ticker=None, run_id="r1")
    key = layout.companyfacts_key(CIK, T0)
    store.put_bytes(key, b'{"facts":{}}')

    before = {
        r.accession: (r.form, r.filing_date, r.primary_document, r.state)
        for r in ledger.all_rows()
        if r.state == "committed"
    }

    assert rebuild_ledger(store, ledger) == (2, 1)

    after = {
        r.accession: (r.form, r.filing_date, r.primary_document, r.state) for r in ledger.all_rows()
    }
    assert after == before  # blob store wins; the planned-only row had no proof and is gone
    assert ledger.latest_companyfacts(CIK)[0] == key  # type: ignore[index]

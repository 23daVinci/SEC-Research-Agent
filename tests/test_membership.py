import hashlib
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from sec_ingest import layout
from sec_ingest.blobstore import LocalBlobStore
from sec_ingest.membership import (
    MembershipError,
    fetch_constituents,
    load_membership,
    normalize_ticker,
    parse_constituents,
)
from sec_ingest.plan import dedupe_by_cik

REAL = (Path(__file__).parent / "fixtures" / "sp500_constituents.csv").read_bytes()
SMALL = (3, 10)
NOW = datetime(2026, 11, 1, 12, 0, 0, tzinfo=UTC)


def csv_of(*rows: str) -> bytes:
    return ("Symbol,Security,CIK\n" + "\n".join(rows) + "\n").encode()


def test_real_constituents_file() -> None:
    companies = parse_constituents(REAL)
    assert len(companies) == 503
    by_ticker = {c.ticker: c.cik for c in companies}
    assert by_ticker["AAPL"] == "0000320193"
    assert "BRK-B" in by_ticker and "BF-B" in by_ticker  # dots normalized to the SEC form
    assert all(len(c.cik) == 10 for c in companies)
    unique, dropped = dedupe_by_cik(companies)
    assert len(unique) == 500  # GOOG/GOOGL, FOX/FOXA, NWS/NWSA share a CIK
    assert set(dropped) == {"GOOG", "FOX", "NWS"}


@pytest.mark.parametrize(
    ("raw", "expected"), [("BRK.B", "BRK-B"), (" aapl ", "AAPL"), ("BF.B", "BF-B")]
)
def test_normalize_ticker(raw: str, expected: str) -> None:
    assert normalize_ticker(raw) == expected


def test_rejects_html_masquerading_as_csv() -> None:
    with pytest.raises(MembershipError, match="HTML"):
        parse_constituents(b"<!DOCTYPE html><html>blocked</html>")


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (b"Ticker,Name\nA,B\n", "unexpected columns"),
        (csv_of("AAA,x,1", "BBB,x,", "CCC,x,3"), "bad ticker/CIK"),
        (csv_of("AAA,x,1", "BBB,x,abc", "CCC,x,3"), "bad ticker/CIK"),
        (csv_of("AAA,x,1", "AAA,x,2", "CCC,x,3"), "duplicate ticker"),
        (csv_of("AAA,x,1", "BBB,x,2"), "expected between"),
    ],
)
def test_rejects_malformed_files(raw: bytes, message: str) -> None:
    with pytest.raises(MembershipError, match=message):
        parse_constituents(raw, expected_rows=SMALL)


def transport(status: int, body: bytes) -> httpx.MockTransport:
    return httpx.MockTransport(lambda request: httpx.Response(status, content=body))


def test_fetch_errors_are_loud() -> None:
    with pytest.raises(MembershipError, match="HTTP 404"):
        fetch_constituents(transport(404, b""))
    boom = httpx.MockTransport(lambda r: (_ for _ in ()).throw(httpx.ConnectError("down")))
    with pytest.raises(MembershipError, match="ConnectError"):
        fetch_constituents(boom)


def test_load_snapshots_raw_bytes_with_provenance(tmp_path: Path) -> None:
    store = LocalBlobStore(tmp_path)
    companies, info = load_membership(store, clock=lambda: NOW, transport=transport(200, REAL))
    assert len(companies) == 503
    key = layout.membership_key(NOW)
    assert store.get_bytes(key) == REAL  # bytes exactly as received
    assert info["snapshot"] == key and info["sha256"] == hashlib.sha256(REAL).hexdigest()
    assert (info["rows"], info["unique_ciks"], info["point_in_time"]) == (503, 500, False)


def test_invalid_download_is_not_snapshotted(tmp_path: Path) -> None:
    store = LocalBlobStore(tmp_path)
    with pytest.raises(MembershipError):
        load_membership(store, clock=lambda: NOW, transport=transport(200, b"<html>nope</html>"))
    assert list(store.list("bronze/reference/sp500_membership")) == []

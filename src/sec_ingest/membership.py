"""S&P 500 membership from the community `datasets/s-and-p-500-companies` constituents CSV.

The file carries each company's CIK, so no ticker matching is needed. It is volunteer-maintained
and not point-in-time: we snapshot the raw bytes with their hash and fetch time so any run can be
explained, and fail loudly when the content does not look like a constituents list.
"""

from __future__ import annotations

import csv
import hashlib
import io
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from . import layout
from .blobstore import BlobStore
from .plan import Company

SP500_URL = (
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
)
# The index has ~503 tickers (about 500 companies). Far outside this means a bad download.
EXPECTED_ROWS = (450, 550)


class MembershipError(Exception):
    pass


def normalize_ticker(ticker: str) -> str:
    """`BRK.B` -> `BRK-B`, the form used by the SEC ticker map."""
    return ticker.strip().upper().replace(".", "-")


def parse_constituents(
    raw: bytes, *, expected_rows: tuple[int, int] = EXPECTED_ROWS
) -> list[Company]:
    """Parse the CSV into companies in file order. Share classes keep separate rows here; the
    plan de-duplicates by CIK."""
    text = raw.decode("utf-8-sig", errors="replace")
    if text.lstrip().startswith("<"):
        raise MembershipError("got an HTML page instead of the constituents CSV")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or not {"Symbol", "CIK"} <= set(reader.fieldnames):
        raise MembershipError(f"unexpected columns: {reader.fieldnames}")
    companies: list[Company] = []
    seen: set[str] = set()
    for line_no, row in enumerate(reader, 2):
        ticker, cik = normalize_ticker(row["Symbol"] or ""), (row["CIK"] or "").strip()
        if not ticker or not cik.isdigit():
            raise MembershipError(f"line {line_no}: bad ticker/CIK {row['Symbol']!r}/{cik!r}")
        if ticker in seen:
            raise MembershipError(f"line {line_no}: duplicate ticker {ticker}")
        seen.add(ticker)
        companies.append(Company(ticker, layout.cik10(cik)))
    low, high = expected_rows
    if not low <= len(companies) <= high:
        raise MembershipError(f"{len(companies)} rows; expected between {low} and {high}")
    return companies


def fetch_constituents(transport: httpx.BaseTransport | None = None, url: str = SP500_URL) -> bytes:
    with httpx.Client(timeout=httpx.Timeout(30, connect=10), transport=transport) as http:
        try:
            resp = http.get(url, follow_redirects=True)
        except httpx.HTTPError as e:
            raise MembershipError(f"could not fetch {url}: {type(e).__name__}") from e
    if resp.status_code != 200:
        raise MembershipError(f"HTTP {resp.status_code} for {url}")
    return resp.content


def load_membership(
    store: BlobStore,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    transport: httpx.BaseTransport | None = None,
    expected_rows: tuple[int, int] = EXPECTED_ROWS,
) -> tuple[list[Company], dict[str, Any]]:
    """Fetch, validate, snapshot the raw bytes. Returns (companies, provenance for the plan)."""
    raw = fetch_constituents(transport)
    companies = parse_constituents(raw, expected_rows=expected_rows)  # validate before saving
    moment = clock()
    key = layout.membership_key(moment)
    store.put_bytes(key, raw)
    info = {
        "name": "sp500",
        "source": SP500_URL,
        "snapshot": key,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "fetched_at": moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rows": len(companies),
        "unique_ciks": len({c.cik for c in companies}),
        "point_in_time": False,
    }
    return companies, info

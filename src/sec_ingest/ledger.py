"""SQLite ledger: one row per filing and its fetch state. Parameterized SQL only."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from importlib import resources
from pathlib import Path

from dateutil.relativedelta import relativedelta

from . import layout
from .blobstore import BlobStore
from .selection import FilingRow


@dataclass(frozen=True)
class LedgerRow:
    accession: str
    cik: str
    ticker_at_plan: str | None
    form: str
    filing_date: date
    report_date: date | None
    primary_document: str
    items: str | None
    state: str
    first_planned_run: str
    committed_run: str | None
    committed_at: str | None
    last_error: str | None


def _to_row(r: sqlite3.Row) -> LedgerRow:
    return LedgerRow(
        accession=r["accession"],
        cik=r["cik"],
        ticker_at_plan=r["ticker_at_plan"],
        form=r["form"],
        filing_date=date.fromisoformat(r["filing_date"]),
        report_date=date.fromisoformat(r["report_date"]) if r["report_date"] else None,
        primary_document=r["primary_document"],
        items=r["items"],
        state=r["state"],
        first_planned_run=r["first_planned_run"],
        committed_run=r["committed_run"],
        committed_at=r["committed_at"],
        last_error=r["last_error"],
    )


class Ledger:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                resources.files("sec_ingest").joinpath("schema.sql").read_text("utf-8")
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with closing(sqlite3.connect(self._path, timeout=30)) as conn:
            conn.row_factory = sqlite3.Row
            with conn:  # commit on success, roll back on error
                yield conn

    # ---- filings -------------------------------------------------------------------------

    def mark_planned(self, row: FilingRow, *, cik: str, ticker: str | None, run_id: str) -> None:
        """Insert a `planned` row. An existing row is kept; a previously `failed` one is retried."""
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO filings (accession, cik, ticker_at_plan, form, filing_date,
                       report_date, primary_document, items, state, first_planned_run)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?)
                   ON CONFLICT(accession) DO UPDATE SET
                       state = CASE WHEN state = 'failed' THEN 'planned' ELSE state END""",
                (
                    row.accession,
                    layout.cik10(cik),
                    ticker,
                    row.form,
                    row.filing_date.isoformat(),
                    row.report_date.isoformat() if row.report_date else None,
                    row.primary_document,
                    row.items,
                    run_id,
                ),
            )

    def mark_committed(self, accession: str, *, run_id: str, at: datetime) -> None:
        with self._connect() as conn:
            cur = conn.execute(
                """UPDATE filings SET state = 'committed', committed_run = ?, committed_at = ?,
                       last_error = NULL WHERE accession = ?""",
                (run_id, at.isoformat(), accession),
            )
            if cur.rowcount != 1:
                raise KeyError(f"cannot commit unknown accession {accession}")

    def mark_failed(self, accession: str, error: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE filings SET state = 'failed', last_error = ? WHERE accession = ? "
                "AND state != 'committed'",
                (error[:2000], accession),
            )

    def get(self, accession: str) -> LedgerRow | None:
        with self._connect() as conn:
            r = conn.execute("SELECT * FROM filings WHERE accession = ?", (accession,)).fetchone()
        return _to_row(r) if r else None

    def committed_accessions(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT accession FROM filings WHERE state = 'committed'")
            return {r["accession"] for r in rows}

    def all_rows(self) -> list[LedgerRow]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM filings ORDER BY cik, filing_date, accession")
            return [_to_row(r) for r in rows]

    def current_periodic(self) -> list[LedgerRow]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM current_periodic ORDER BY cik, form")
            return [_to_row(r) for r in rows]

    def eight_k_window(self, as_of: date, months: int) -> list[LedgerRow]:
        start = as_of - relativedelta(months=months)
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM filings WHERE form = '8-K' AND state = 'committed'
                   AND filing_date >= ? AND filing_date <= ? ORDER BY cik, filing_date""",
                (start.isoformat(), as_of.isoformat()),
            )
            return [_to_row(r) for r in rows]

    # ---- companyfacts --------------------------------------------------------------------

    def record_companyfacts_snapshot(
        self, cik: str, *, fetched_at: datetime, path: str, sha256: str
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO companyfacts_snapshots (cik, fetched_at, path, sha256)
                   VALUES (?, ?, ?, ?) ON CONFLICT(cik, fetched_at) DO NOTHING""",
                (layout.cik10(cik), fetched_at.isoformat(), path, sha256),
            )

    def latest_companyfacts(self, cik: str) -> tuple[str, str] | None:
        """(path, sha256) of the newest snapshot, or None."""
        with self._connect() as conn:
            r = conn.execute(
                "SELECT path, sha256 FROM companyfacts_snapshots WHERE cik = ? "
                "ORDER BY fetched_at DESC LIMIT 1",
                (layout.cik10(cik),),
            ).fetchone()
        return (r["path"], r["sha256"]) if r else None

    def companyfacts_snapshot_count(self, cik: str) -> int:
        with self._connect() as conn:
            r = conn.execute(
                "SELECT COUNT(*) AS n FROM companyfacts_snapshots WHERE cik = ?",
                (layout.cik10(cik),),
            ).fetchone()
        return int(r["n"])

    def clear(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM filings")
            conn.execute("DELETE FROM companyfacts_snapshots")


def rebuild_ledger(store: BlobStore, ledger: Ledger) -> tuple[int, int]:
    """Reconstruct the ledger from the blob store, which wins any disagreement.

    Committed filings come from `_meta.json` files; companyfacts snapshots from the files on disk.
    `planned` and `failed` rows carry no blob proof and are intentionally dropped; the next plan
    re-creates them. Returns (filings, companyfacts snapshots) restored.
    """
    ledger.clear()
    filings = snapshots = 0
    for key in store.list(layout.FILINGS_PREFIX):
        if not key.endswith(f"/{layout.META_FILENAME}"):
            continue
        meta = json.loads(store.get_bytes(key))
        row = FilingRow(
            accession=meta["accession"],
            form=meta["form"],
            filing_date=date.fromisoformat(meta["filing_date"]),
            report_date=date.fromisoformat(meta["report_date"]) if meta["report_date"] else None,
            primary_document=next(
                d["filename"].removeprefix("primary__")
                for d in meta["documents"]
                if d["role"] == "primary"
            ),
            items=meta.get("items"),
        )
        ledger.mark_planned(
            row, cik=meta["cik"], ticker=meta.get("ticker_at_plan"), run_id=meta["run_id"]
        )
        ledger.mark_committed(
            row.accession, run_id=meta["run_id"], at=datetime.fromisoformat(meta["fetched_at"])
        )
        filings += 1
    for key in store.list(layout.COMPANYFACTS_PREFIX):
        cik = key.split("cik=")[1].split("/")[0]
        stamp = key.rsplit("fetched=", 1)[1].removesuffix(".json")
        ledger.record_companyfacts_snapshot(
            cik,
            fetched_at=layout.ts_from_key(stamp),
            path=key,
            sha256=hashlib.sha256(store.get_bytes(key)).hexdigest(),
        )
        snapshots += 1
    return filings, snapshots

"""Fetch and commit one filing (doc section 9, steps 1-7).

Order matters: index, primary, exhibits, then `_meta.json` last as the commit marker, then the
ledger. A crash anywhere before `_meta.json` leaves an uncommitted folder that is simply redone.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

from . import layout
from .blobstore import BlobStore
from .edgar_client import EdgarClient, EdgarError, HaltRun, Response
from .exhibits import ExhibitSelector, select_ex99
from .ledger import Ledger
from .selection import FilingRow

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1


class EmptyDocument(EdgarError):
    pass


@dataclass(frozen=True)
class FilingOutcome:
    accession: str
    state: Literal["committed", "skipped", "failed"]
    error: str | None = None
    bytes_written: int = 0


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row_from_plan(line: Mapping[str, Any]) -> FilingRow:
    return FilingRow(
        accession=line["accession"],
        form=line["form"],
        filing_date=date.fromisoformat(line["filing_date"]),
        report_date=date.fromisoformat(line["report_date"]) if line.get("report_date") else None,
        primary_document=line["primary_document"],
        items=line.get("items"),
        accepted_at=line.get("accepted_at"),
        is_inline_xbrl=line.get("is_inline_xbrl"),
    )


def _document(role: str, filename: str, url: str, resp: Response) -> dict[str, Any]:
    if not resp.content:
        raise EmptyDocument(f"empty body for {url}")
    return {
        "role": role,
        "filename": filename,
        "source_url": url,
        "http_status": resp.status,
        "bytes": len(resp.content),
        "sha256": resp.sha256,
    }


def fetch_filing(
    *,
    client: EdgarClient,
    store: BlobStore,
    ledger: Ledger,
    line: Mapping[str, Any],
    run_id: str,
    exhibit_selector: ExhibitSelector = select_ex99,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FilingOutcome:
    """Fetch one planned filing. Per-filing failures are recorded and returned; `HaltRun` and
    `ImmutabilityViolation` propagate because they must stop the whole run."""
    row = _row_from_plan(line)
    cik, accession = line["cik"], row.accession
    prefix = layout.filing_prefix(cik, accession)

    if store.exists(layout.meta_key(cik, accession)):
        _reconcile_committed(store, ledger, line, row)
        return FilingOutcome(accession, "skipped")

    ledger.mark_planned(row, cik=cik, ticker=line.get("ticker"), run_id=run_id)
    base = layout.filing_base_url(cik, accession)
    docs: list[dict[str, Any]] = []
    written = 0
    try:
        index = client.get(f"{base}/index.json")
        # Uncommitted folder: overwriting a partial attempt is allowed (doc section 15).
        store.put_bytes(f"{prefix}/index.json", index.content, overwrite=True)
        docs.append(_document("index", "index.json", f"{base}/index.json", index))
        written += len(index.content)

        primary_url = f"{base}/{row.primary_document}"
        primary = client.get(primary_url)
        primary_name = f"primary__{row.primary_document}"
        store.put_bytes(f"{prefix}/{primary_name}", primary.content, overwrite=True)
        docs.append(_document("primary", primary_name, primary_url, primary))
        written += len(primary.content)

        if row.form == "8-K":
            # index.json has no document types; the -index.htm page does (design doc section 10).
            page_name = f"{accession}-index.htm"
            page = client.get(f"{base}/{page_name}")
            store.put_bytes(f"{prefix}/{page_name}", page.content, overwrite=True)
            docs.append(_document("index_html", page_name, f"{base}/{page_name}", page))
            written += len(page.content)
            for ex in exhibit_selector(page.content):
                url = f"{base}/{ex.filename}"
                resp = client.get(url)
                name = f"exhibits/{ex.label}__{ex.filename}"
                store.put_bytes(f"{prefix}/{name}", resp.content, overwrite=True)
                docs.append(_document("exhibit", name, url, resp))
                written += len(resp.content)

        now = clock()
        meta = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "as_of": line.get("as_of"),
            "selection_reason": line["reason"],
            "cik": cik,
            "ticker_at_plan": line.get("ticker"),
            "company_name": line.get("company_name"),
            "accession": accession,
            "form": row.form,
            "filing_date": row.filing_date.isoformat(),
            "report_date": row.report_date.isoformat() if row.report_date else None,
            "accepted_at": row.accepted_at,
            "items": row.items,
            "is_inline_xbrl": row.is_inline_xbrl,
            "amends_accession": None,
            "documents": docs,
            "fetched_at": _iso(now),
        }
        store.put_bytes(
            layout.meta_key(cik, accession), (json.dumps(meta, indent=2) + "\n").encode()
        )
        ledger.mark_committed(accession, run_id=run_id, at=now)
        return FilingOutcome(accession, "committed", bytes_written=written)
    except HaltRun:
        raise
    except EdgarError as e:
        log.error("filing failed", extra={"accession": accession, "cik": cik, "error": str(e)})
        ledger.mark_failed(accession, f"{type(e).__name__}: {e}")
        return FilingOutcome(accession, "failed", error=f"{type(e).__name__}: {e}")


def _reconcile_committed(
    store: BlobStore, ledger: Ledger, line: Mapping[str, Any], row: FilingRow
) -> None:
    """The blob store wins: if `_meta.json` exists but the ledger lags, repair the ledger."""
    existing = ledger.get(row.accession)
    if existing is not None and existing.state == "committed":
        return
    meta = json.loads(store.get_bytes(layout.meta_key(line["cik"], row.accession)))
    ledger.mark_planned(row, cik=line["cik"], ticker=line.get("ticker"), run_id=meta["run_id"])
    ledger.mark_committed(
        row.accession, run_id=meta["run_id"], at=datetime.fromisoformat(meta["fetched_at"])
    )

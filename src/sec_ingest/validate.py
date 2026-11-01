"""End-of-run Bronze validation (doc section 12). Errors fail the DAG; warnings do not."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from . import layout
from .blobstore import BlobStore
from .ledger import Ledger


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    filings_audited: int = 0  # filings whose documents were re-hashed (check 4)
    full_audit: bool = False

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": self.errors,
            "warnings": self.warnings,
            "filings_audited": self.filings_audited,
            "full_audit": self.full_audit,
        }


def _accessions_in_facts(raw: bytes) -> set[str]:
    out: set[str] = set()
    facts = json.loads(raw).get("facts", {})
    for concepts in facts.values():
        for concept in concepts.values():
            for items in concept.get("units", {}).values():
                out.update(i["accn"] for i in items if "accn" in i)
    return out


def validate_run(
    *,
    store: BlobStore,
    ledger: Ledger,
    run_id: str,
    configured_tickers: Sequence[str],
    plan_lines: Sequence[Mapping[str, Any]],
    plan_summary: Mapping[str, Any],
    full_audit: bool = False,
) -> ValidationReport:
    """`full_audit=False` re-hashes only filings committed in this run (cheap at any scale);
    `True` re-hashes every committed filing (an occasional integrity audit)."""
    rep = ValidationReport(full_audit=full_audit)
    ciks = sorted(plan_summary.get("coverage_history_files_followed", {}))

    # 1. every configured ticker resolved to exactly one CIK
    if plan_summary.get("ciks_planned") != len(configured_tickers):
        rep.errors.append(
            f"[1] planned {plan_summary.get('ciks_planned')} CIKs for "
            f"{len(configured_tickers)} configured tickers"
        )

    for cik, why in plan_summary.get("unplannable", {}).items():
        rep.errors.append(f"[1] {cik} could not be planned: {why}")

    # 2. committed 10-K and 10-Q per CIK, or a documented exception
    current = {(r.cik, r.form) for r in ledger.current_periodic()}
    exceptions = plan_summary.get("exceptions", {})
    for cik in ciks:
        for form, tag in (("10-K", "no_10k"), ("10-Q", "no_10q")):
            if (cik, form) not in current and tag not in exceptions.get(cik, []):
                rep.errors.append(f"[2] {cik} has no committed {form} and no recorded exception")

    # 3. the 8-K coverage guard ran for every CIK (a gap would have failed the plan)
    if len(ciks) != plan_summary.get("ciks_planned"):
        rep.errors.append(
            f"[3] coverage guard recorded for {len(ciks)} CIKs, "
            f"planned {plan_summary.get('ciks_planned')}"
        )

    # 4. every committed filing: documents present, non-empty, hashes match _meta.json
    # 5. no accession appears twice (ledger primary key, and across blob-store folders)
    committed = ledger.committed_accessions()
    meta_accessions: Counter[str] = Counter()
    for key in store.list(layout.FILINGS_PREFIX):
        if not key.endswith(f"/{layout.META_FILENAME}"):
            continue
        meta = json.loads(store.get_bytes(key))
        meta_accessions[meta["accession"]] += 1
        if not full_audit and meta["run_id"] != run_id:
            continue
        rep.filings_audited += 1
        folder = key.removesuffix(layout.META_FILENAME)
        for doc in meta["documents"]:
            path = folder + doc["filename"]
            if not store.exists(path):
                rep.errors.append(f"[4] {meta['accession']}: missing {doc['filename']}")
                continue
            data = store.get_bytes(path)
            if not data:
                rep.errors.append(f"[4] {meta['accession']}: empty {doc['filename']}")
            elif hashlib.sha256(data).hexdigest() != doc["sha256"]:
                rep.errors.append(f"[4] {meta['accession']}: hash mismatch {doc['filename']}")
    for acc in committed - set(meta_accessions):
        rep.errors.append(f"[4] {acc} is committed in the ledger but has no _meta.json")
    rep.errors += [
        f"[5] accession {a} appears in {n} folders" for a, n in meta_accessions.items() if n > 1
    ]

    # 6. every fetch decision is now committed, or failed with an error
    for line in plan_lines:
        if line["decision"] != "fetch":
            continue
        row = ledger.get(line["accession"])
        if row is None or row.state == "planned":
            rep.errors.append(f"[6] {line['accession']} was planned to fetch but never ran")
        elif row.state == "failed" and not row.last_error:
            rep.errors.append(f"[6] {line['accession']} failed without an error message")

    # 7. every companyfacts_refresh CIK has a new snapshot (this run or later) that parses
    run_start = layout.ts_from_key(run_id)
    for cik in plan_summary.get("companyfacts_refresh", []):
        latest = ledger.latest_companyfacts(cik)
        if latest is None:
            rep.errors.append(f"[7] {cik}: no companyfacts snapshot")
            continue
        stamp = latest[0].rsplit("fetched=", 1)[1].removesuffix(".json")
        if layout.ts_from_key(stamp) < run_start:
            rep.errors.append(f"[7] {cik}: companyfacts snapshot predates this run")
            continue
        try:
            json.loads(store.get_bytes(latest[0]))
        except ValueError:
            rep.errors.append(f"[7] {cik}: companyfacts snapshot is not valid JSON")

    # 8. cross-layer link: current filings appear in the latest companyfacts (warning only)
    facts_cache: dict[str, set[str]] = {}
    for r in ledger.current_periodic():
        if r.cik not in facts_cache:
            latest = ledger.latest_companyfacts(r.cik)
            facts_cache[r.cik] = (
                _accessions_in_facts(store.get_bytes(latest[0])) if latest else set()
            )
        if r.accession not in facts_cache[r.cik]:
            rep.warnings.append(
                f"[8] {r.cik} {r.form} {r.accession} not in latest companyfacts (facts may lag)"
            )
    return rep

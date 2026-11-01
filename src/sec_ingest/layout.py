"""Blob keys and identifier helpers (doc sections 4 and 6). Keys are relative, prefix-shaped."""

from __future__ import annotations

from datetime import UTC, datetime

LEDGER_FILENAME = "ledger.sqlite"
FETCH_LOG_KEY = "_manifests/fetch_log.jsonl"
FILINGS_PREFIX = "bronze/filings"
COMPANYFACTS_PREFIX = "bronze/companyfacts"
META_FILENAME = "_meta.json"

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"


def cik10(cik: int | str) -> str:
    return str(int(cik)).zfill(10)


def cik_unpadded(cik: int | str) -> str:
    return str(int(cik))


def accession_nodash(accession: str) -> str:
    return accession.replace("-", "")


def ts_for_key(moment: datetime) -> str:
    """UTC timestamp safe for filenames: `2026-10-30T14-02-11Z`."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")


def ts_from_key(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=UTC)


def submissions_url(cik: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{cik10(cik)}.json"


def submissions_history_url(name: str) -> str:
    return f"https://data.sec.gov/submissions/{name}"


def companyfacts_url(cik: str) -> str:
    return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10(cik)}.json"


def filing_base_url(cik: str, accession: str) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik_unpadded(cik)}/{accession_nodash(accession)}"
    )


def ticker_map_key(moment: datetime) -> str:
    return f"bronze/reference/company_tickers/fetched={ts_for_key(moment)}.json"


def submissions_key(cik: str, moment: datetime) -> str:
    return f"bronze/submissions/cik={cik10(cik)}/fetched={ts_for_key(moment)}.json"


def submissions_history_key(cik: str, moment: datetime, name: str) -> str:
    return f"bronze/submissions/cik={cik10(cik)}/fetched={ts_for_key(moment)}__{name}"


def companyfacts_key(cik: str, moment: datetime) -> str:
    return f"{COMPANYFACTS_PREFIX}/cik={cik10(cik)}/fetched={ts_for_key(moment)}.json"


def filing_prefix(cik: str, accession: str) -> str:
    return f"{FILINGS_PREFIX}/cik={cik10(cik)}/accession={accession}"


def meta_key(cik: str, accession: str) -> str:
    return f"{filing_prefix(cik, accession)}/{META_FILENAME}"


def run_prefix(run_id: str) -> str:
    return f"_manifests/runs/run_id={run_id}"


def plan_key(run_id: str) -> str:
    return f"{run_prefix(run_id)}/plan.jsonl"


def plan_summary_key(run_id: str) -> str:
    return f"{run_prefix(run_id)}/plan_summary.json"


def fetch_summary_key(run_id: str) -> str:
    return f"{run_prefix(run_id)}/fetch_summary.json"

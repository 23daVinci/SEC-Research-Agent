"""Stage functions shared by the CLI and the Airflow DAGs, so both run identical logic."""

from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from . import layout, plan
from .blobstore import LocalBlobStore
from .config import Config, load_config
from .edgar_client import EdgarClient
from .exhibits import ExhibitSelector, select_ex99
from .fetch_filing import FilingOutcome
from .ledger import Ledger
from .membership import load_membership
from .run_fetch import fetch_companyfacts, fetch_filings, write_fetch_summary
from .validate import ValidationReport, validate_run

Clock = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(UTC)


class FetchFailed(Exception):
    """Some filings or companyfacts failed after retries; the run must not look successful."""


class _JsonFormatter(logging.Formatter):
    _EXTRA = (
        "accession",
        "cik",
        "run_id",
        "url",
        "error",
        "ticker",
        "n",
        "total",
        "state",
        "bytes",
        "to_fetch",
    )

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        out.update({k: getattr(record, k) for k in self._EXTRA if hasattr(record, k)})
        return json.dumps(out)


def configure_logging(level: int = logging.INFO) -> None:
    """One JSON object per line on stdout, which the Alloy/Loki pipeline parses by field."""
    pkg = logging.getLogger("sec_ingest")
    if any(getattr(h, "_sec_ingest", False) for h in pkg.handlers):
        return
    handler = logging.StreamHandler(sys.stdout)
    handler._sec_ingest = True  # type: ignore[attr-defined]
    handler.setFormatter(_JsonFormatter())
    pkg.addHandler(handler)
    pkg.setLevel(level)
    pkg.propagate = False


@dataclass
class Runtime:
    cfg: Config
    store: LocalBlobStore
    ledger: Ledger

    def membership(self, clock: Clock) -> tuple[list[plan.Company], dict[str, Any]]:
        """S&P 500 constituents, snapshotted into the blob store."""
        return load_membership(self.store, clock=clock)

    def client(self, run_id: str) -> EdgarClient:
        c = self.cfg
        return EdgarClient(
            user_agent=c.user_agent,
            run_id=run_id,
            rate_limit_per_sec=c.rate_limit_per_sec,
            max_attempts=c.max_attempts,
            connect_timeout_s=c.connect_timeout_s,
            read_timeout_s=c.read_timeout_s,
            log_sink=lambda line: self.store.append_line(layout.FETCH_LOG_KEY, line),
        )


def open_runtime(cfg: Config | None = None) -> Runtime:
    cfg = cfg or load_config()
    store = LocalBlobStore(cfg.bronze_root)
    return Runtime(cfg, store, Ledger(store.root / layout.LEDGER_FILENAME))


# ---- stages ----------------------------------------------------------------------------------


def run_plan_stage(
    rt: Runtime,
    *,
    tickers: list[str] | None = None,
    universe: str | None = None,
    run_id: str | None = None,
    limit: int | None = None,
    clock: Clock = _utcnow,
) -> plan.PlanResult:
    """Build the plan. An explicit `run_id` (Airflow) makes task retries reuse the same run.

    Companies come from explicit `tickers`, else from `universe` ("tickers" = the configured
    list, "sp500" = the constituents snapshot), which defaults to the config. `limit` plans only
    the first N, for staged rollouts (25, then 100, then all).
    """
    source = "tickers" if tickers else (universe or rt.cfg.universe)
    if source not in ("tickers", "sp500"):
        raise ValueError(f"unknown universe {source!r}")
    run_id = run_id or plan.new_run_id(clock())
    as_of = layout.ts_from_key(run_id).date()
    who: dict[str, Any]
    if source == "sp500":
        companies, info = rt.membership(clock)
        who = {"companies": companies[:limit], "universe": {**info, "limit": limit}}
    else:
        who = {"tickers": (tickers or list(rt.cfg.tickers))[:limit]}
    with rt.client(run_id) as client:
        return plan.build_plan(
            client=client,
            store=rt.store,
            ledger=rt.ledger,
            run_id=run_id,
            as_of=as_of,
            window_months=rt.cfg.eight_k_window_months,
            clock=clock,
            **who,
        )


def preview_universe_stage(rt: Runtime, *, clock: Clock = _utcnow) -> dict[str, Any]:
    """Fetch and snapshot the constituents and report problems, without planning anything."""
    companies, info = rt.membership(clock)
    unique, dropped = plan.dedupe_by_cik(companies)
    run_id = plan.new_run_id(clock())
    with rt.client(run_id) as client:
        tm = client.get(layout.TICKER_MAP_URL)
    rt.store.put_bytes(layout.ticker_map_key(clock()), tm.content)
    in_sec = {layout.cik10(e["cik_str"]) for e in json.loads(tm.content).values()}
    return {
        **info,
        "companies_after_dedupe": len(unique),
        "duplicate_tickers": dropped,
        "ciks_not_in_sec_ticker_map": sorted(c.cik for c in unique if c.cik not in in_sec),
    }


def fetch_filings_stage(
    rt: Runtime,
    run_id: str,
    *,
    exhibit_selector: ExhibitSelector = select_ex99,
    clock: Clock = _utcnow,
) -> list[FilingOutcome]:
    """Fetch every planned filing, then raise `FetchFailed` if any failed (all are tried first)."""
    lines, _ = plan.read_plan(rt.store, run_id)
    with rt.client(run_id) as client:
        outcomes = fetch_filings(
            client=client,
            store=rt.store,
            ledger=rt.ledger,
            plan_lines=lines,
            run_id=run_id,
            exhibit_selector=exhibit_selector,
            clock=clock,
        )
    failed = [o for o in outcomes if o.state == "failed"]
    if failed:
        raise FetchFailed(
            f"{len(failed)} filing(s) failed: "
            + "; ".join(f"{o.accession}: {o.error}" for o in failed)
        )
    return outcomes


def companyfacts_stage(
    rt: Runtime, run_id: str, *, clock: Clock = _utcnow
) -> dict[str, str | None]:
    _, summary = plan.read_plan(rt.store, run_id)
    with rt.client(run_id) as client:
        results = fetch_companyfacts(
            client=client,
            store=rt.store,
            ledger=rt.ledger,
            ciks=summary["companyfacts_refresh"],
            clock=clock,
        )
    errors = {c: e for c, e in results.items() if e}
    if errors:
        raise FetchFailed(f"companyfacts failed: {errors}")
    return results


def validate_stage(rt: Runtime, run_id: str, *, full_audit: bool = False) -> ValidationReport:
    lines, summary = plan.read_plan(rt.store, run_id)
    report = validate_run(
        store=rt.store,
        ledger=rt.ledger,
        run_id=run_id,
        # The plan records the tickers it was built for (a CLI --tickers override is deliberate).
        configured_tickers=summary.get("tickers", rt.cfg.tickers),
        plan_lines=lines,
        plan_summary=summary,
        full_audit=full_audit,
    )
    rt.store.put_bytes(
        f"{layout.run_prefix(run_id)}/validation.json",
        (json.dumps(report.to_dict(), indent=2) + "\n").encode(),
        overwrite=True,
    )
    return report


def summarize_stage(
    rt: Runtime, run_id: str, *, started_monotonic: float | None = None
) -> dict[str, Any]:
    """Write fetch_summary.json from ledger and blob state, so it survives task retries.

    Wall time is measured from `started_monotonic`, or from the run_id timestamp when omitted.
    """
    lines, summary = plan.read_plan(rt.store, run_id)
    outcomes: list[FilingOutcome] = []
    for line in lines:
        if line["decision"] != "fetch":
            continue
        row = rt.ledger.get(line["accession"])
        if row is not None and row.state == "committed" and row.committed_run == run_id:
            meta = json.loads(rt.store.get_bytes(layout.meta_key(row.cik, row.accession)))
            size = sum(d["bytes"] for d in meta["documents"])
            outcomes.append(FilingOutcome(row.accession, "committed", bytes_written=size))
        elif row is not None and row.state == "committed":
            outcomes.append(FilingOutcome(row.accession, "skipped"))
        else:
            err = (row.last_error if row else None) or "never ran"
            outcomes.append(FilingOutcome(line["accession"], "failed", error=err))

    run_start = layout.ts_from_key(run_id)
    cf: dict[str, str | None] = {}
    for cik in summary["companyfacts_refresh"]:
        latest = rt.ledger.latest_companyfacts(cik)
        fresh = (
            latest
            and layout.ts_from_key(latest[0].rsplit("fetched=", 1)[1].removesuffix(".json"))
            >= run_start
        )
        cf[cik] = None if fresh else "no snapshot in this run"

    retries = 0
    if rt.store.exists(layout.FETCH_LOG_KEY):
        for raw in rt.store.get_bytes(layout.FETCH_LOG_KEY).decode().splitlines():
            entry = json.loads(raw)
            retries += entry["run_id"] == run_id and entry["attempt"] > 1
    note = "no work: plan was empty" if summary["empty"] else None
    if started_monotonic is None:
        elapsed = (datetime.now(UTC) - run_start).total_seconds()
        started_monotonic = time.monotonic() - elapsed
    return write_fetch_summary(
        rt.store,
        run_id=run_id,
        outcomes=outcomes,
        companyfacts=cf,
        retries=retries,
        started_monotonic=started_monotonic,
        note=note,
    )


def run_all_fetch(
    rt: Runtime, run_id: str, *, exhibit_selector: ExhibitSelector = select_ex99
) -> tuple[dict[str, Any], ValidationReport]:
    """CLI convenience: filings, companyfacts, validate, summary. Summary is always written."""
    started = time.monotonic()
    error: Exception | None = None
    try:
        fetch_filings_stage(rt, run_id, exhibit_selector=exhibit_selector)
    except FetchFailed as e:
        error = e
    try:
        companyfacts_stage(rt, run_id)
    except FetchFailed as e:
        error = error or e
    report = validate_stage(rt, run_id)
    summary = summarize_stage(rt, run_id, started_monotonic=started)
    if error:
        raise error
    return summary, report

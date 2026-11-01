"""`python -m sec_ingest <command>`: run the pipeline without Airflow."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from . import pipeline
from .edgar_client import EdgarError
from .ledger import rebuild_ledger
from .membership import MembershipError


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sec_ingest", description="Bronze ingestion of SEC EDGAR data")
    sub = p.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="build an immutable plan for a new run")
    plan.add_argument("--tickers", nargs="+", help="override the configured tickers")
    plan.add_argument(
        "--universe", choices=["tickers", "sp500"], help="company source (default: from config)"
    )
    plan.add_argument("--limit", type=int, help="plan only the first N companies (staged rollout)")

    sub.add_parser("universe", help="fetch and snapshot the S&P 500 list; report problems")

    fetch = sub.add_parser("fetch", help="execute the plan for a run_id")
    fetch.add_argument("--run-id", required=True)

    validate = sub.add_parser("validate", help="validate a run")
    validate.add_argument("--run-id", required=True)
    validate.add_argument(
        "--full", action="store_true", help="re-hash every committed filing, not just this run's"
    )

    sub.add_parser("rebuild-ledger", help="reconstruct the ledger from _meta.json files")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    pipeline.configure_logging()
    rt = pipeline.open_runtime()
    try:
        if args.command == "plan":
            tickers = [t.upper() for t in args.tickers] if args.tickers else None
            result = pipeline.run_plan_stage(
                rt, tickers=tickers, universe=args.universe, limit=args.limit
            )
            print(json.dumps(result.summary, indent=2))
            print(f"run_id: {result.run_id}", file=sys.stderr)
        elif args.command == "universe":
            print(json.dumps(pipeline.preview_universe_stage(rt), indent=2))
        elif args.command == "fetch":
            summary, report = pipeline.run_all_fetch(rt, args.run_id)
            print(json.dumps({"fetch": summary, "validation": report.to_dict()}, indent=2))
            return 0 if report.ok else 1
        elif args.command == "validate":
            report = pipeline.validate_stage(rt, args.run_id, full_audit=args.full)
            print(json.dumps(report.to_dict(), indent=2))
            return 0 if report.ok else 1
        elif args.command == "rebuild-ledger":
            filings, snapshots = rebuild_ledger(rt.store, rt.ledger)
            print(f"restored {filings} filings and {snapshots} companyfacts snapshots")
    except (EdgarError, pipeline.FetchFailed, MembershipError) as e:
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

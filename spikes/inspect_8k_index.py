"""Spike 1: how do we identify EX-99 exhibits for an 8-K? (design doc section 10)

Downloads a few real 8-K filing indexes (`index.json` and the `-index.htm` page) plus one 10-K and
one 10-Q index into tests/fixtures/spike1/, and prints what each one says about document types.
These files double as test fixtures and documentation of the real shapes.

    SEC_USER_AGENT="Your Name you@example.com" python spikes/inspect_8k_index.py [TICKER ...]

About 25 requests at 5 req/s. Not part of the test suite.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from sec_ingest import layout, selection
from sec_ingest.edgar_client import EdgarClient

OUT = Path("tests/fixtures/spike1")
DEFAULT_TICKERS = ["AAPL", "JPM", "XOM"]


def main(tickers: list[str]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with EdgarClient(user_agent=os.environ.get("SEC_USER_AGENT"), run_id="spike1") as client:
        tm = json.loads(client.get(layout.TICKER_MAP_URL).content)
        for ticker in tickers:
            cik = next(layout.cik10(e["cik_str"]) for e in tm.values() if e["ticker"] == ticker)
            raw = client.get(layout.submissions_url(cik)).content
            if ticker == tickers[0]:
                (OUT / f"submissions_{ticker}.json").write_bytes(raw)
            rows = selection.zip_filings(json.loads(raw)["filings"]["recent"])
            picks = [r for r in rows if r.form == "8-K"][:2]
            picks += (
                [next(r for r in rows if r.form == f) for f in ("10-K", "10-Q")]
                if ticker == tickers[0]
                else []
            )
            for r in picks:
                base = layout.filing_base_url(cik, r.accession)
                for name in ("index.json", f"{r.accession}-index.htm"):
                    body = client.get(f"{base}/{name}").content
                    (OUT / f"{ticker}_{r.form}_{r.accession}_{name}").write_bytes(body)
                items = json.loads(
                    (OUT / f"{ticker}_{r.form}_{r.accession}_index.json").read_bytes()
                )
                files = items["directory"]["item"]
                print(f"\n{ticker} {r.form} {r.accession} items={r.items!r}")
                print("  index.json keys per file:", sorted(files[0]) if files else "none")
                for f in files:
                    print("   ", f.get("name"), "| type:", f.get("type"), "| size:", f.get("size"))
    print(f"\nSaved to {OUT}.")
    print("Decide the rule, implement it in sec_ingest/exhibits.py, update design doc section 10.")


if __name__ == "__main__":
    main([t.upper() for t in sys.argv[1:]] or DEFAULT_TICKERS)

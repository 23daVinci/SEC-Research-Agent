-- Plain SQL so moving to Postgres is a driver swap. The ledger is a cache of what the blob
-- store proves (each committed filing has a _meta.json); see rebuild_ledger.

CREATE TABLE IF NOT EXISTS filings (
  accession         TEXT PRIMARY KEY,   -- dashed form
  cik               TEXT NOT NULL,      -- 10-digit string
  ticker_at_plan    TEXT,               -- convenience only, never a join key
  form              TEXT NOT NULL,      -- '10-K', '10-Q', '8-K'
  filing_date       DATE NOT NULL,
  report_date       DATE,
  primary_document  TEXT NOT NULL,
  items             TEXT,               -- 8-K item codes, raw string
  state             TEXT NOT NULL CHECK (state IN ('planned', 'committed', 'failed')),
  first_planned_run TEXT NOT NULL,      -- run_id
  committed_run     TEXT,               -- run_id
  committed_at      TIMESTAMP,
  last_error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_filings_cik_form_date ON filings (cik, form, filing_date DESC);

-- "Current" is derived, never a flag we flip: latest committed 10-K and 10-Q per company.
CREATE VIEW IF NOT EXISTS current_periodic AS
SELECT f.*
FROM filings f
WHERE f.state = 'committed'
  AND f.form IN ('10-K', '10-Q')
  AND f.filing_date = (
    SELECT MAX(g.filing_date) FROM filings g
    WHERE g.cik = f.cik AND g.form = f.form AND g.state = 'committed'
  );

CREATE TABLE IF NOT EXISTS companyfacts_snapshots (
  cik        TEXT NOT NULL,
  fetched_at TIMESTAMP NOT NULL,
  path       TEXT NOT NULL,
  sha256     TEXT NOT NULL,
  PRIMARY KEY (cik, fetched_at)
);

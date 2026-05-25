#!/usr/bin/env python3
"""
reparse_cc.py
Re-parse exhibit URLs for CC filings that have known data quality issues:
  - sample_size is wrong (e.g. '2' due to regex bug)
  - exception_count is None when it should be 0 or a number
  - findings is empty but exception_count > 0
  - provider exists but sample/findings are missing

Operates by re-fetching and re-parsing the stored exhibit_url,
then updating the procedure and filing rows in-place.
"""
import json, sqlite3, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from exhibit_parser import extract_aup_data
import aup_updater as _upd

DB_PATH = Path(__file__).parent / "aup_dashboard.db"

# CC issuers to re-examine
CC_KEYS = [
    "mission_lane", "mercury_financial", "continental_finance", "newday_funding",
    "genesis_financial", "avant_card", "access_financial", "imprint_payments",
    "fair_square", "prosper_card", "cw_nexus",
]

def needs_reparse(row) -> bool:
    """Return True if a filing row likely has stale/wrong parsed data."""
    provider = row["aup_provider"] or ""
    sample = row["sample_size"]
    exc = row["exception_count"]
    findings = row["findings_json"] or "[]"

    # No provider → was 15Ga-1 (already deleted) or unprocessed
    if not provider:
        return False
    # Known bad sample (single-digit for a large-sample AUP)
    if sample and int(sample) < 10 and provider:
        return True
    # Has provider and exceptions > 0 but no finding details
    if exc and int(exc) > 0 and findings == "[]":
        return True
    # Has provider but no exception count at all (should at least be 0)
    if provider and exc is None:
        return True
    return False


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    ph = ",".join("?" * len(CC_KEYS))
    rows = conn.execute(f"""
        SELECT f.id AS fid, f.accession_no, f.exhibit_url, f.aup_provider,
               p.id AS pid, p.sample_size, p.exception_count, p.findings_json
        FROM filings f
        LEFT JOIN procedures p ON p.filing_id = f.id
        WHERE f.issuer_key IN ({ph})
          AND f.aup_provider IS NOT NULL
          AND f.exhibit_url IS NOT NULL
        ORDER BY f.issuer_key, f.filed_date DESC
    """, CC_KEYS).fetchall()

    reparsed = 0
    skipped = 0

    for row in rows:
        if not needs_reparse(row):
            skipped += 1
            continue

        exhibit_url = row["exhibit_url"]
        acc = row["accession_no"]
        print(f"Re-parsing {acc[-10:]} (sample={row['sample_size']} exc={row['exception_count']}) ...")

        try:
            time.sleep(0.15)
            aup_data = extract_aup_data(exhibit_url)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        procs = aup_data.get("procedures", [])
        if not procs:
            print("  No procedures extracted, skipping")
            continue

        proc = procs[0]
        new_provider = aup_data.get("aup_provider") or row["aup_provider"]
        new_sample = proc.get("sample_size")
        new_exc = proc.get("exception_count")
        new_rate = proc.get("exception_rate")
        new_findings = json.dumps(proc.get("findings", []))
        new_deal = _upd._extract_deal_name(aup_data.get("raw_text") or "")

        # Update filing
        conn.execute("""
            UPDATE filings SET aup_provider=?, report_date=?
            WHERE id=?
        """, (new_provider, aup_data.get("report_date"), row["fid"]))

        # Update deal name only if we got something and current is blank/wrong
        if new_deal:
            conn.execute("""
                UPDATE filings SET deal_name=COALESCE(NULLIF(deal_name,''), ?)
                WHERE id=?
            """, (new_deal, row["fid"]))

        # Update procedure
        conn.execute("""
            UPDATE procedures
            SET sample_size=?, exception_count=?, exception_rate=?, findings_json=?
            WHERE id=?
        """, (new_sample, new_exc, new_rate, new_findings, row["pid"]))

        conn.commit()
        reparsed += 1
        print(f"  -> sample={new_sample} exc={new_exc} rate={new_rate} findings={len(proc.get('findings',[]))}")

    conn.close()
    print(f"\nDone: reparsed={reparsed}  skipped={skipped}")


if __name__ == "__main__":
    main()

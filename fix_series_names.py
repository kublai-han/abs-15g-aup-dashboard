#!/usr/bin/env python3
"""
fix_series_names.py
Re-fetch deal names for CC filings that are missing series designations.
Uses the exhibit text (not cover form) since series names appear in the AUP letter.
"""
import json, re, sqlite3, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from exhibit_parser import fetch_exhibit
import aup_updater as _upd

DB_PATH = Path(__file__).parent / "aup_dashboard.db"

# Issuers where series designations matter
CC_KEYS = [
    "mission_lane", "mercury_financial", "continental_finance", "newday_funding",
    "genesis_financial", "avant_card", "access_financial",
]

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    ph = ",".join("?" * len(CC_KEYS))
    # Get filings where deal_name has no series info but exhibit exists
    rows = conn.execute(f"""
        SELECT id, issuer_key, filed_date, accession_no, exhibit_url, deal_name, aup_provider
        FROM filings
        WHERE issuer_key IN ({ph})
          AND exhibit_url IS NOT NULL
          AND aup_provider IS NOT NULL
        ORDER BY issuer_key, filed_date DESC
    """, CC_KEYS).fetchall()

    fixed = 0
    for r in rows:
        current_deal = r["deal_name"] or ""
        # Check if series info is already present (Series YYYY-X or YYYY-REV1 etc.)
        if re.search(r"Series\s+\d{4}|YYYY-", current_deal, re.IGNORECASE):
            continue

        exhibit_url = r["exhibit_url"]
        # Skip cover-form URLs (no AUP data in them)
        fn = exhibit_url.rsplit("/", 1)[-1].lower()
        if "abs15g" in fn and "ex99" not in fn:
            continue

        print(f"  Checking {r['issuer_key']} {r['filed_date']} {r['accession_no'][-10:]} (current: {current_deal!r})")
        try:
            time.sleep(0.15)
            raw = fetch_exhibit(exhibit_url)
            new_deal = _upd._extract_deal_name(raw)
        except Exception as e:
            print(f"    ERROR: {e}")
            continue

        if new_deal and new_deal != current_deal:
            # Only update if the new deal name has a Series or is more specific
            if re.search(r"Series\s+\d{4}|[A-Z]\d|REV\d|\d{4}-\d+", new_deal, re.IGNORECASE) or (
                not current_deal and new_deal
            ):
                conn.execute("UPDATE filings SET deal_name=? WHERE id=?", (new_deal, r["id"]))
                conn.commit()
                print(f"    -> Updated: {new_deal!r}")
                fixed += 1
            else:
                print(f"    -> No series in new name either: {new_deal!r}")
        else:
            print(f"    -> No change: {new_deal!r}")

    conn.close()
    print(f"\nFixed {fixed} deal names")

if __name__ == "__main__":
    main()

"""
Re-parse all newday_funding exhibits to capture all 11 procedure fields,
not just those with exceptions.  Updates findings_json, exception_count,
exception_rate for every newday filing.
"""

import json
import sqlite3
import time
from pathlib import Path

from exhibit_parser import fetch_exhibit, parse_aup_html

DB = Path(__file__).parent / "aup_dashboard.db"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT f.id, f.deal_name, f.filed_date, f.exhibit_url,
           p.id AS pid, p.pool_size, p.sample_size, p.exception_count, p.findings_json
    FROM filings f JOIN procedures p ON p.filing_id = f.id
    WHERE f.issuer_key = 'newday_funding'
    ORDER BY f.filed_date DESC
""").fetchall()

print(f"NewDay filings to re-parse: {len(rows)}")
ok = err = 0

for r in rows:
    url = r["exhibit_url"]
    print(f"  {r['filed_date']}  {r['deal_name'][:50]} ...", end=" ", flush=True)
    try:
        html = fetch_exhibit(url)
        time.sleep(0.35)
    except Exception as e:
        print(f"FETCH ERROR: {e}")
        err += 1
        continue

    results = parse_aup_html(html)
    if not results:
        print("PARSE ERROR: no results")
        err += 1
        continue

    p = results[0]
    findings = p.get("findings") or []
    exc = p.get("exception_count") or 0
    rate = p.get("exception_rate")

    # Compute distinct field count (unique section names across findings)
    distinct_fields = len({f.split(": ")[0] if ": " in f else f for f in findings})

    # Keep existing pool_size / sample_size — only update findings fields
    conn.execute(
        "UPDATE procedures SET findings_json=?, exception_count=?, exception_rate=?, fields_count=? WHERE id=?",
        (json.dumps(findings) if findings else None, exc, rate, distinct_fields or None, r["pid"]),
    )
    conn.commit()

    pass_ct = sum(1 for f in findings if f.lower().endswith(": no exception"))
    exc_ct = len(findings) - pass_ct
    print(f"fields={len(findings)} distinct={distinct_fields} (pass={pass_ct} exc={exc_ct})  exc_count={exc}")
    ok += 1

conn.execute("UPDATE filings SET raw_text = NULL")
conn.execute("UPDATE procedures SET raw_text = NULL")
conn.commit()
conn.execute("VACUUM")
conn.close()

print(f"\nDone: {ok} updated, {err} errors")

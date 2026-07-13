"""
Post-ingest cleanup for Commercial MBS filings:

1. Extract deal_name, pool size (loan count) with CMBS-specific patterns
   (KPMG/EY "Re: X - Data File Procedures", PwC "with respect to the X,
   Commercial Mortgage Pass-Through Certificates, Series ..." formats).
2. Normalize aup_provider capitalization.
3. Reclassify: conduit-shelf filings on a single mortgage loan are
   single-asset/single-borrower deals -> asset_type 'large_loan'.
4. Junk-skip rows with neither a deal name nor any procedure data
   (move to skipped_filings so they are not re-fetched).
"""

import json
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from exhibit_parser import fetch_exhibit

DB = Path(__file__).parent / "aup_dashboard.db"


def p(s):
    sys.stdout.buffer.write((s + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


def html_to_text(html: str) -> str:
    import html as _html
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", html)))


DEAL_PATTERNS = [
    # KPMG/EY: "Re: <name> - Data File Procedures"
    re.compile(r"Re:\s*(.{6,100}?)\s*[–—-]\s*Data\s+File", re.IGNORECASE),
    # EY: "Re: <name> (the "Issuing Entity")"
    re.compile(r"Re:\s*(.{6,100}?)\s*\(the\s+[\"“]Issuing\s+Entity", re.IGNORECASE),
    # PwC: "related to the <name>, Commercial Mortgage Pass-Through"
    re.compile(r"(?:related\s+to|with\s+respect\s+to)\s+the\s+"
               r"([A-Z][A-Za-z0-9 .,&()\-]{4,90}?),?\s+Commercial\s+Mortgage\s+Pass-Through",),
    # "offering by <name>," (KPMG body)
    re.compile(r"offering\s+by\s+([A-Z][A-Za-z0-9 .,&()\-]{4,80}?\d{4}-[A-Z0-9]+(?:\s+(?:LLC|Mortgage\s+Trust|Trust))?)"),
    # generic trust token: "Xxxx Trust 2026-ABC" / "XXXX 2026-C11 Mortgage Trust"
    re.compile(r"([A-Z][A-Za-z0-9 .&\-]{2,60}?(?:Mortgage\s+)?Trust\s+\d{4}-[A-Z0-9]+"
               r"|[A-Z][A-Za-z0-9 .&\-]{2,40}?\s+\d{4}-[A-Z0-9]+\s+Mortgage\s+Trust"
               r"|MF1\s+\d{4}-[A-Z0-9]+\s+LLC)"),
]

WORDS1 = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
          "seven": 7, "eight": 8, "nine": 9, "ten": 10}

POOL_PATTERNS = [
    r"information\s+(?:relating\s+to|on|for)\s+(?:the\s+)?([\d,]+|one|two|three|four|five|six|seven|eight|nine|ten)\s+"
    r"(?:fixed[- ]rate\s+|floating[- ]rate\s+)?(?:commercial\s+)?(?:mortgage\s+loans?|whole\s+loans?|collateral\s+interests?|loans?)",
    r"containing\s+(?:certain\s+)?information\s+.{0,60}?([\d,]+|one)\s+mortgage\s+loans?",
    r"([\d,]+|one|two|three)\s+(?:commercial\s+)?mortgage\s+loans?\s+\(the\s+[\"“]Mortgage\s+Loans?",
    r"([\d,]+)\s+collateral\s+interests\s+\(",
]

FIRM_PATTERNS = [
    (r"Ernst\s*&\s*Young", "Ernst & Young LLP"),
    (r"KPMG", "KPMG LLP"),
    (r"PricewaterhouseCoopers|PwC", "PricewaterhouseCoopers LLP"),
    (r"Deloitte", "Deloitte & Touche LLP"),
    (r"Grant\s+Thornton", "Grant Thornton LLP"),
]


def extract_deal(text: str) -> str | None:
    head = text[:6000]
    for pat in DEAL_PATTERNS:
        m = pat.search(head)
        if m:
            name = m.group(1).strip().rstrip(",").strip()
            name = re.sub(r"^(?:the|The)\s+", "", name)
            if 6 <= len(name) <= 95 and not name.lower().startswith("certain"):
                return name
    return None


def extract_pool(text: str) -> int | None:
    head = text[:8000]
    for pat in POOL_PATTERNS:
        m = re.search(pat, head, re.IGNORECASE)
        if m:
            v = m.group(1).lower().replace(",", "")
            if v in WORDS1:
                return WORDS1[v]
            if v.isdigit() and 1 <= int(v) <= 10_000:
                return int(v)
    return None


def extract_provider(text: str) -> str | None:
    head = text[:3000]
    for pat, canon in FIRM_PATTERNS:
        if re.search(pat, head, re.IGNORECASE):
            return canon
    return None


conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Optional: pass a fetched_at cutoff (ISO date) to only process rows
# ingested on/after that date, e.g.  python fix_cmbs.py 2026-07-13
_cutoff = sys.argv[1] if len(sys.argv) > 1 else None
_where_extra = " AND f.fetched_at >= ?" if _cutoff else ""
rows = conn.execute(f"""
    SELECT f.id, f.accession_no, f.issuer_key, f.filed_date, f.deal_name, f.aup_provider,
           f.asset_type, f.exhibit_url,
           (SELECT p.id FROM procedures p WHERE p.filing_id = f.id LIMIT 1) AS pid
    FROM filings f
    WHERE f.asset_type IN ('conduit', 'cre_clo'){_where_extra}
    ORDER BY f.filed_date DESC
""", (_cutoff,) if _cutoff else ()).fetchall()

p(f"CMBS filings to process: {len(rows)}")
now = datetime.now(timezone.utc).isoformat()
stats = {"deal": 0, "pool": 0, "large_loan": 0, "junked": 0, "fetch_fail": 0}

for i, r in enumerate(rows):
    try:
        html = fetch_exhibit(r["exhibit_url"])
        time.sleep(0.25)
    except Exception as e:
        stats["fetch_fail"] += 1
        p(f"  FETCH FAIL {r['issuer_key']} {r['filed_date']}: {e}")
        continue

    text = html_to_text(html)
    deal = extract_deal(text)
    pool = extract_pool(text)
    provider = extract_provider(text) or r["aup_provider"]

    # Junk: no deal name extracted AND no procedure row AND no pool
    if not deal and r["pid"] is None and not pool:
        conn.execute("DELETE FROM procedures WHERE filing_id=?", (r["id"],))
        conn.execute("DELETE FROM filings WHERE id=?", (r["id"],))
        conn.execute(
            "INSERT OR IGNORE INTO skipped_filings"
            " (accession_no, issuer_key, filed_date, reason, created_at)"
            " VALUES (?,?,?,?,?)",
            (r["accession_no"], r["issuer_key"], r["filed_date"],
             "no usable AUP data (cmbs triage)", now),
        )
        stats["junked"] += 1
        conn.commit()
        continue

    # Classification: conduit shelf + single mortgage loan = SASB / large-loan
    new_type = r["asset_type"]
    if r["asset_type"] == "conduit" and pool == 1:
        new_type = "large_loan"
        stats["large_loan"] += 1

    if deal:
        stats["deal"] += 1
    if pool:
        stats["pool"] += 1

    conn.execute(
        "UPDATE filings SET deal_name=COALESCE(?, deal_name), aup_provider=?, asset_type=? WHERE id=?",
        (deal, provider, new_type, r["id"]),
    )
    if pool and r["pid"] is not None:
        conn.execute(
            "UPDATE procedures SET pool_size=?, sample_size=COALESCE(sample_size, ?) WHERE filing_id=?",
            (str(pool), str(pool), r["id"]),
        )
    conn.commit()

    if i % 50 == 0:
        p(f"  [{i}/{len(rows)}] {r['issuer_key']:18s} {r['filed_date']}  deal={deal or 'MISS'}")

p(f"\nStats: {stats}")
conn.commit()
conn.close()
p("Done")

"""
Fix missing pool_size for credit card ABS deals.

Patterns by issuer group:
  Deloitte/Atlanticus (access_financial, mercury): "with respect to N general-purpose revolving credit card accounts"
  Deloitte (avant_card, mission_lane older): "listing with respect to N credit card receivable accounts"
  Deloitte (genesis, imprint): "listing with respect to N revolving private label credit card accounts"
  Deloitte (newday): "containing an account number for each of the N account numbers in the Loan Pool"
  PwC (prosper_card): "listing of N Receivables"
"""

import sqlite3, urllib.request, re, html as _html, time
from pathlib import Path

HEADERS = {"User-Agent": "AUP Dashboard kennethjhan@gmail.com"}
DB = Path(__file__).parent / "aup_dashboard.db"


def fetch_text(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        raw = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", errors="replace")
        text = _html.unescape(re.sub(r"<[^>]+>", " ", raw))
        return re.sub(r"\s+", " ", text)
    except Exception:
        return None


POOL_PATTERNS = [
    # Atlanticus/Deloitte: "with respect to N general-purpose revolving credit card accounts"
    r"with\s+respect\s+to\s+([\d,]+)\s+(?:general[- ]purpose\s+revolving\s+)?credit\s+card\s+(?:receivable\s+)?accounts",
    # Deloitte older: "listing with respect to N general purpose revolving credit card accounts"
    r"listing\s+with\s+respect\s+to\s+([\d,]+)\s+general\s+(?:purpose\s+)?(?:revolving\s+)?credit\s+card\s+accounts",
    # Deloitte: "listing with respect to N revolving/private label credit card accounts"
    r"listing\s+with\s+respect\s+to\s+([\d,]+)\s+(?:revolving\s+)?(?:private\s+label\s+)?credit\s+card\s+(?:receivable\s+)?accounts",
    # NewDay: "account number for each of the N account numbers/credit card receivables in the Loan Pool"
    r"account\s+number\s+for\s+each\s+of\s+the\s+([\d,]+)\s+(?:account\s+numbers|credit\s+card\s+receivables)\s+in\s+the\s+Loan\s+Pool",
    # PwC Prosper: "listing of N Receivables"
    r"listing\s+of\s+([\d,]+)\s+Receivables\s+provided",
    # Generic: "N credit card accounts (the 'Statistical Data File')"
    r"([\d,]+)\s+(?:general[- ]purpose\s+revolving\s+|revolving\s+)?credit\s+card\s+(?:receivable\s+)?accounts\s+\(",
    # Generic: "N credit card receivables"
    r"([\d,]+)\s+credit\s+card\s+receivables?\s+\(",
    # Generic large fallback: "N account numbers in the Loan Pool"
    r"([\d,]+)\s+account\s+numbers\s+in\s+the\s+Loan",
]


def extract_pool(text):
    for pat in POOL_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val_str = m.group(1).replace(",", "")
            if val_str.isdigit():
                val = int(val_str)
                if 1_000 <= val <= 50_000_000:
                    return val, pat
    return None, None


conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT f.id, f.issuer_key, f.deal_name, f.filed_date, f.exhibit_url, p.sample_size
    FROM filings f JOIN procedures p ON p.filing_id = f.id
    WHERE f.asset_type = 'credit_card'
      AND (p.pool_size IS NULL OR p.pool_size = '')
    ORDER BY f.issuer_key, f.filed_date DESC
""").fetchall()

print(f"Credit card deals missing pool_size: {len(rows)}")
fixed = 0
skipped = 0

for r in rows:
    text = fetch_text(r["exhibit_url"])
    time.sleep(0.35)
    if not text:
        print(f"  FAIL  {r['issuer_key']} {r['filed_date']} {r['deal_name']}")
        skipped += 1
        continue

    pool, pat = extract_pool(text)
    if pool:
        conn.execute("UPDATE procedures SET pool_size=? WHERE filing_id=?", (str(pool), r["id"]))
        conn.commit()
        fixed += 1
        print(f"  OK  {r['issuer_key']:<20} {r['filed_date']}  pool={pool:,}  {r['deal_name']}")
    else:
        print(f"  MISS {r['issuer_key']:<20} {r['filed_date']}  {r['deal_name']}")
        skipped += 1

print(f"\nDone: {fixed} fixed, {skipped} skipped/failed")
conn.execute("UPDATE filings SET raw_text = NULL")
conn.execute("UPDATE procedures SET raw_text = NULL")
conn.commit()
conn.execute("VACUUM")
conn.close()

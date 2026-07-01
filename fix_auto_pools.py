"""
Fix missing pool_size for auto ABS deals where the exhibit discloses a total count.

Issuers where pool count IS in the exhibit:
  - santander_drive: "with respect to N automobile receivables"
  - consumer_portfolio (CPS): "containing information on/related to N ... contracts/receivables"
  - prestige_financial: same CPS pattern

Issuers where pool count is NOT in exhibit text (skip):
  - ally_auto, ford_credit, stellantis, avis_budget
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


# Patterns ordered most-specific first
POOL_PATTERNS = [
    # Santander Drive: "with respect to 75,667 automobile receivables/loans"
    r"with\s+respect\s+to\s+([\d,]+)\s+automobile\s+(?:receivables?|loans?)",
    # Santander: "listing with respect to N automobile loans"
    r"listing\s+with\s+respect\s+to\s+([\d,]+)\s+automobile\s+(?:receivables?|loans?)",
    # CPS / Prestige: "containing information on/related to N automobile retail installment..."
    r"containing\s+information\s+(?:on|related\s+to)\s+([\d,]+)\s+(?:automobile\s+)?(?:retail\s+installment\s+sale\s+|retail\s+installment\s+)?(?:contracts?|receivables?)",
    # Santander older variant: "consisting of N automobile receivables"
    r"consisting\s+of\s+([\d,]+)\s+automobile\s+receivables",
    # Generic fallback: "N automobile receivables (the 'Initial Receivables')"
    r"([\d,]+)\s+automobile\s+receivables?\s+\(the",
    # Generic: "N retail installment sale contracts (the 'Receivables')"
    r"([\d,]+)\s+retail\s+installment\s+sale\s+contracts?\s+\(the",
]

ISSUERS_TO_FIX = {"santander_drive", "consumer_portfolio", "prestige_financial"}


def extract_pool(text):
    for pat in POOL_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val_str = m.group(1).replace(",", "")
            if val_str.isdigit():
                val = int(val_str)
                if 500 <= val <= 999_999:
                    return val, pat
    return None, None


conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT f.id, f.issuer_key, f.deal_name, f.filed_date, f.exhibit_url, p.sample_size
    FROM filings f JOIN procedures p ON p.filing_id = f.id
    WHERE f.asset_type = 'auto'
      AND f.issuer_key IN ('santander_drive', 'consumer_portfolio', 'prestige_financial')
      AND (p.pool_size IS NULL OR p.pool_size = '')
    ORDER BY f.issuer_key, f.filed_date DESC
""").fetchall()

print(f"Auto deals to fix: {len(rows)}")
fixed = 0
skipped = 0

for r in rows:
    text = fetch_text(r["exhibit_url"])
    time.sleep(0.35)
    if not text:
        print(f"  FETCH FAIL  {r['issuer_key']} {r['filed_date']} {r['deal_name']}")
        skipped += 1
        continue

    pool, pat = extract_pool(text)
    if pool:
        conn.execute("UPDATE procedures SET pool_size=? WHERE filing_id=?", (str(pool), r["id"]))
        conn.commit()
        fixed += 1
        print(f"  OK  {r['issuer_key']} {r['filed_date']}  pool={pool:,}  {r['deal_name']}")
    else:
        print(f"  MISS {r['issuer_key']} {r['filed_date']}  {r['deal_name']}")
        skipped += 1

print(f"\nDone: {fixed} fixed, {skipped} skipped/failed")
conn.execute("UPDATE filings SET raw_text = NULL")
conn.execute("UPDATE procedures SET raw_text = NULL")
conn.commit()
conn.execute("VACUUM")
conn.close()

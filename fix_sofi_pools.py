"""Fix pool_size for all SoFi deals with missing pool."""
import sqlite3, urllib.request, re, html as _html, time
from pathlib import Path

HEADERS = {"User-Agent": "AUP Dashboard kennethjhan@gmail.com"}
DB = Path(__file__).parent / "aup_dashboard.db"

def fetch_text(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        raw = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", errors="replace")
        text = _html.unescape(re.sub(r"<[^>]+>", " ", raw))
        return re.sub(r"\s+", " ", text)
    except:
        return None

def extract_pool(text):
    patterns = [
        r"([\d,]+)\s+loans\s+represented\s+on\s+the\s+(?:Initial\s+)?Data\s+Tape",
        r"selected\s+(?:the\s+Sample\s+Loans\s+)?from\s+the\s+([\d,]+)\s+loans",
        r"([\d,]+)\s+(?:private\s+(?:consumer|student)\s+)?loans\s+(?:in\s+the|on\s+the)\s+(?:Initial\s+)?Data\s+Tape",
        r"pool\s+of\s+(?:assets\s+)?(?:on\s+)?(?:the\s+)?(?:Initial\s+)?Data\s+Tape[^.]{0,60}?([\d,]+)\s+loans",
        r"Initial\s+Data\s+Tape\s+(?:identified[^.]+)?contained\s+([\d,]+)\s+loans",
        r"Data\s+Tape[^.]{0,30}([\d,]+)\s+(?:private\s+)?(?:consumer\s+|student\s+)?loans",
        r"([\d,]+)\s+loans\s+that\s+will\s+collateralize",
        r"portfolio\s+of\s+([\d,]+)\s+(?:private\s+)?(?:consumer\s+|student\s+)?loans",
        r"([\d,]+)\s+(?:private\s+)?(?:consumer\s+|student\s+)?loans\s+\(the\s+[\"']?(?:Data|Statistical|Initial)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = int(m.group(1).replace(",", ""))
            if val > 100:
                return val, pat
    return None, None

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

filings = conn.execute("""
    SELECT f.id, f.deal_name, f.filed_date, f.exhibit_url, p.sample_size
    FROM filings f JOIN procedures p ON p.filing_id=f.id
    WHERE f.issuer_key='sofi' AND (p.pool_size IS NULL OR p.pool_size='')
    ORDER BY f.filed_date DESC
""").fetchall()

print(f"SoFi deals missing pool_size: {len(filings)}")

fixed = 0
for f in filings:
    text = fetch_text(f["exhibit_url"])
    time.sleep(0.35)
    if not text:
        print(f"  id={f['id']} {f['filed_date']}: FETCH FAILED")
        continue

    pool, pat = extract_pool(text)
    if pool:
        conn.execute("UPDATE procedures SET pool_size=? WHERE filing_id=?", (str(pool), f["id"]))
        conn.commit()
        fixed += 1
        print(f"  id={f['id']} {f['filed_date']}: pool={pool}  {f['deal_name']}")
    else:
        # Try to find any plausible number
        candidates = []
        for m in re.finditer(r"([\d,]+)", text):
            raw_val = m.group(1).replace(",", "")
            if raw_val.isdigit():
                val = int(raw_val)
                sample = int(f["sample_size"]) if f["sample_size"] else 0
                if val > max(sample * 2, 500) and val < 500000:
                    ctx = text[max(0, m.start()-80):m.end()+30]
                    if any(kw in ctx.lower() for kw in ["loan", "tape", "pool", "data file", "collateral"]):
                        candidates.append((val, ctx[:80]))
        if candidates:
            best = candidates[0][0]
            print(f"  id={f['id']} {f['filed_date']}: candidate pool={best} ctx={candidates[0][1][:60]}  {f['deal_name']}")
            conn.execute("UPDATE procedures SET pool_size=? WHERE filing_id=?", (str(best), f["id"]))
            conn.commit()
            fixed += 1
        else:
            print(f"  id={f['id']} {f['filed_date']}: NO MATCH  {f['deal_name']}")

print(f"\nFixed {fixed}/{len(filings)} SoFi pool sizes")
conn.execute("VACUUM")
conn.close()

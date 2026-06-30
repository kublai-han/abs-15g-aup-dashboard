"""
Fix filings.exhibit_url that point to local file:// paths instead of
EDGAR URLs. Resolves each via accession_no, then attempts to backfill
missing pool_size/fields_count from the real exhibit text.
"""
import sqlite3
import urllib.request
import re
import html as _html
import time
from pathlib import Path

HEADERS = {"User-Agent": "AUP Dashboard kennethjhan@gmail.com"}
DB = Path(__file__).parent / "aup_dashboard.db"


def fetch_text(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        raw = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", errors="replace")
        text = _html.unescape(re.sub(r"<[^>]+>", " ", raw))
        return re.sub(r"\s+", " ", text)
    except Exception:
        return None


def get_exhibits(cik, accn):
    acc_nodash = accn.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{accn}-index.htm"
    raw = urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=20).read().decode()
    links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.htm)"', raw, re.IGNORECASE)
    return [
        (l.rsplit("/", 1)[-1], "https://www.sec.gov" + l)
        for l in links if "index" not in l.rsplit("/", 1)[-1].lower()
    ]


def extract_pool(text):
    patterns = [
        r"([\d,]+)\s+(?:unsecured\s+)?consumer\s+loans?\s*\(",
        r"listing\s+of\s+([\d,]+)\s+[Ll]oans",
        r"total\s+of\s+([\d,]+)\s+(?:secured\s+and\s+unsecured\s+)?receivables",
        r"containing\s+information\s+on\s+([\d,]+)\s+as\s+of",
        r"pool\s+of\s+([\d,]+)\s+(?:loans?|receivables?)",
        r"([\d,]+)\s+[Ll]oans\s+provided\s+to\s+us",
        r"with\s+respect\s+to\s+([\d,]+)\s+(?:unsecured\s+)?(?:consumer\s+)?loans",
        r"approximately\s+([\d,]+)\s+loans",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                val = int(m.group(1).replace(",", ""))
                if val > 50:
                    return val
            except ValueError:
                continue
    return None


def extract_fields(text):
    m = re.search(r"\bthe\s+(\d{1,2})\s+(?:fields?|characteristics?|attributes?)\b", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    for marker in ["Characteristics", "Attribute"]:
        pos = text.find(marker)
        if pos < 0:
            continue
        section = text[pos:pos + 2500]
        items = re.findall(r"\b(\d{1,2})\.\s+[A-Z]", section)
        nums = [int(x) for x in items]
        if nums:
            return max(nums)
    sa = re.findall(r"Specified Attribute\s+(\d+)", text)
    if sa:
        return max(int(x) for x in sa)
    return None


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    filings = conn.execute("""
        SELECT f.id, f.issuer_key, f.deal_name, f.filed_date, f.cik, f.accession_no,
               p.pool_size, p.fields_count
        FROM filings f JOIN procedures p ON p.filing_id = f.id
        WHERE f.exhibit_url LIKE 'file:%'
        ORDER BY f.issuer_key, f.filed_date DESC
    """).fetchall()

    print(f"Filings with local URLs to fix: {len(filings)}")

    url_fixed = 0
    pool_fixed = 0
    fields_fixed = 0
    errors = 0

    for i, f in enumerate(filings):
        try:
            exhibits = get_exhibits(f["cik"], f["accession_no"])
        except Exception:
            errors += 1
            continue

        ex99 = next((url for fn, url in exhibits if "ex99" in fn.lower() and "abs15g" not in fn.lower()), None)
        if not ex99:
            ex99 = next((url for fn, url in exhibits if "abs15g" not in fn.lower()), None)
        if not ex99:
            errors += 1
            continue

        text = fetch_text(ex99)
        time.sleep(0.3)
        if not text:
            errors += 1
            continue

        conn.execute("UPDATE filings SET exhibit_url=? WHERE id=?", (ex99, f["id"]))
        url_fixed += 1
        changes = ["url"]

        if not f["pool_size"]:
            pool = extract_pool(text)
            if pool:
                conn.execute("UPDATE procedures SET pool_size=? WHERE filing_id=?", (str(pool), f["id"]))
                pool_fixed += 1
                changes.append(f"pool={pool}")

        if f["fields_count"] is None:
            fields = extract_fields(text)
            if fields:
                conn.execute("UPDATE procedures SET fields_count=? WHERE filing_id=?", (fields, f["id"]))
                fields_fixed += 1
                changes.append(f"fields={fields}")

        conn.commit()
        print(f"  [{i+1}/{len(filings)}] {f['issuer_key']} {f['filed_date']} -> {', '.join(changes)}")

    print(f"\nDone: {url_fixed} URLs fixed, {pool_fixed} pool_size filled, {fields_fixed} fields_count filled, {errors} errors")

    conn.execute("UPDATE filings SET raw_text = NULL")
    conn.execute("UPDATE procedures SET raw_text = NULL")
    conn.commit()
    conn.execute("VACUUM")
    conn.close()


if __name__ == "__main__":
    main()

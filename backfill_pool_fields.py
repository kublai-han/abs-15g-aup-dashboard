"""
Backfill missing pool_size and fields_count across all ABS filings
by re-fetching exhibits from EDGAR and applying broad extraction patterns.
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


def extract_pool_size(text, sample_size=None):
    """Try multiple patterns to find the total pool/population size."""
    patterns = [
        r"([\d,]+)\s+(?:unsecured\s+|secured\s+(?:and\s+unsecured\s+)?)?consumer\s+loans?\s+\(the\s+[“\"]Statistical",
        r"total\s+of\s+([\d,]+)\s+(?:secured\s+and\s+unsecured\s+)?(?:consumer\s+)?receivables",
        r"(?:portfolio|population|pool)\s+of\s+([\d,]+)\s+(?:consumer\s+)?(?:loans?|receivables?|contracts?|accounts?)",
        r"containing\s+(?:data\s+(?:as\s+)?(?:represented\s+to\s+us\s+by\s+the\s+Company,?\s+)?)?(?:as\s+of[^,]+,\s+)?with\s+respect\s+to\s+([\d,]+)\s+(?:consumer\s+)?loans?",
        r"([\d,]+)\s+(?:student\s+)?loans?\s+(?:and\s+their|contained|in\s+the\s+Data\s+File)",
        r"Data\s+File\s+(?:containing|provided|with)[^.]*?([\d,]+)\s+(?:consumer\s+)?(?:loans?|receivables?|accounts?)",
        r"([\d,]+)\s+(?:total\s+)?(?:receivables?|accounts?|contracts?)\s+(?:as\s+of|in\s+the\s+(?:Data|Loan)\s+File)",
    ]
    candidates = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            try:
                val = int(m.group(1).replace(",", ""))
                if val > 50 and (not sample_size or val > sample_size):
                    candidates.append(val)
            except (ValueError, IndexError):
                continue
    if candidates:
        # Most common / first found that's plausible
        return candidates[0]
    return None


def extract_fields_count(text):
    """Try to find the count of tested characteristics/attributes/fields."""
    # Pattern 1: "the N fields" or "N characteristics" stated explicitly
    m = re.search(r"\bthe\s+(\d{1,2})\s+(?:fields?|characteristics?|attributes?)\b", text, re.IGNORECASE)
    if m:
        return int(m.group(1))

    m = re.search(r"\binvolved\s+the\s+\w+\s+\((\d{1,2})\)\s+fields?", text, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # Pattern 2: numbered list "1. X 2. Y ... N. Z" near "Characteristics" or "Attribute"
    for marker in ["Characteristics", "Attribute"]:
        pos = text.find(marker)
        if pos < 0:
            continue
        section = text[pos:pos + 2000]
        items = re.findall(r"\b(\d{1,2})\.\s+[A-Z]", section)
        nums = [int(x) for x in items]
        if nums:
            # Use the max value reached in a monotonically-ish increasing sequence
            return max(nums)

    # Pattern 3: "Specified Attribute N" max
    sa = re.findall(r"Specified Attribute\s+(\d+)", text)
    if sa:
        return max(int(x) for x in sa)

    return None


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    filings = conn.execute("""
        SELECT f.id, f.issuer_key, f.deal_name, f.filed_date, f.exhibit_url,
               p.pool_size, p.sample_size, p.fields_count
        FROM filings f JOIN procedures p ON p.filing_id = f.id
        WHERE f.exhibit_url LIKE 'http%'
          AND ((p.pool_size IS NULL OR p.pool_size = '') OR p.fields_count IS NULL)
        ORDER BY f.issuer_key, f.filed_date DESC
    """).fetchall()

    print(f"Filings to process: {len(filings)}")

    pool_fixed = 0
    fields_fixed = 0
    errors = 0

    for i, f in enumerate(filings):
        text = fetch_text(f["exhibit_url"])
        time.sleep(0.3)

        if not text:
            errors += 1
            continue

        changes = []

        if not f["pool_size"]:
            sample = None
            try:
                sample = int(f["sample_size"]) if f["sample_size"] else None
            except (ValueError, TypeError):
                pass
            pool = extract_pool_size(text, sample)
            if pool:
                conn.execute("UPDATE procedures SET pool_size=? WHERE filing_id=?", (str(pool), f["id"]))
                pool_fixed += 1
                changes.append(f"pool={pool}")

        if f["fields_count"] is None:
            fields = extract_fields_count(text)
            if fields:
                conn.execute("UPDATE procedures SET fields_count=? WHERE filing_id=?", (fields, f["id"]))
                fields_fixed += 1
                changes.append(f"fields={fields}")

        if changes:
            conn.commit()
            print(f"  [{i+1}/{len(filings)}] {f['issuer_key']} {f['filed_date']} -> {', '.join(changes)}")
        elif (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(filings)}] processed ({pool_fixed} pool, {fields_fixed} fields so far)")

    print(f"\nDone: {pool_fixed} pool_size fixed, {fields_fixed} fields_count fixed, {errors} fetch errors")

    conn.execute("UPDATE filings SET raw_text = NULL")
    conn.execute("UPDATE procedures SET raw_text = NULL")
    conn.commit()
    conn.execute("VACUUM")
    conn.close()


if __name__ == "__main__":
    main()

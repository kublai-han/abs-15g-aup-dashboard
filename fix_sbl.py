"""
Clean up small_business_loan filings after initial ingest:

1. Repair exhibit URLs stored under the wrong CIK (readycap: updater used the
   last CIK in the list when building EFTS URLs) and re-parse.
2. First Citizens: repoint from the 5.4MB Exhibit 99.5 loan tape to the
   Exhibit 99.1 Deloitte AUP letter and re-parse.
3. Recompute deal_name, aup_provider, pool_size for every SBL filing using
   SBL-specific patterns.
"""

import json
import re
import sqlite3
import sys
import time
from pathlib import Path

from exhibit_parser import extract_aup_data, fetch_exhibit

DB = Path(__file__).parent / "aup_dashboard.db"


def p(s):
    sys.stdout.buffer.write((s + "\n").encode("utf-8", errors="replace"))
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# URL repairs
# ---------------------------------------------------------------------------
URL_FIXES = {
    # readycap: accessions filed under CIK 1795936, URLs built with 2137901
    "https://www.sec.gov/Archives/edgar/data/2137901/000153949719002241/exh99-1.htm":
        "https://www.sec.gov/Archives/edgar/data/1795936/000153949719002241/exh99-1.htm",
    "https://www.sec.gov/Archives/edgar/data/2137901/000092963823002016/exhibit993-1.htm":
        "https://www.sec.gov/Archives/edgar/data/1795936/000092963823002016/exhibit993-1.htm",
    # first_citizens: 99.5 is the raw loan tape; 99.1 is the Deloitte AUP letter
    "https://www.sec.gov/Archives/edgar/data/2128731/000119312526262000/d228051dex995.htm":
        "https://www.sec.gov/Archives/edgar/data/2128731/000119312526262000/d228051dex991.htm",
}

# ---------------------------------------------------------------------------
# Deal name patterns (checked in order)
# ---------------------------------------------------------------------------
DEAL_PATTERNS = [
    # KPMG "Re: <name> - Data File Procedures"
    (re.compile(r"Re:\s*(.{10,120}?)\s*[–—-]\s*Data\s+File", re.IGNORECASE), None),
    # Deloitte "proposed offering of <Trust name YYYY-X>"
    (re.compile(
        r"(?:proposed\s+)?offering\s+of\s+"
        r"([A-Z][A-Za-z0-9 .,()\-]*?(?:Trust|LLC)[A-Za-z0-9 .,()\-]*?\d{4}-[A-Z0-9]+)",
    ), None),
    # CBIZ/Mulligan "<Entity LLC> - Asset-Backed Notes, Series YYYY-N"
    (re.compile(
        r"([A-Z][A-Za-z ]+Asset\s+Securitization(?:\s+[IVX]+)?\s+LLC)"
        r"\s*[–—-]?\s*Asset-Backed\s+Notes,\s+Series\s+(\d{4}-\d+)",
    ), "series"),
    # Grant Thornton/Kalamata "<Entity, LLC>'s (the "Issuer") issuance of Asset-Backed Notes, Series YYYY-N"
    (re.compile(
        r"([A-Z][A-Za-z0-9 .,]+?LLC).{0,60}?issuance\s+of\s+"
        r"Asset-Backed\s+Notes,\s+Series\s+(\d{4}-\d+)",
    ), "series"),
]

# ---------------------------------------------------------------------------
# Provider detection — first known firm mentioned in the exhibit
# ---------------------------------------------------------------------------
FIRM_PATTERNS = [
    (r"Deloitte\s*&\s*Touche\s+LLP", "Deloitte & Touche LLP"),
    (r"KPMG\s+LLP", "KPMG LLP"),
    (r"KPMG", "KPMG LLP"),
    (r"Grant\s+Thornton", "Grant Thornton LLP"),
    (r"CBIZ\s+MHM", "CBIZ MHM, LLC"),
    (r"AMC\s+Diligence", "AMC Diligence, LLC"),
    (r"PricewaterhouseCoopers|PwC", "PricewaterhouseCoopers LLP"),
    (r"Ernst\s*&\s*Young", "Ernst & Young LLP"),
    (r"BDO\s+USA", "BDO USA"),
    (r"RSM\s+US", "RSM US LLP"),
]

# ---------------------------------------------------------------------------
# Pool size patterns
# ---------------------------------------------------------------------------
POOL_PATTERNS = [
    # KPMG: "containing information on 2,335 small business receivables..."
    r"information\s+on\s+([\d,]+)\s+(?:small\s+business|merchant|SBA|business|loan|receivab)",
    # Deloitte: "consisting of N small business loans" / "N loans (the ..."
    r"consist(?:ing|ed|s)\s+of\s+([\d,]+)\s+(?:unguaranteed\s+)?(?:small\s+business\s+|SBA\s+)?loans",
    r"listing\s+of\s+([\d,]+)\s+(?:small\s+business\s+|SBA\s+)?loans",
    r"([\d,]+)\s+(?:small\s+business|SBA(?:\s+7\(a\))?)\s+loans\s+\(",
    r"portfolio\s+of\s+([\d,]+)\s+(?:small\s+business\s+|SBA\s+)?loans",
    # CBIZ/Mulligan: "population of N ..."
    r"population\s+of\s+([\d,]+)\s",
    r"([\d,]+)\s+(?:loan\s+)?receivables\s+\(",
]


def html_to_text(html: str) -> str:
    import html as _html
    text = _html.unescape(re.sub(r"<[^>]+>", " ", html))
    return re.sub(r"\s+", " ", text)


def extract_deal(text: str, issuer_key: str) -> str | None:
    for pat, mode in DEAL_PATTERNS:
        m = pat.search(text)
        if m:
            if mode == "series":
                return f"{m.group(1).strip()}, Series {m.group(2)}"
            name = m.group(1).strip().rstrip(",").strip()
            # Drop trailing " Notes" from KPMG "Re: ... Notes - Data File"
            name = re.sub(r"\s+Notes$", "", name)
            return name
    return None


def extract_provider(text: str) -> str | None:
    head = text[:4000]
    for pat, canon in FIRM_PATTERNS:
        if re.search(pat, head, re.IGNORECASE):
            return canon
    for pat, canon in FIRM_PATTERNS:
        if re.search(pat, text, re.IGNORECASE):
            return canon
    return None


def extract_pool(text: str) -> int | None:
    for pat in POOL_PATTERNS:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            v = m.group(1).replace(",", "")
            if v.isdigit() and 50 <= int(v) <= 5_000_000:
                return int(v)
    return None


conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT f.id, f.issuer_key, f.filed_date, f.deal_name, f.exhibit_url,
           p.id AS pid, p.pool_size, p.sample_size, p.exception_count
    FROM filings f LEFT JOIN procedures p ON p.filing_id = f.id
    WHERE f.asset_type = 'small_business_loan'
    ORDER BY f.issuer_key, f.filed_date
""").fetchall()

p(f"SBL filings to fix: {len(rows)}")

for r in rows:
    url = URL_FIXES.get(r["exhibit_url"], r["exhibit_url"])
    url_changed = url != r["exhibit_url"]

    try:
        html = fetch_exhibit(url)
        time.sleep(0.3)
    except Exception as e:
        p(f"  FETCH FAIL {r['issuer_key']} {r['filed_date']}: {e}")
        continue

    text = html_to_text(html)
    deal = extract_deal(text, r["issuer_key"])
    provider = extract_provider(text)
    pool = extract_pool(text)

    conn.execute(
        "UPDATE filings SET exhibit_url=?, deal_name=COALESCE(?, deal_name),"
        " aup_provider=COALESCE(?, aup_provider) WHERE id=?",
        (url, deal, provider, r["id"]),
    )

    # Re-parse procedures for repaired URLs or rows missing them entirely
    if url_changed or r["pid"] is None:
        try:
            aup = extract_aup_data(url)
            conn.execute("DELETE FROM procedures WHERE filing_id=?", (r["id"],))
            for proc in aup.get("procedures", []):
                conn.execute(
                    """INSERT INTO procedures
                       (filing_id, procedure_number, description, pool_size,
                        sample_size, exception_count, exception_rate,
                        findings_json, raw_text, fields_count)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (r["id"], proc.get("procedure_number"), proc.get("description"),
                     proc.get("pool_size"), proc.get("sample_size"),
                     proc.get("exception_count"), proc.get("exception_rate"),
                     json.dumps(proc.get("findings", [])), None,
                     proc.get("fields_count")),
                )
        except Exception as e:
            p(f"  REPARSE FAIL {r['issuer_key']} {r['filed_date']}: {e}")

    if pool:
        conn.execute(
            "UPDATE procedures SET pool_size=? WHERE filing_id=?",
            (str(pool), r["id"]),
        )
    conn.commit()

    p(f"  {r['issuer_key']:16s} {r['filed_date']}  deal={deal or 'MISS':50s} prov={(provider or 'MISS'):24s} pool={pool or 'MISS'}")

conn.execute("UPDATE filings SET raw_text = NULL")
conn.execute("UPDATE procedures SET raw_text = NULL")
conn.commit()
conn.execute("VACUUM")
conn.close()
p("Done")

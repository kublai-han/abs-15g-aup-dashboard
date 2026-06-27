"""
Re-ingest all MBS filings to extract TPR grade distributions.

For each MBS filing:
1. Scan all exhibits in the filing index
2. Find the TPR report (Clayton/Visionet/AMC/Clarifii) — skip KPMG/Deloitte/EY
3. Parse A/B/C/D grade percentages
4. Update procedures table with grades, reviewer, sample size
5. Update filings table with TPR exhibit URL and reviewer as aup_provider
"""
import sqlite3
import urllib.request
import re
import html as _html
import time
import json
from pathlib import Path

from mbs_parser import parse_grade_distribution

HEADERS = {"User-Agent": "AUP Dashboard kennethjhan@gmail.com"}
DB = Path(__file__).parent / "aup_dashboard.db"

MBS_TYPES = {"nqm", "second_lien", "npl", "sfr", "rtl", "mortgage"}

ACCOUNTING_FIRMS = {"kpmg", "deloitte", "pricewaterhousecoopers", "pwc", "ernst & young",
                    "ernst &", "ey llp", "grant thornton", "rsm", "crowe", "baker tilly", "bdo"}

TPR_FIRMS = {"clayton", "visionet", "amc", "situsamc", "consolidated analytics",
             "clarifii", "digital risk", "recovco", "covius"}


def fetch_text(url):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        raw = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", errors="replace")
        text = _html.unescape(re.sub(r"<[^>]+>", " ", raw))
        return re.sub(r"\s+", " ", text)
    except Exception:
        return None


def is_tpr_exhibit(text_snippet):
    tl = text_snippet[:2000].lower()
    return any(firm in tl for firm in TPR_FIRMS)


def is_accounting_aup(text_snippet):
    tl = text_snippet[:2000].lower()
    if "agreed-upon procedures" in tl or "agreed upon procedures" in tl:
        return any(firm in tl for firm in ACCOUNTING_FIRMS)
    return False


def get_filing_exhibits(cik, accn):
    acc_nodash = accn.replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{accn}-index.htm"
    raw = urllib.request.urlopen(
        urllib.request.Request(index_url, headers=HEADERS), timeout=20
    ).read().decode()
    links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.htm)"', raw, re.IGNORECASE)
    base = "https://www.sec.gov"
    return [
        (l.rsplit("/", 1)[-1], base + l)
        for l in links
        if "index" not in l.rsplit("/", 1)[-1].lower()
    ]


def find_tpr_and_parse(cik, accn):
    """Find TPR exhibit in a filing and parse grade distribution."""
    try:
        exhibits = get_filing_exhibits(cik, accn)
    except Exception:
        return None, None

    # Skip the cover form (abs15g.htm)
    exhibits = [(fn, url) for fn, url in exhibits if "abs15g" not in fn.lower()]

    # Try each exhibit — look for TPR content with grade tables
    for fn, url in exhibits:
        text = fetch_text(url)
        time.sleep(0.25)
        if not text:
            continue

        # Skip accounting firm AUPs
        if is_accounting_aup(text):
            continue

        # Check if it's a TPR report
        if not is_tpr_exhibit(text):
            # Also check for grade tables without explicit TPR branding
            has_grades = any(kw in text.lower() for kw in [
                "overall grade migration", "overall loan results",
                "overall results summary", "nrsro grade",
            ])
            if not has_grades:
                continue

        result = parse_grade_distribution(text)
        if result:
            return url, result

    return None, None


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # Get all MBS filings
    filings = conn.execute("""
        SELECT f.id, f.issuer_key, f.cik, f.accession_no, f.filed_date,
               f.exhibit_url, f.deal_name
        FROM filings f
        JOIN procedures p ON p.filing_id = f.id
        WHERE f.issuer_key IN (
            SELECT DISTINCT key FROM (
                SELECT issuer_key as key FROM filings
                WHERE asset_type IN ('nqm','second_lien','npl','sfr','rtl','mortgage')
            )
        )
        ORDER BY f.issuer_key, f.filed_date DESC
    """).fetchall()

    # Simpler: just get all MBS filings by asset_type
    filings = conn.execute("""
        SELECT f.id, f.issuer_key, f.cik, f.accession_no, f.filed_date,
               f.exhibit_url, f.deal_name, f.asset_type
        FROM filings f
        JOIN procedures p ON p.filing_id = f.id
        WHERE f.asset_type IN ('nqm','second_lien','npl','sfr','rtl','mortgage')
          AND p.grade_a_pct IS NULL
        ORDER BY f.issuer_key, f.filed_date DESC
    """).fetchall()

    print(f"MBS filings to process: {len(filings)}")

    updated = 0
    skipped = 0
    no_tpr = 0

    for i, f in enumerate(filings):
        fid = f["id"]
        cik = f["cik"]
        accn = f["accession_no"]

        tpr_url, result = find_tpr_and_parse(cik, accn)

        if not result:
            no_tpr += 1
            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(filings)}] processed ({updated} updated, {no_tpr} no TPR)")
            continue

        # Update filings table
        conn.execute(
            "UPDATE filings SET exhibit_url=?, aup_provider=? WHERE id=?",
            (tpr_url, result["reviewer"], fid),
        )

        # Update procedures table with grades
        conn.execute("""
            UPDATE procedures
            SET grade_a_pct=?, grade_b_pct=?, grade_c_pct=?, grade_d_pct=?,
                sample_size=?
            WHERE filing_id=?
        """, (
            result["grade_a"], result["grade_b"],
            result["grade_c"], result["grade_d"],
            str(result["sample"]) if result["sample"] else None,
            fid,
        ))

        # Update deal name if parser found one and DB doesn't have one
        if result.get("deal_name") and not f["deal_name"]:
            conn.execute(
                "UPDATE filings SET deal_name=? WHERE id=?",
                (result["deal_name"], fid),
            )

        conn.commit()
        updated += 1
        print(
            f"  [{i+1}/{len(filings)}] {f['issuer_key']} {f['filed_date']}: "
            f"A={result['grade_a']:.1f}% B={result['grade_b']:.1f}% "
            f"C={result['grade_c']:.1f}% D={result['grade_d']:.1f}% "
            f"sample={result['sample']} reviewer={result['reviewer']}"
        )

        time.sleep(0.2)

    print(f"\nDone: {updated} updated, {no_tpr} no TPR found, {skipped} skipped")
    print(f"Total MBS filings: {len(filings)}")

    # Clear raw_text to keep DB small
    conn.execute("UPDATE filings SET raw_text = NULL")
    conn.execute("UPDATE procedures SET raw_text = NULL")
    conn.commit()
    conn.execute("VACUUM")

    conn.close()


if __name__ == "__main__":
    main()

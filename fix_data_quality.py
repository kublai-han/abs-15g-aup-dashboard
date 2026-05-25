#!/usr/bin/env python3
"""
fix_data_quality.py
Comprehensive data quality fixes for CC and other AUP filings:
  1. Re-parse NewDay exc=None filings (fix "with no exception" detection)
  2. Re-parse Continental Finance exc=None filings
  3. Re-parse Fair Square exc=None filings
  4. Fix Access Financial 2024 (bad exhibit_url → cover form)
  5. Fix exception_rate=None where exc>0 and sample>0
  6. Clean noisy findings (confidence interval boilerplate, etc.)
  7. Fix Continental Finance deal names
"""
import json, re, sqlite3, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from exhibit_parser import extract_aup_data, fetch_exhibit
import aup_updater as _upd

DB_PATH = Path(__file__).parent / "aup_dashboard.db"

# ── 1. Re-parse filings with exc=None that have known patterns ────────────────

REPARSE_ISSUERS = [
    "newday_funding", "continental_finance", "fair_square",
    "access_financial", "cw_nexus",
]

def reparse_exc_none(conn):
    """Re-parse filings where exc=None but aup_provider is set."""
    ph = ",".join("?" * len(REPARSE_ISSUERS))
    rows = conn.execute(f"""
        SELECT f.id AS fid, f.issuer_key, f.filed_date, f.accession_no,
               f.exhibit_url, f.cik, f.aup_provider,
               p.id AS pid, p.sample_size, p.exception_count, p.findings_json
        FROM filings f
        LEFT JOIN procedures p ON p.filing_id = f.id
        WHERE f.issuer_key IN ({ph})
          AND f.aup_provider IS NOT NULL
          AND p.exception_count IS NULL
        ORDER BY f.issuer_key, f.filed_date DESC
    """, REPARSE_ISSUERS).fetchall()

    fixed = 0
    for row in rows:
        exhibit_url = row["exhibit_url"] or ""

        # If exhibit_url points to cover form, try to resolve actual Exhibit 99.1
        fn = exhibit_url.rsplit("/", 1)[-1].lower()
        is_cover = (
            not fn
            or ("abs15g" in fn and "ex99" not in fn)
            or fn.endswith("a.htm")  # e.g. a81516554a.htm (Access Financial cover)
        )
        # Try EFTS lookup for better exhibit URL
        if is_cover or not exhibit_url:
            acc = row["accession_no"]
            cik = row["cik"]
            better_url = _upd._find_ex99_1_url(cik, acc)
            if better_url and better_url != exhibit_url:
                print(f"  Resolved exhibit: {row['issuer_key']} {row['filed_date']} -> {better_url.rsplit('/',1)[-1]}")
                conn.execute("UPDATE filings SET exhibit_url=? WHERE id=?", (better_url, row["fid"]))
                conn.commit()
                exhibit_url = better_url

        if not exhibit_url:
            print(f"  SKIP {row['issuer_key']} {row['filed_date']}: no exhibit URL")
            continue

        print(f"  Re-parsing {row['issuer_key']} {row['filed_date']} ...", end=" ", flush=True)
        try:
            time.sleep(0.15)
            aup = extract_aup_data(exhibit_url)
        except Exception as e:
            print(f"ERROR: {e}")
            continue

        procs = aup.get("procedures", [])
        if not procs:
            print("no procedures")
            continue
        proc = procs[0]
        new_exc = proc.get("exception_count")
        new_rate = proc.get("exception_rate")
        new_sample = proc.get("sample_size")
        new_findings = json.dumps(proc.get("findings", []))
        new_provider = aup.get("aup_provider") or row["aup_provider"]

        # Only update if we now have a definitive exc value
        if new_exc is None:
            print(f"still None (sample={new_sample})")
            continue

        # Update procedure row
        if row["pid"]:
            conn.execute("""
                UPDATE procedures
                SET exception_count=?, exception_rate=?, sample_size=COALESCE(?,sample_size),
                    findings_json=?
                WHERE id=?
            """, (new_exc, new_rate, new_sample, new_findings, row["pid"]))
        else:
            conn.execute("""
                INSERT INTO procedures (filing_id, sample_size, exception_count, exception_rate, findings_json)
                VALUES (?, ?, ?, ?, ?)
            """, (row["fid"], new_sample, new_exc, new_rate, new_findings))

        # Update provider if changed
        conn.execute("UPDATE filings SET aup_provider=? WHERE id=?", (new_provider, row["fid"]))
        conn.commit()
        fixed += 1
        print(f"exc={new_exc} rate={new_rate} findings={len(proc.get('findings',[]))}")

    return fixed


# ── 2. Compute missing exception_rates ────────────────────────────────────────

def compute_missing_rates(conn):
    """Fill in exception_rate where exc>0 and sample>0 but rate is NULL."""
    rows = conn.execute("""
        SELECT p.id, p.exception_count, p.sample_size
        FROM procedures p
        WHERE p.exception_count IS NOT NULL
          AND p.exception_count > 0
          AND p.exception_rate IS NULL
          AND p.sample_size IS NOT NULL
          AND p.sample_size != ''
    """).fetchall()
    fixed = 0
    for row in rows:
        try:
            sample = int(row["sample_size"])
            if sample > 0:
                rate = row["exception_count"] / sample
                conn.execute("UPDATE procedures SET exception_rate=? WHERE id=?", (rate, row["id"]))
                fixed += 1
        except (ValueError, ZeroDivisionError):
            pass
    conn.commit()
    return fixed


# ── 3. Clean noisy findings from known boilerplate patterns ───────────────────

_NOISE_PATTERNS = re.compile(
    r"^findings?\s+are\s+as\s+described\s+herein"
    r"|^discrepancies\s+between\s+the\s+(?:Database|database)\s+and\s+the\s+Source"
    r"|^findings?\s+are\s+included\s+in\s+Attachment"
    r"|^findings?\s+with\s+respect\s+to\s*:"
    r"|errors?,?\s+within\s+a\s+total\s+population"
    r"|errors?\s+within\s+the\s+total\s+population"
    r"|^findings?:$"
    r"|^exceptions?$"
    r"|^exception\s+if\s+(?:there\s+was\s+a\s+difference|differences\s+are\s+greater)"
    r"|confidence\s+level|presuming|could\s+be\s+observed"
    r"|error\s+rate\s+in\s+the\s+population",
    re.IGNORECASE,
)

def clean_noisy_findings(conn):
    """Remove boilerplate findings lines from all procedure rows."""
    rows = conn.execute("SELECT id, findings_json FROM procedures WHERE findings_json IS NOT NULL").fetchall()
    cleaned = 0
    for row in rows:
        try:
            findings = json.loads(row["findings_json"])
        except Exception:
            continue
        filtered = [f for f in findings if not _NOISE_PATTERNS.search(f)]
        # Restore "No exceptions noted" if all filtered out and proc was clean
        if not filtered and findings:
            # Keep at least something if all were boilerplate
            pass
        if filtered != findings:
            conn.execute("UPDATE procedures SET findings_json=? WHERE id=?",
                         (json.dumps(filtered), row["id"]))
            cleaned += 1
    conn.commit()
    return cleaned


# ── 4. Fix Continental Finance deal names ─────────────────────────────────────

def fix_continental_deal_names(conn):
    """Fix deal names that start with 'Report Of' or are MISSING for Continental Finance."""
    rows = conn.execute("""
        SELECT f.id, f.filed_date, f.exhibit_url, f.deal_name
        FROM filings f
        WHERE f.issuer_key = 'continental_finance'
          AND (f.deal_name IS NULL OR f.deal_name LIKE 'Report%' OR f.deal_name = '')
    """).fetchall()
    fixed = 0
    for row in rows:
        # Continental Finance always uses the same trust name
        # 2016-2022: Continental Finance Credit Card ABS Master Trust Series YYYY-A
        # try to extract from exhibit or use known pattern
        exhibit_url = row["exhibit_url"] or ""
        if not exhibit_url:
            continue
        try:
            time.sleep(0.15)
            raw = fetch_exhibit(exhibit_url)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(raw, "html.parser")
            text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
            # Try to find "Continental Finance Credit Card ABS Master Trust, Series YYYY-?"
            m = re.search(
                r"Continental\s+Finance\s+Credit\s+Card\s+(?:ABS\s+)?Master\s+Trust"
                r"(?:[,\s]+Series\s+(\d{4}-[A-Z0-9]+))?",
                text, re.IGNORECASE
            )
            if m:
                trust = "Continental Finance Credit Card ABS Master Trust"
                if m.group(1):
                    trust += f", Series {m.group(1)}"
                if trust != (row["deal_name"] or ""):
                    conn.execute("UPDATE filings SET deal_name=? WHERE id=?", (trust, row["id"]))
                    conn.commit()
                    print(f"  Continental {row['filed_date']}: {trust!r}")
                    fixed += 1
        except Exception as e:
            print(f"  Continental {row['filed_date']}: ERROR {e}")
    return fixed


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("=== Step 1: Re-parse exc=None filings ===")
    n = reparse_exc_none(conn)
    print(f"  Fixed: {n}")

    print()
    print("=== Step 2: Compute missing exception rates ===")
    n = compute_missing_rates(conn)
    print(f"  Computed rates for: {n} procedures")

    print()
    print("=== Step 3: Clean noisy findings ===")
    n = clean_noisy_findings(conn)
    print(f"  Cleaned: {n} procedure rows")

    print()
    print("=== Step 4: Fix Continental Finance deal names ===")
    n = fix_continental_deal_names(conn)
    print(f"  Fixed: {n} deal names")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

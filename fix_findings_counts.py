"""
fix_findings_counts.py

Fixes all existing findings in the DB:
  1. Deduplicate findings_json (remove short label duplicates like "APR" alongside
     "One difference in APR")
  2. Re-label NewDay findings with section attribute names
     ("Sample Pool = 54.9; System = 49.9" → "Purchase Interest Rate: Sample Pool = 54.9; ...")
  3. Set exception_count = len(cleaned_findings) for all filings
  4. Recompute exception_rate = exception_count / sample_size

Run once after deploying the exhibit_parser.py changes.
"""

import json
import re
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from exhibit_parser import (
    _dedup_findings,
    _parse_newday_labeled,
    _sum_exception_counts_from_findings,
    fetch_exhibit,
)

DB_PATH = Path(__file__).parent / "aup_dashboard.db"


# ── 1.  Dedup all existing findings ──────────────────────────────────────────

def dedup_all_findings(conn) -> int:
    rows = conn.execute(
        "SELECT id, findings_json, exception_count FROM procedures "
        "WHERE findings_json IS NOT NULL"
    ).fetchall()

    updated = 0
    for pid, fj, exc in rows:
        try:
            findings = json.loads(fj)
        except Exception:
            continue
        if not isinstance(findings, list) or not findings:
            continue

        cleaned = _dedup_findings(findings)
        if cleaned != findings:
            conn.execute(
                "UPDATE procedures SET findings_json=? WHERE id=?",
                (json.dumps(cleaned), pid),
            )
            updated += 1

    conn.commit()
    return updated


# ── 2.  Re-label NewDay findings with section attribute names ─────────────────

_RE_NEWDAY_SAMPLE_POOL = re.compile(r"Sample\s+Pool\s*[=:]", re.IGNORECASE)


def label_newday_findings(conn) -> int:
    rows = conn.execute("""
        SELECT f.id AS fid, f.filed_date, f.exhibit_url, f.aup_provider,
               p.id AS pid, p.exception_count, p.sample_size, p.findings_json
        FROM filings f JOIN procedures p ON p.filing_id = f.id
        WHERE f.issuer_key = 'newday_funding'
          AND p.exception_count > 0
          AND p.findings_json IS NOT NULL
    """).fetchall()

    fixed = 0
    for row in rows:
        try:
            findings = json.loads(row["findings_json"])
        except Exception:
            continue

        # Only target filings whose findings look like raw "Sample Pool = X" values
        if not any(_RE_NEWDAY_SAMPLE_POOL.search(f) for f in findings):
            continue

        url = row["exhibit_url"]
        if not url:
            continue

        print(f"  Re-labelling NewDay {row['filed_date']} ...", end=" ", flush=True)
        try:
            time.sleep(0.2)
            raw_html = fetch_exhibit(url)
        except Exception as e:
            print(f"FETCH ERROR: {e}")
            continue

        labeled = _parse_newday_labeled(raw_html)
        if not labeled:
            print("no labeled sections found")
            continue

        new_exc = len(labeled)
        sample = row["sample_size"]
        try:
            new_rate = new_exc / int(sample) if sample else None
        except (ValueError, TypeError):
            new_rate = None

        conn.execute(
            "UPDATE procedures SET findings_json=?, exception_count=?, exception_rate=? "
            "WHERE id=?",
            (json.dumps(labeled), new_exc, new_rate, row["pid"]),
        )
        conn.commit()
        print(f"exc {row['exception_count']}->{new_exc}  {findings} -> {labeled}")
        fixed += 1

    return fixed


# ── 3.  Sync exception_count = len(findings) for all filings ──────────────────

def sync_exception_counts(conn) -> int:
    """
    Set exception_count = sum of stated counts in each finding description.

    "Seven differences in buyer name" + "Two differences in APR" → 7+2 = 9.
    A finding with no explicit count (e.g. NewDay's "Purchase APR: Sample Pool = X")
    contributes 1.  This is consistent with how _parse_html_tables() computes counts.
    """
    rows = conn.execute("""
        SELECT p.id, p.exception_count, p.sample_size, p.findings_json
        FROM procedures p
        WHERE p.findings_json IS NOT NULL
          AND p.exception_count IS NOT NULL
          AND p.exception_count > 0
    """).fetchall()

    updated = 0
    for row in rows:
        try:
            findings = json.loads(row["findings_json"])
        except Exception:
            continue
        if not isinstance(findings, list) or not findings:
            continue

        new_exc = _sum_exception_counts_from_findings(findings)
        if new_exc == 0:
            continue

        old_exc = row["exception_count"]
        if old_exc == new_exc:
            continue   # already correct

        try:
            new_rate = new_exc / int(row["sample_size"]) if row["sample_size"] else None
        except (ValueError, TypeError):
            new_rate = None

        conn.execute(
            "UPDATE procedures SET exception_count=?, exception_rate=? WHERE id=?",
            (new_exc, new_rate, row["id"]),
        )
        updated += 1

    conn.commit()
    return updated


# ── 4.  Fill in exception_rate where still NULL ───────────────────────────────

def fill_missing_rates(conn) -> int:
    rows = conn.execute("""
        SELECT p.id, p.exception_count, p.sample_size
        FROM procedures p
        WHERE p.exception_count IS NOT NULL
          AND p.exception_count > 0
          AND p.exception_rate IS NULL
          AND p.sample_size IS NOT NULL
    """).fetchall()

    filled = 0
    for row in rows:
        try:
            rate = row["exception_count"] / int(row["sample_size"])
            conn.execute("UPDATE procedures SET exception_rate=? WHERE id=?", (rate, row["id"]))
            filled += 1
        except (ValueError, ZeroDivisionError, TypeError):
            pass

    conn.commit()
    return filled


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("=== Step 1: Deduplicate all findings_json ===")
    n = dedup_all_findings(conn)
    print(f"  Cleaned: {n} procedure rows")

    print()
    print("=== Step 2: Re-label NewDay findings with section attribute names ===")
    n = label_newday_findings(conn)
    print(f"  Re-labelled: {n} NewDay filings")

    print()
    print("=== Step 3: Sync exception_count = len(findings) ===")
    n = sync_exception_counts(conn)
    print(f"  Updated: {n} procedure rows")

    print()
    print("=== Step 4: Fill missing exception rates ===")
    n = fill_missing_rates(conn)
    print(f"  Filled: {n} rates")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

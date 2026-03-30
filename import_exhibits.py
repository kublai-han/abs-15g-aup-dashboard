"""
import_exhibits.py

Scans local exhibit folders, matches files to filings in the DB by issuer +
filed_date, parses AUP content, and stores procedures.

Usage:
    python import_exhibits.py
"""

import logging
import re
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_DIR = Path(__file__).resolve().parent
DB_PATH = _DIR / "aup_dashboard.db"

_SCRAPER_DIR = Path(r"C:\Users\kenne\OneDrive\Documents\AI Structured Finance\AUP dashboard\AUP Scraper")

# Auto-discover: any folder named {issuer_key}_exhibits
EXHIBIT_DIRS: dict[str, Path] = {
    d.name.replace("_exhibits", ""): d
    for d in _SCRAPER_DIR.iterdir()
    if d.is_dir() and d.name.endswith("_exhibits")
}
logger.info("Exhibit dirs found: %s", list(EXHIBIT_DIRS.keys()))

# Prefer HTML over PDF (parser handles both, but HTML is cleaner for AUP letters)
EXT_PRIORITY = [".html", ".htm", ".pdf"]

# Filename pattern: {Issuer}_Ex991_{YYYY-MM-DD}_{suffix}.{ext}
_RE_FILENAME = re.compile(
    r"^(?P<issuer>[^_]+)_Ex991_(?P<date>\d{4}-\d{2}-\d{2})(?:_(?P<suffix>[^.]+))?\.(?P<ext>html?|pdf)$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _filing_by_issuer_date(conn, issuer_key: str, filed_date: str):
    """Return filing row matching issuer_key + filed_date, or None.
    Falls back to ±2 day window if exact match not found."""
    from datetime import date, timedelta
    row = conn.execute(
        "SELECT * FROM filings WHERE issuer_key = ? AND filed_date = ?",
        (issuer_key, filed_date),
    ).fetchone()
    if row:
        return row
    # Try nearby dates (filing date on EDGAR sometimes differs from exhibit date by 1-2 days)
    try:
        base = date.fromisoformat(filed_date)
    except ValueError:
        return None
    for delta in [1, -1, 2, -2]:
        candidate = (base + timedelta(days=delta)).isoformat()
        row = conn.execute(
            "SELECT * FROM filings WHERE issuer_key = ? AND filed_date = ?",
            (issuer_key, candidate),
        ).fetchone()
        if row:
            logger.debug("  Matched %s %s via date offset %+d → %s", issuer_key, filed_date, delta, candidate)
            return row
    return None


def _already_imported(conn, filing_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM procedures WHERE filing_id = ? LIMIT 1", (filing_id,)
    ).fetchone() is not None


def _store_procedures(conn, filing_id: int, issuer_key: str, filed_date: str, procedures: list):
    import json
    rows = []
    for p in procedures:
        findings = p.get("findings") or p.get("finding")
        rows.append((
            filing_id,
            p.get("procedure_number"),
            p.get("procedure_description", "") or p.get("description", ""),
            p.get("pool_size"),
            p.get("sample_size"),
            p.get("exception_count"),
            p.get("exception_rate"),
            json.dumps(findings) if isinstance(findings, (list, dict)) else (findings or ""),
            p.get("raw_text", ""),
        ))
    conn.executemany(
        """INSERT OR IGNORE INTO procedures
           (filing_id, procedure_number, description,
            pool_size, sample_size, exception_count,
            exception_rate, findings_json, raw_text)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()


def _update_filing_exhibit_url(conn, filing_id: int, path: Path):
    conn.execute(
        "UPDATE filings SET exhibit_url = ? WHERE id = ?",
        (path.as_uri(), filing_id),
    )
    conn.commit()


def _collect_files(exhibit_dir: Path) -> dict[str, list[Path]]:
    """
    Group files by (date, suffix) key, returning only the best extension per group.
    Returns dict: date -> list[Path] (one file per date+suffix combo)
    """
    groups: dict[tuple, dict[str, Path]] = {}
    for f in sorted(exhibit_dir.iterdir()):
        m = _RE_FILENAME.match(f.name)
        if not m:
            continue
        date = m.group("date")
        suffix = m.group("suffix") or ""
        ext = "." + m.group("ext").lower()
        key = (date, suffix)
        if key not in groups:
            groups[key] = {}
        groups[key][ext] = f

    # Pick best extension per group
    result: dict[str, list[Path]] = {}
    for (date, suffix), ext_map in groups.items():
        for ext in EXT_PRIORITY:
            if ext in ext_map:
                result.setdefault(date, []).append(ext_map[ext])
                break

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if str(_DIR) not in sys.path:
        sys.path.insert(0, str(_DIR))
    from exhibit_parser import extract_aup_data

    conn = _get_conn()
    total_parsed = 0
    total_skipped = 0
    total_errors = 0

    for issuer_key, exhibit_dir in EXHIBIT_DIRS.items():
        if not exhibit_dir.exists():
            logger.warning("Exhibit dir not found: %s", exhibit_dir)
            continue

        files_by_date = _collect_files(exhibit_dir)
        logger.info("%-10s — %d dates found in %s", issuer_key, len(files_by_date), exhibit_dir.name)

        for date, file_list in sorted(files_by_date.items()):
            filing = _filing_by_issuer_date(conn, issuer_key, date)
            if not filing:
                logger.debug("  No filing in DB for %s / %s — skipping", issuer_key, date)
                total_skipped += 1
                continue

            filing_id = filing["id"]

            if _already_imported(conn, filing_id):
                logger.debug("  Already imported %s / %s", issuer_key, date)
                total_skipped += 1
                continue

            # Use first file for this date (best ext already selected)
            path = file_list[0]
            logger.info("  Parsing %s", path.name)

            try:
                result = extract_aup_data(str(path))
            except Exception as exc:
                logger.error("  Parse error for %s: %s", path.name, exc)
                total_errors += 1
                continue

            if result.get("error"):
                logger.warning("  Parser warning for %s: %s", path.name, result["error"])

            procedures = result.get("procedures", [])
            logger.info("  → %d procedures extracted", len(procedures))

            _store_procedures(conn, filing_id, issuer_key, date, procedures)
            _update_filing_exhibit_url(conn, filing_id, path)
            total_parsed += 1

    conn.close()
    logger.info("Done. parsed=%d  skipped=%d  errors=%d", total_parsed, total_skipped, total_errors)


if __name__ == "__main__":
    main()

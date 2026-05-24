#!/usr/bin/env python3
"""
fetch_credit_cards.py

One-time script to populate the database with credit card ABS AUP data.
Targets discrete-deal credit card issuers that file 15Ga-2 AUP reports
(Exhibit 99.1) on Form ABS-15G.

Usage:
    python fetch_credit_cards.py                     # all CC issuers
    python fetch_credit_cards.py mission_lane        # specific issuer
    python fetch_credit_cards.py --dry-run           # list filings, no DB writes
    python fetch_credit_cards.py --limit 5           # max 5 filings per issuer
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
_DIR = Path(__file__).resolve().parent
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))

from issuers import CREDIT_CARD_ISSUERS
from exhibit_parser import extract_aup_data
import aup_updater as _upd   # reuse fetch/DB helpers

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = _DIR / "aup_dashboard.db"


# ---------------------------------------------------------------------------
# Main fetch logic
# ---------------------------------------------------------------------------

def fetch_issuer(issuer_key: str, issuer_info: dict, conn, dry_run: bool = False, limit: int = 0) -> dict:
    """Fetch and store ABS-15G filings for one credit card issuer."""
    cik_list = [
        str(c).strip()
        for c in issuer_info.get("ciks", [issuer_info.get("cik", "")])
        if str(c).strip()
    ]

    summary = {"new": 0, "skipped": 0, "no_exhibit": 0, "errors": 0}

    all_filings = []
    for cik in cik_list:
        logger.info("Fetching filings for %s (CIK %s) ...", issuer_key, cik)
        filings = _upd.fetch_filings_for_issuer(cik, issuer_key)
        for f in filings:
            f["_cik"] = cik   # tag which CIK this came from
        all_filings.extend(filings)

    # Sort by date descending; apply limit if set
    all_filings.sort(key=lambda f: f.get("filed_date", ""), reverse=True)
    if limit:
        all_filings = all_filings[:limit]

    logger.info("  Total ABS-15G filings found: %d", len(all_filings))

    for filing in all_filings:
        accession_no = filing["accession_no"]
        if not accession_no:
            continue

        # Check if already in DB
        if _upd._filing_exists(conn, accession_no):
            logger.debug("  Already in DB: %s", accession_no)
            summary["skipped"] += 1
            continue

        cik = filing["_cik"]
        filed_date = filing.get("filed_date", "")
        form_type  = filing.get("form_type", "ABS-15G")

        logger.info("  Processing %s  filed=%s  acc=%s", issuer_key, filed_date, accession_no)

        if dry_run:
            logger.info("    [DRY RUN] would fetch exhibit")
            summary["new"] += 1
            continue

        # Find Exhibit 99.1 (the actual AUP letter)
        exhibit_url = (
            _upd._find_ex99_1_url(cik, accession_no)
            or filing.get("exhibit_url")
            or _upd._find_exhibit_url(cik, accession_no)
        )

        if not exhibit_url:
            logger.warning("  No Exhibit 99.1 found for %s / %s", issuer_key, accession_no)
            # Still store the filing record so we don't retry it every run
            _upd._insert_filing(
                conn=conn,
                issuer_key=issuer_key,
                cik=cik,
                accession_no=accession_no,
                form_type=form_type,
                filed_date=filed_date,
                period_of_report=filing.get("period_of_report", ""),
                exhibit_url=None,
                aup_data=None,
                deal_name=None,
                asset_type="credit_card",
            )
            summary["no_exhibit"] += 1
            continue

        # Fetch cover form for deal name extraction
        cover_raw = ""
        cover_url = filing.get("exhibit_url")
        if cover_url and cover_url != exhibit_url:
            try:
                from exhibit_parser import fetch_exhibit
                cover_raw = fetch_exhibit(cover_url)
            except Exception:
                pass

        deal_name = _upd._extract_deal_name(cover_raw)

        # Parse the AUP exhibit
        aup_data = None
        try:
            logger.info("  Parsing exhibit: %s", exhibit_url)
            aup_data = extract_aup_data(exhibit_url)
        except Exception as exc:
            logger.error("  Parse error for %s / %s: %s", issuer_key, accession_no, exc)
            aup_data = {"error": str(exc), "procedures": [], "raw_text": ""}
            summary["errors"] += 1

        # Fallback deal name from exhibit text
        if not deal_name and aup_data:
            deal_name = _upd._extract_deal_name(aup_data.get("raw_text") or "")

        procs = (aup_data or {}).get("procedures", [])
        provider = (aup_data or {}).get("aup_provider")
        logger.info(
            "  Parsed: provider=%s  procedures=%d  deal=%s",
            provider, len(procs), deal_name or "(none)",
        )

        try:
            _upd._insert_filing(
                conn=conn,
                issuer_key=issuer_key,
                cik=cik,
                accession_no=accession_no,
                form_type=form_type,
                filed_date=filed_date,
                period_of_report=filing.get("period_of_report", ""),
                exhibit_url=exhibit_url,
                aup_data=aup_data,
                deal_name=deal_name,
                asset_type="credit_card",
            )
            summary["new"] += 1
        except Exception as exc:
            logger.error("  DB insert error for %s / %s: %s", issuer_key, accession_no, exc)
            summary["errors"] += 1

    return summary


def main():
    parser = argparse.ArgumentParser(description="Fetch credit card ABS AUP data from EDGAR")
    parser.add_argument("issuers", nargs="*", help="Issuer keys to fetch (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="List filings without writing to DB")
    parser.add_argument("--limit", type=int, default=0, help="Max filings to process per issuer (0=all)")
    args = parser.parse_args()

    target_keys = args.issuers if args.issuers else list(CREDIT_CARD_ISSUERS.keys())

    # Validate keys
    unknown = set(target_keys) - set(CREDIT_CARD_ISSUERS.keys())
    if unknown:
        logger.error("Unknown issuer keys: %s", unknown)
        logger.error("Available: %s", sorted(CREDIT_CARD_ISSUERS.keys()))
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Credit Card AUP Fetch  %s", datetime.now(timezone.utc).isoformat())
    logger.info("Issuers: %s", target_keys)
    if args.dry_run:
        logger.info("DRY RUN — no DB writes")
    logger.info("=" * 60)

    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    _upd._ensure_schema(conn)

    totals = {"new": 0, "skipped": 0, "no_exhibit": 0, "errors": 0}

    for key in target_keys:
        info = CREDIT_CARD_ISSUERS[key]
        if not info.get("active", False):
            logger.info("Skipping inactive issuer: %s", key)
            continue

        logger.info("\n--- %s (%s) ---", info["name"], key)
        result = fetch_issuer(key, info, conn, dry_run=args.dry_run, limit=args.limit)
        for k in totals:
            totals[k] += result[k]
        logger.info(
            "  Done: new=%d skipped=%d no_exhibit=%d errors=%d",
            result["new"], result["skipped"], result["no_exhibit"], result["errors"],
        )

    conn.close()

    logger.info("\n" + "=" * 60)
    logger.info("TOTAL: new=%d  skipped=%d  no_exhibit=%d  errors=%d",
                totals["new"], totals["skipped"], totals["no_exhibit"], totals["errors"])

    if not args.dry_run and totals["new"] > 0:
        logger.info("\nVACUUMing database ...")
        import sqlite3 as _sq
        with _sq.connect(DB_PATH) as vc:
            vc.execute("VACUUM")
        logger.info("VACUUM complete.")


if __name__ == "__main__":
    main()

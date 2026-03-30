#!/usr/bin/env python3
"""
download_exhibits.py

Generic Exhibit 99.1 downloader for all issuers in the DB.
Uses filing records already stored in aup_dashboard.db to find exhibit URLs,
then downloads HTML and PDF versions to per-issuer folders.

Usage:
    python download_exhibits.py                    # all issuers
    python download_exhibits.py sofi lendingclub   # specific issuers only
    python download_exhibits.py --skip-existing    # skip already-downloaded dates
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

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
OUTPUT_BASE = Path(r"C:\Users\kenne\OneDrive\Documents\AI Structured Finance\AUP dashboard\AUP Scraper")

EDGAR_BASE = "https://www.sec.gov"
RATE_LIMIT_SEC = 0.5   # seconds between EDGAR requests

HEADERS = {
    "User-Agent": "AUP Research Tool contact@example.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}


# ---------------------------------------------------------------------------
# EDGAR helpers
# ---------------------------------------------------------------------------

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _get_exhibit_url(session: requests.Session, cik: str, accession_no: str) -> tuple[str | None, str | None]:
    """
    Fetch the filing index page and return (html_url, pdf_url) for Exhibit 99.1.
    Either may be None if not found.
    """
    acc_plain = accession_no.replace("-", "")
    cik_plain = cik.lstrip("0")
    index_url = (
        f"{EDGAR_BASE}/Archives/edgar/data/{cik_plain}/{acc_plain}/{accession_no}-index.htm"
    )

    try:
        time.sleep(RATE_LIMIT_SEC)
        resp = session.get(index_url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Could not fetch index %s: %s", index_url, exc)
        return None, None

    soup = BeautifulSoup(resp.content, "html.parser")
    table = soup.find("table", {"class": "tableFile"})
    if not table:
        return None, None

    html_url = pdf_url = None
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue
        doc_type = cols[3].get_text(strip=True).upper()
        description = cols[1].get_text(strip=True).lower()
        if "EX-99.1" not in doc_type and not ("99.1" in description and "exhibit" in description):
            continue
        link = cols[2].find("a")
        if not link:
            continue
        url = urljoin(EDGAR_BASE, link["href"])
        if url.lower().endswith(".pdf"):
            pdf_url = url
        else:
            html_url = url

    return html_url, pdf_url


def _download(session: requests.Session, url: str) -> bytes | None:
    try:
        time.sleep(RATE_LIMIT_SEC)
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as exc:
        logger.warning("Download failed %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def download_issuer(
    issuer_key: str,
    filings: list[dict],
    output_dir: Path,
    skip_existing: bool,
) -> dict:
    """Download exhibits for one issuer. Returns summary dict."""
    output_dir.mkdir(parents=True, exist_ok=True)
    session = _session()
    label = issuer_key.replace("_", " ").title()
    total = len(filings)
    ok = skipped = failed = 0

    for i, filing in enumerate(filings, 1):
        cik = filing["cik"].lstrip("0")
        acc = filing["accession_no"]
        date = filing["filed_date"]
        acc_short = acc.replace("-", "")[:10]

        logger.info("[%d/%d] %s  %s  %s", i, total, label, date, acc)

        # Check if already downloaded
        if skip_existing:
            existing = list(output_dir.glob(f"{label.replace(' ', '_')}_Ex991_{date}_*.html")) + \
                       list(output_dir.glob(f"{label.replace(' ', '_')}_Ex991_{date}_*.pdf"))
            if existing:
                logger.info("  Already downloaded — skipping")
                skipped += 1
                continue

        html_url, pdf_url = _get_exhibit_url(session, cik, acc)

        if not html_url and not pdf_url:
            logger.warning("  No Exhibit 99.1 found")
            failed += 1
            continue

        downloaded_any = False
        for url, ext in [(html_url, "html"), (pdf_url, "pdf")]:
            if not url:
                continue
            data = _download(session, url)
            if not data:
                continue
            filename = f"{label.replace(' ', '_')}_Ex991_{date}_{acc_short}.{ext}"
            (output_dir / filename).write_bytes(data)
            logger.info("  Saved %s (%.1f KB)", filename, len(data) / 1024)
            downloaded_any = True

        if downloaded_any:
            ok += 1
        else:
            failed += 1

    logger.info("%s done: ok=%d skipped=%d failed=%d", label, ok, skipped, failed)
    return {"issuer": issuer_key, "ok": ok, "skipped": skipped, "failed": failed}


def main():
    parser = argparse.ArgumentParser(description="Download AUP exhibits for all issuers")
    parser.add_argument("issuers", nargs="*", help="Issuer keys to download (default: all)")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip dates that already have a downloaded file (default: on)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get all issuers in DB
    all_keys = [r[0] for r in conn.execute(
        "SELECT DISTINCT issuer_key FROM filings ORDER BY issuer_key"
    ).fetchall()]

    target_keys = args.issuers if args.issuers else all_keys
    unknown = set(target_keys) - set(all_keys)
    if unknown:
        logger.error("Unknown issuer keys: %s. Available: %s", unknown, all_keys)
        sys.exit(1)

    # Skip issuers that already have exhibit dirs with files
    already_have = {"affirm", "oportun", "pagaya"}
    if not args.issuers:
        target_keys = [k for k in target_keys if k not in already_have]
        logger.info("Skipping already-downloaded issuers: %s", sorted(already_have))

    logger.info("Downloading exhibits for: %s", target_keys)

    summaries = []
    for key in target_keys:
        filings = [dict(r) for r in conn.execute(
            "SELECT cik, accession_no, filed_date FROM filings WHERE issuer_key = ? ORDER BY filed_date DESC",
            (key,)
        ).fetchall()]

        if not filings:
            logger.info("No filings in DB for %s — skipping", key)
            continue

        label = key.replace("_", " ").title()
        output_dir = OUTPUT_BASE / f"{key}_exhibits"
        logger.info("=== %s: %d filings → %s ===", label, len(filings), output_dir)

        summary = download_issuer(key, filings, output_dir, args.skip_existing)
        summaries.append(summary)

    conn.close()

    print("\n" + "=" * 60)
    print(f"{'Issuer':<20} {'OK':>5} {'Skipped':>8} {'Failed':>7}")
    print("-" * 60)
    for s in summaries:
        print(f"{s['issuer']:<20} {s['ok']:>5} {s['skipped']:>8} {s['failed']:>7}")
    total_ok = sum(s["ok"] for s in summaries)
    print(f"\nTotal downloaded: {total_ok} exhibits")


if __name__ == "__main__":
    main()

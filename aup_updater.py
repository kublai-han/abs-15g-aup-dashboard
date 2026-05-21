"""
aup_updater.py

Daily updater that checks SEC EDGAR for new ABS-15G filings, downloads
Exhibit 99.1 AUP letters, parses them via exhibit_parser, and stores
results in the project database.

SEC EDGAR endpoints (no authentication required):
  Submissions  : https://data.sec.gov/submissions/CIK{cik_padded}.json
  Filing index : https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany
                   &CIK={cik}&type=ABS-15G&dateb=&owner=include&count=40
  Document base: https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/

Rate limit    : max 10 requests / second (enforced by _edgar_get)
User-Agent    : required by SEC – set via EDGAR_USER_AGENT env var or
                the module-level constant USER_AGENT below.
"""

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Project-local imports
# ---------------------------------------------------------------------------
# exhibit_parser lives in the same package / directory
from exhibit_parser import extract_aup_data

# issuers.py is expected to expose:
#   ISSUERS: dict[str, dict]  where each value has at least a "cik" key.
# Example issuers.py entry:
#   ISSUERS = {
#       "ALLY_AUTO_2024": {"cik": "0001234567", "name": "Ally Auto Receivables Trust 2024"},
#   }
try:
    from issuers import ISSUERS  # type: ignore
except ImportError:
    ISSUERS: dict = {}
    logging.getLogger(__name__).warning(
        "issuers.py not found – ISSUERS is empty. "
        "Create issuers.py with an ISSUERS dict to enable updates."
    )

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USER_AGENT: str = os.environ.get(
    "EDGAR_USER_AGENT", "AUP Dashboard contact@example.com"
)

# Paths
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "aup_dashboard.db"
LOG_PATH = BASE_DIR / "logs" / "aup_updater.log"

# EDGAR endpoints
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik_padded}.json"
EDGAR_FILING_INDEX_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcompany&CIK={cik}&type=ABS-15G"
    "&dateb=&owner=include&count=40&search_text="
)
EDGAR_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/"

# Rate-limiting: 10 req/s max → enforce a minimum gap between requests
_MIN_REQUEST_INTERVAL = 0.11  # seconds  (≈ 9 req/s with headroom)
_last_request_ts: float = 0.0

# Retry strategy: back off on 429 / 5xx
_RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=1.5,          # 1.5 s, 2.25 s, 3.4 s … between retries
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=_RETRY_STRATEGY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/html, */*",
            "Accept-Encoding": "gzip, deflate",
        }
    )
    return session


_session: requests.Session = _build_session()


def _edgar_get(url: str, **kwargs) -> requests.Response:
    """
    Rate-limited GET wrapper around the shared session.

    Enforces a minimum inter-request interval to stay within EDGAR's
    10 req/s limit.  The retry strategy (exponential backoff on 429/5xx)
    is handled by the HTTPAdapter.
    """
    global _last_request_ts

    elapsed = time.monotonic() - _last_request_ts
    if elapsed < _MIN_REQUEST_INTERVAL:
        time.sleep(_MIN_REQUEST_INTERVAL - elapsed)

    logger.debug("GET %s", url)
    resp = _session.get(url, timeout=30, **kwargs)
    _last_request_ts = time.monotonic()

    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_db() -> sqlite3.Connection:
    """Open (or create) the SQLite database and ensure schema exists."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS filings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            issuer_key      TEXT    NOT NULL,
            cik             TEXT    NOT NULL,
            accession_no    TEXT    NOT NULL UNIQUE,
            form_type       TEXT,
            filed_date      TEXT,
            period_of_report TEXT,
            exhibit_url     TEXT,
            fetched_at      TEXT,
            aup_provider    TEXT,
            report_date     TEXT,
            raw_text        TEXT,
            parse_error     TEXT,
            deal_name       TEXT,
            asset_type      TEXT
        );

        CREATE TABLE IF NOT EXISTS procedures (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id       INTEGER NOT NULL REFERENCES filings(id),
            procedure_number INTEGER,
            description     TEXT,
            pool_size       TEXT,
            sample_size     TEXT,
            exception_count INTEGER,
            exception_rate  REAL,
            findings_json   TEXT,
            raw_text        TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_filings_issuer  ON filings(issuer_key);
        CREATE INDEX IF NOT EXISTS idx_filings_accno   ON filings(accession_no);
        CREATE INDEX IF NOT EXISTS idx_procedures_fid  ON procedures(filing_id);
        """
    )
    conn.commit()


def _filing_exists(conn: sqlite3.Connection, accession_no: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM filings WHERE accession_no = ?", (accession_no,)
    ).fetchone()
    return row is not None


def _insert_filing(
    conn: sqlite3.Connection,
    issuer_key: str,
    cik: str,
    accession_no: str,
    form_type: str,
    filed_date: str,
    period_of_report: str,
    exhibit_url: Optional[str],
    aup_data: Optional[dict],
    deal_name: Optional[str] = None,
    asset_type: Optional[str] = None,
) -> int:
    """Insert a filing row and its procedure rows; return the new filing id."""
    # Ensure deal_name / asset_type columns exist (idempotent migrations)
    for col, typ in (("deal_name", "TEXT"), ("asset_type", "TEXT")):
        try:
            conn.execute(f"ALTER TABLE filings ADD COLUMN {col} {typ}")
            conn.commit()
        except Exception:
            pass  # column already exists

    fetched_at = datetime.now(timezone.utc).isoformat()
    raw_text = aup_data.get("raw_text", "") if aup_data else None
    aup_provider = aup_data.get("aup_provider") if aup_data else None
    report_date = aup_data.get("report_date") if aup_data else None
    parse_error = aup_data.get("error") if aup_data else None

    cur = conn.execute(
        """
        INSERT INTO filings
            (issuer_key, cik, accession_no, form_type, filed_date,
             period_of_report, exhibit_url, fetched_at,
             aup_provider, report_date, raw_text, parse_error,
             deal_name, asset_type)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            issuer_key, cik, accession_no, form_type, filed_date,
            period_of_report, exhibit_url, fetched_at,
            aup_provider, report_date, raw_text, parse_error,
            deal_name, asset_type,
        ),
    )
    filing_id = cur.lastrowid

    procedures = (aup_data or {}).get("procedures", [])
    for proc in procedures:
        conn.execute(
            """
            INSERT INTO procedures
                (filing_id, procedure_number, description, pool_size,
                 sample_size, exception_count, exception_rate,
                 findings_json, raw_text)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                filing_id,
                proc.get("procedure_number"),
                proc.get("description"),
                proc.get("pool_size"),
                proc.get("sample_size"),
                proc.get("exception_count"),
                proc.get("exception_rate"),
                json.dumps(proc.get("findings", [])),
                proc.get("raw_text"),
            ),
        )

    conn.commit()
    logger.info(
        "Stored filing %s for issuer %s (%d procedures)",
        accession_no, issuer_key, len(procedures),
    )
    return filing_id


# ---------------------------------------------------------------------------
# EDGAR API helpers
# ---------------------------------------------------------------------------

def _pad_cik(cik: str) -> str:
    """Left-pad a CIK string to 10 digits as required by the submissions API."""
    return cik.lstrip("0").zfill(10)


def _accession_to_path(accession_no: str) -> str:
    """Convert '0001234567-24-000001' to '000123456724000001' (no dashes)."""
    return accession_no.replace("-", "")


def _find_exhibit_url(cik: str, accession_no: str) -> Optional[str]:
    """
    Retrieve the filing index and return the URL of Exhibit 99.1 (AUP letter).

    Falls back to any file whose name contains 'aup', 'exhibit', or '99'.
    """
    accession_path = _accession_to_path(accession_no)
    index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/"
        f"{accession_path}/{accession_path}-index.htm"
    )

    try:
        resp = _edgar_get(index_url)
        html = resp.text
    except Exception as exc:
        logger.warning("Could not fetch filing index %s: %s", index_url, exc)
        return None

    # Try the JSON index (more reliable)
    json_index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/"
        f"{accession_path}/{accession_path}-index.json"
    )
    try:
        resp_j = _edgar_get(json_index_url)
        idx = resp_j.json()
        for doc in idx.get("documents", []):
            doc_type = (doc.get("type") or "").lower()
            doc_file = (doc.get("filename") or "").lower()
            if "99.1" in doc_type or "aup" in doc_file or "exhibit" in doc_file:
                base = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/"
                    f"{accession_path}/"
                )
                return urljoin(base, doc.get("filename", ""))
    except Exception:
        pass  # fall through to HTML scrape

    # Fallback: scrape the HTML index for Exhibit 99.1 links
    from exhibit_parser import _import_bs4  # local import to avoid circular dep
    try:
        BeautifulSoup = _import_bs4()
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            text: str = (a.get_text() or "").lower()
            if "99.1" in text or "aup" in text or href.lower().endswith(".pdf"):
                if href.startswith("/"):
                    return "https://www.sec.gov" + href
                elif href.startswith("http"):
                    return href
    except Exception as exc:
        logger.warning("HTML index scrape failed: %s", exc)

    return None


# Markers that indicate a filing was parsed from the SEC cover form, not the AUP letter
_BAD_PROVIDER_MARKERS = ("number of securitizer", "number of depositor", "number of")

# Known legitimate accounting firms that issue AUP letters
_KNOWN_AUDIT_FIRMS = (
    "deloitte",
    "ernst & young",
    "ernst and young",
    "ey us",
    "ey llp",
    "kpmg",
    "pricewaterhousecoopers",
    "pwc",
    "grant thornton",
    "bdo usa",
    "bdo llp",
    "rsm us",
    "rsm llp",
    "moss adams",
    "crowe",
    "cohen & company",
    "cohen and company",
    "withum",
    "baker tilly",
)


def _is_bad_parse(aup_provider: Optional[str]) -> bool:
    """
    Return True if the stored aup_provider looks wrong — either a cover-form
    artifact or a non-audit entity name captured by the generic LLP/LLC pattern.
    Returns False for None/empty (which means no firm found, which is valid for
    15Ga-1 filings that have no AUP).
    """
    p = (aup_provider or "").lower()
    if not p:
        return False  # None/empty = no firm detected (OK for non-AUP filings)
    # Original cover-form boilerplate artifacts
    if any(m in p for m in _BAD_PROVIDER_MARKERS):
        return True
    # Non-empty provider that doesn't match any known accounting firm = bad parse
    return not any(firm in p for firm in _KNOWN_AUDIT_FIRMS)


def _find_ex99_1_url(cik: str, accession_no: str) -> Optional[str]:
    """
    Query EDGAR EFTS full-text index to find the Exhibit 99.1 document URL
    for an ABS-15G filing.  This is the actual AUP letter; the primary
    document is merely a cover form that references it.
    """
    try:
        search_url = (
            f"https://efts.sec.gov/LATEST/search-index?q=%22{accession_no}%22"
        )
        resp = _edgar_get(search_url)
        hits = resp.json().get("hits", {}).get("hits", [])
        cik_int = str(int(cik.lstrip("0") or "0"))
        acc_nodash = accession_no.replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/"
        for hit in hits:
            doc_id   = hit.get("_id", "")            # "0001234-26-000001:filename.htm"
            src      = hit.get("_source", {})
            desc     = (src.get("file_description") or "").upper()
            seq      = src.get("sequence", 99)
            filename = doc_id.split(":", 1)[-1] if ":" in doc_id else ""
            # Exhibit 99.1 is the non-cover document: description contains 99.1
            # or filename contains ex99, and it is NOT sequence 1 (the cover form)
            if filename and ("99.1" in desc or "EX-99" in filename.upper()) and seq != 1:
                return base + filename
        # Fallback: return seq=2 document if it looks like an exhibit
        for hit in hits:
            doc_id  = hit.get("_id", "")
            src     = hit.get("_source", {})
            seq     = src.get("sequence", 99)
            filename = doc_id.split(":", 1)[-1] if ":" in doc_id else ""
            if filename and seq == 2:
                return base + filename
    except Exception as exc:
        logger.warning("EFTS Exhibit 99.1 lookup failed for %s: %s", accession_no, exc)
    return None


_RE_DEAL_NAME = re.compile(
    r"(?:issuing entity|trust|series)[^\n]{0,5}[:\s]+"
    r"([A-Z][A-Za-z0-9 ,.\-]+(?:Trust|Series|Notes?)\s*[\d-]+[A-Z0-9\-]*)",
    re.IGNORECASE,
)
_RE_DEAL_NAME2 = re.compile(
    r"([A-Z][A-Za-z0-9 ]+"
    r"(?:Auto|Credit Card|Automobile|Receivables?|Funding)"
    r"[A-Za-z0-9 ]*(?:Trust|Series)\s*[\d]{4}-[A-Z0-9]+)",
)
# Pattern for auto AUP letters: "notes issued by <Trust Name>"
_RE_DEAL_NAME3 = re.compile(
    r"(?:issued\s+by|issuance\s+of[^.]{0,30}issued\s+by|notes\s+issued\s+by)\s+"
    r"([A-Z][A-Za-z0-9 ,.\-]+(?:Trust|Series)\s*[\d]{4}-[A-Z0-9]+)",
    re.IGNORECASE,
)


def _extract_deal_name(text: str) -> Optional[str]:
    """Extract the trust / series name from an ABS-15G document (cover or exhibit)."""
    from bs4 import BeautifulSoup
    if "<html" in text.lower():
        text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    # Collapse newlines / runs of whitespace so multi-line trust names are found
    text = re.sub(r"\s+", " ", text)
    for pat in (_RE_DEAL_NAME3, _RE_DEAL_NAME, _RE_DEAL_NAME2):
        m = pat.search(text)
        if m:
            return m.group(1).strip()
    return None


def fetch_filings_for_issuer(cik: str, issuer_key: str) -> list[dict]:
    """
    Query the EDGAR submissions API for ABS-15G filings for *cik*.

    Returns a list of filing dicts, each with keys:
        accession_no, form_type, filed_date, period_of_report
    """
    cik_padded = _pad_cik(cik)
    url = EDGAR_SUBMISSIONS_URL.format(cik_padded=cik_padded)

    try:
        resp = _edgar_get(url)
        data = resp.json()
    except requests.HTTPError as exc:
        logger.error(
            "HTTP error fetching submissions for %s (CIK %s): %s",
            issuer_key, cik, exc,
        )
        return []
    except (requests.RequestException, json.JSONDecodeError) as exc:
        logger.error(
            "Error fetching submissions for %s (CIK %s): %s",
            issuer_key, cik, exc,
        )
        return []

    filings: list[dict] = []
    recent = data.get("filings", {}).get("recent", {})

    if not recent:
        logger.info("No filings data in submissions response for CIK %s", cik)
        return filings

    accessions    = recent.get("accessionNumber", [])
    form_types    = recent.get("form", [])
    filed_dates   = recent.get("filingDate", [])
    periods       = recent.get("reportDate", [])
    primary_docs  = recent.get("primaryDocument", [])

    def _build_exhibit_url(cik_raw: str, acc: str, primary_doc: str) -> Optional[str]:
        """Construct exhibit URL directly from submissions API data — no index fetch needed."""
        if not primary_doc:
            return None
        cik_int = str(int(cik_raw.lstrip("0") or "0"))
        acc_path = acc.replace("-", "")
        return (
            f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_path}/{primary_doc}"
        )

    for i, form_type in enumerate(form_types):
        if "ABS-15G" not in (form_type or "").upper():
            continue

        acc         = accessions[i]  if i < len(accessions)   else ""
        primary_doc = primary_docs[i] if i < len(primary_docs) else ""
        filings.append(
            {
                "accession_no":     acc,
                "form_type":        form_type,
                "filed_date":       filed_dates[i] if i < len(filed_dates) else "",
                "period_of_report": periods[i]     if i < len(periods)     else "",
                "exhibit_url":      _build_exhibit_url(cik, acc, primary_doc),
            }
        )

    # EDGAR may split older filings across paginated JSON files; handle the
    # "files" list (additional pages of submission history).
    for extra_file in data.get("filings", {}).get("files", []):
        extra_url = "https://data.sec.gov/submissions/" + extra_file.get("name", "")
        if not extra_url.endswith(".json"):
            continue
        try:
            extra_resp = _edgar_get(extra_url)
            extra_data = extra_resp.json()
        except Exception as exc:
            logger.warning("Could not fetch paginated filings file %s: %s", extra_url, exc)
            continue

        extra_primary_docs = extra_data.get("primaryDocument", [])
        for i, form_type in enumerate(extra_data.get("form", [])):
            if "ABS-15G" not in (form_type or "").upper():
                continue
            acc_list  = extra_data.get("accessionNumber", [])
            fd_list   = extra_data.get("filingDate", [])
            per_list  = extra_data.get("reportDate", [])
            acc       = acc_list[i] if i < len(acc_list) else ""
            pdoc      = extra_primary_docs[i] if i < len(extra_primary_docs) else ""
            filings.append(
                {
                    "accession_no":     acc,
                    "form_type":        form_type,
                    "filed_date":       fd_list[i]  if i < len(fd_list)  else "",
                    "period_of_report": per_list[i] if i < len(per_list) else "",
                    "exhibit_url":      _build_exhibit_url(cik, acc, pdoc),
                }
            )

    logger.info(
        "Found %d ABS-15G filing(s) for issuer %s (CIK %s)",
        len(filings), issuer_key, cik,
    )
    return filings


def check_for_new_filings() -> dict:
    """
    Main daily check.

    Iterates over all issuers, fetches their EDGAR filing lists, identifies
    filings not yet in the database, downloads + parses exhibits for each
    new filing, and persists results.

    Returns
    -------
    dict
        {
            "checked_issuers": int,
            "new_filings": int,
            "errors": int,
            "details": list[dict]   # one entry per new filing
        }
    """
    summary = {
        "checked_issuers": 0,
        "new_filings": 0,
        "errors": 0,
        "details": [],
    }

    if not ISSUERS:
        logger.warning("ISSUERS dict is empty – nothing to check.")
        return summary

    conn = _get_db()

    try:
        for issuer_key, issuer_info in ISSUERS.items():
            cik_list: list[str] = [
                str(c).strip() for c in issuer_info.get("ciks", [issuer_info.get("cik", "")])
                if str(c).strip()
            ]
            if not cik_list:
                logger.warning("Issuer %s has no CIK – skipping.", issuer_key)
                continue

            summary["checked_issuers"] += 1
            filings: list[dict] = []
            for cik in cik_list:
                logger.info("Checking issuer: %s (CIK %s)", issuer_key, cik)
                try:
                    filings += fetch_filings_for_issuer(cik, issuer_key)
                except Exception as exc:
                    logger.error("Failed to fetch filings for %s CIK %s: %s", issuer_key, cik, exc)
                    summary["errors"] += 1

            for filing in filings:
                accession_no = filing["accession_no"]
                if not accession_no:
                    continue

                if _filing_exists(conn, accession_no):
                    # Re-try if: (a) stored with no exhibit_url, or (b) bad parse from cover form
                    row = conn.execute(
                        "SELECT id, exhibit_url, aup_provider FROM filings WHERE accession_no = ?",
                        (accession_no,),
                    ).fetchone()
                    needs_retry = row and (
                        not row["exhibit_url"] or _is_bad_parse(row["aup_provider"])
                    )
                    if needs_retry:
                        logger.info("Re-parsing %s (bad or missing exhibit)", accession_no)
                        try:
                            ex99_url = _find_ex99_1_url(cik, accession_no)
                            retry_url = ex99_url or filing.get("exhibit_url") or row["exhibit_url"]
                            if retry_url:
                                aup_data = extract_aup_data(retry_url)
                                procs = (aup_data or {}).get("procedures", [])
                                # Try deal name from exhibit text first, then cover form
                                deal_name = (
                                    _extract_deal_name(aup_data.get("raw_text") or "")
                                    or _extract_deal_name(filing.get("_cover_raw") or "")
                                )
                                asset_type = issuer_info.get("type")
                                conn.execute(
                                    "UPDATE filings SET exhibit_url=?, aup_provider=?, report_date=?,"
                                    " raw_text=?, parse_error=?, deal_name=COALESCE(deal_name,?),"
                                    " asset_type=COALESCE(asset_type,?) WHERE id=?",
                                    (retry_url, aup_data.get("aup_provider"), aup_data.get("report_date"),
                                     aup_data.get("raw_text"), aup_data.get("error"), deal_name,
                                     asset_type, row["id"]),
                                )
                                conn.execute("DELETE FROM procedures WHERE filing_id=?", (row["id"],))
                                for proc in procs:
                                    conn.execute(
                                        """INSERT INTO procedures
                                           (filing_id, procedure_number, description, pool_size,
                                            sample_size, exception_count, exception_rate, findings_json, raw_text)
                                           VALUES (?,?,?,?,?,?,?,?,?)""",
                                        (row["id"], proc.get("procedure_number"), proc.get("description"),
                                         proc.get("pool_size"), proc.get("sample_size"),
                                         proc.get("exception_count"), proc.get("exception_rate"),
                                         json.dumps(proc.get("findings", [])), proc.get("raw_text")),
                                    )
                                conn.commit()
                                logger.info("Re-parsed %s -> %d procedures (provider=%s)",
                                            accession_no, len(procs), aup_data.get("aup_provider"))
                                if procs:
                                    summary["new_filings"] += 1
                        except Exception as exc:
                            logger.warning("Re-parse failed for %s: %s", accession_no, exc)
                    else:
                        logger.debug("Already stored: %s", accession_no)
                    continue

                logger.info(
                    "New filing found: %s %s filed %s",
                    issuer_key, accession_no, filing["filed_date"],
                )

                # Always try Exhibit 99.1 first (real AUP letter); fall back to primary doc
                exhibit_url = (
                    _find_ex99_1_url(cik, accession_no)
                    or filing.get("exhibit_url")
                    or _find_exhibit_url(cik, accession_no)
                )

                # Fetch cover form to extract deal name (primary doc from submissions API)
                cover_raw = ""
                cover_url = filing.get("exhibit_url")
                if cover_url and cover_url != exhibit_url:
                    try:
                        from exhibit_parser import fetch_exhibit
                        cover_raw = fetch_exhibit(cover_url)
                    except Exception:
                        pass
                # Deal name will also be checked from exhibit text after parsing
                deal_name = _extract_deal_name(cover_raw)
                issuer_info = ISSUERS.get(issuer_key, {})
                asset_type  = issuer_info.get("type")

                aup_data: Optional[dict] = None

                if exhibit_url:
                    logger.info("Parsing exhibit: %s", exhibit_url)
                    try:
                        aup_data = extract_aup_data(exhibit_url)
                    except Exception as exc:
                        logger.error(
                            "Exhibit parse failed for %s / %s: %s",
                            issuer_key, accession_no, exc,
                        )
                        aup_data = {"error": str(exc), "procedures": [], "raw_text": ""}
                        summary["errors"] += 1
                else:
                    logger.warning(
                        "No Exhibit 99.1 URL found for %s / %s",
                        issuer_key, accession_no,
                    )

                # Fall back to exhibit text for deal name if cover form didn't have it
                if not deal_name and aup_data:
                    deal_name = _extract_deal_name(aup_data.get("raw_text") or "")

                # Store in DB regardless (so we don't reprocess on next run)
                try:
                    filing_id = _insert_filing(
                        conn=conn,
                        issuer_key=issuer_key,
                        cik=cik,
                        accession_no=accession_no,
                        form_type=filing["form_type"],
                        filed_date=filing["filed_date"],
                        period_of_report=filing["period_of_report"],
                        exhibit_url=exhibit_url,
                        aup_data=aup_data,
                        deal_name=deal_name,
                        asset_type=asset_type,
                    )
                except Exception as exc:
                    logger.error(
                        "DB insert failed for %s / %s: %s",
                        issuer_key, accession_no, exc,
                    )
                    summary["errors"] += 1
                    continue

                summary["new_filings"] += 1
                summary["details"].append(
                    {
                        "issuer_key":   issuer_key,
                        "accession_no": accession_no,
                        "filed_date":   filing["filed_date"],
                        "filing_id":    filing_id,
                        "exhibit_url":  exhibit_url,
                        "aup_provider": (aup_data or {}).get("aup_provider"),
                        "procedures":   len((aup_data or {}).get("procedures", [])),
                    }
                )

    finally:
        conn.close()

    logger.info(
        "Daily check complete – issuers=%d new_filings=%d errors=%d",
        summary["checked_issuers"],
        summary["new_filings"],
        summary["errors"],
    )
    return summary


def update_all_issuers() -> None:
    """
    Full update cycle: run check_for_new_filings and log a structured
    summary to both stdout and the log file.
    """
    logger.info("=" * 60)
    logger.info("AUP Dashboard – daily update started  %s", datetime.now(timezone.utc).isoformat())
    logger.info("=" * 60)

    summary = check_for_new_filings()

    logger.info("-" * 60)
    logger.info("Summary:")
    logger.info("  Issuers checked : %d", summary["checked_issuers"])
    logger.info("  New filings     : %d", summary["new_filings"])
    logger.info("  Errors          : %d", summary["errors"])

    if summary["details"]:
        logger.info("  New filing details:")
        for item in summary["details"]:
            logger.info(
                "    %-30s  acc=%-25s  filed=%s  procs=%d",
                item["issuer_key"],
                item["accession_no"],
                item["filed_date"],
                item["procedures"],
            )

    logger.info("=" * 60)
    logger.info("Daily update complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    update_all_issuers()

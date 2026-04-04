"""
aup_database.py

SQLite database layer for ABS-15G AUP dashboard data.

Database file: aup_data.db (created in the current working directory unless
an explicit path is passed to init_db / the module-level DB_PATH constant).

Tables
------
filings          -- one row per ABS-15G filing retrieved from SEC EDGAR
exhibits         -- parsed exhibit records belonging to a filing
aup_results      -- individual AUP procedure findings extracted from exhibits
data_quality_log -- DQA / data-quality check history
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH: Path = Path("aup_data.db")

# DDL -----------------------------------------------------------------------

_DDL_FILINGS = """
CREATE TABLE IF NOT EXISTS filings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer_key       TEXT    NOT NULL,
    issuer_name      TEXT    NOT NULL,
    cik              TEXT    NOT NULL,
    accession_number TEXT    NOT NULL UNIQUE,
    filed_date      TEXT,           -- ISO-8601 date string  (YYYY-MM-DD)
    period_of_report TEXT,           -- ISO-8601 date string
    form_type        TEXT    DEFAULT 'ABS-15G',
    primary_doc_url  TEXT,
    created_at       TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

_DDL_EXHIBITS = """
CREATE TABLE IF NOT EXISTS exhibits (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id      INTEGER NOT NULL REFERENCES filings(id) ON DELETE CASCADE,
    exhibit_number TEXT,
    exhibit_url    TEXT,
    exhibit_type   TEXT,
    raw_text       TEXT,
    parsed_data    TEXT,             -- JSON blob
    created_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

_DDL_AUP_RESULTS = """
CREATE TABLE IF NOT EXISTS aup_results (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    exhibit_id           INTEGER NOT NULL REFERENCES exhibits(id) ON DELETE CASCADE,
    issuer_key           TEXT    NOT NULL,
    filed_date          TEXT,
    procedure_number     TEXT,
    procedure_description TEXT,
    finding              TEXT,
    exception_count      INTEGER,
    pool_size            INTEGER,
    exception_rate       REAL,
    created_at           TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

_DDL_DATA_QUALITY_LOG = """
CREATE TABLE IF NOT EXISTS data_quality_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    check_date  TEXT    NOT NULL,
    issuer_key  TEXT    NOT NULL,
    filing_id   INTEGER REFERENCES filings(id) ON DELETE SET NULL,
    check_type  TEXT    NOT NULL,
    status      TEXT    NOT NULL,  -- e.g. 'pass', 'fail', 'warn'
    notes       TEXT,
    created_at  TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_filings_issuer_key  ON filings(issuer_key);",
    "CREATE INDEX IF NOT EXISTS idx_filings_cik          ON filings(cik);",
    "CREATE INDEX IF NOT EXISTS idx_filings_filed_date  ON filings(filed_date);",
    "CREATE INDEX IF NOT EXISTS idx_exhibits_filing_id   ON exhibits(filing_id);",
    "CREATE INDEX IF NOT EXISTS idx_aup_results_exhibit  ON aup_results(exhibit_id);",
    "CREATE INDEX IF NOT EXISTS idx_aup_results_issuer   ON aup_results(issuer_key);",
    "CREATE INDEX IF NOT EXISTS idx_dqa_issuer_key       ON data_quality_log(issuer_key);",
]

# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


@contextmanager
def _get_connection(db_path: Path = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    """
    Yield a SQLite connection with foreign-key enforcement enabled.

    The connection is committed on clean exit and rolled back on exception.
    Row factory is set to ``sqlite3.Row`` for dict-style access.
    """
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------


def init_db(db_path: Path = DB_PATH) -> None:
    """
    Create all tables and indexes if they do not already exist.

    Safe to call multiple times (idempotent).

    Parameters
    ----------
    db_path : Path, optional
        Path to the SQLite database file.  Defaults to ``aup_data.db`` in the
        current working directory.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with _get_connection(db_path) as conn:
        for ddl in (_DDL_FILINGS, _DDL_EXHIBITS, _DDL_AUP_RESULTS, _DDL_DATA_QUALITY_LOG):
            conn.execute(ddl)
        for idx_sql in _INDEXES:
            conn.execute(idx_sql)

    logger.info("Database initialised at %s", db_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    """Return current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _serialize(value: Any) -> Any:
    """JSON-serialize dicts / lists; pass everything else through unchanged."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


# ---------------------------------------------------------------------------
# filings table
# ---------------------------------------------------------------------------


def insert_filing(filing_dict: dict, db_path: Path = DB_PATH) -> int:
    """
    Upsert a filing record identified by ``accession_number``.

    On conflict the existing row is updated with the supplied values (except
    ``created_at``, which is preserved from the original insert).

    Parameters
    ----------
    filing_dict : dict
        Must contain at minimum: ``issuer_key``, ``issuer_name``, ``cik``,
        ``accession_number``.  Optional keys: ``filed_date``,
        ``period_of_report``, ``form_type``, ``primary_doc_url``.

    Returns
    -------
    int
        The ``rowid`` / ``id`` of the inserted or updated row.
    """
    required = {"issuer_key", "issuer_name", "cik", "accession_number"}
    missing = required - filing_dict.keys()
    if missing:
        raise ValueError(f"insert_filing: missing required fields: {missing}")

    sql = """
        INSERT INTO filings
            (issuer_key, issuer_name, cik, accession_number,
             filed_date, period_of_report, form_type, primary_doc_url, created_at)
        VALUES
            (:issuer_key, :issuer_name, :cik, :accession_number,
             :filed_date, :period_of_report, :form_type, :primary_doc_url, :created_at)
        ON CONFLICT(accession_number) DO UPDATE SET
            issuer_key       = excluded.issuer_key,
            issuer_name      = excluded.issuer_name,
            cik              = excluded.cik,
            filed_date      = excluded.filed_date,
            period_of_report = excluded.period_of_report,
            form_type        = excluded.form_type,
            primary_doc_url  = excluded.primary_doc_url
    """
    params = {
        "issuer_key": filing_dict["issuer_key"],
        "issuer_name": filing_dict["issuer_name"],
        "cik": filing_dict["cik"],
        "accession_number": filing_dict["accession_number"],
        "filed_date": filing_dict.get("filed_date"),
        "period_of_report": filing_dict.get("period_of_report"),
        "form_type": filing_dict.get("form_type", "ABS-15G"),
        "primary_doc_url": filing_dict.get("primary_doc_url"),
        "created_at": _now_utc(),
    }

    with _get_connection(db_path) as conn:
        cur = conn.execute(sql, params)
        # For an UPDATE the lastrowid is 0; fetch the actual id.
        if cur.lastrowid:
            return cur.lastrowid
        row = conn.execute(
            "SELECT id FROM filings WHERE accession_number = ?",
            (filing_dict["accession_number"],),
        ).fetchone()
        return row["id"]


def get_filings(
    issuer_key: Optional[str] = None,
    limit: int = 100,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """
    Query filings, optionally filtered by issuer.

    Parameters
    ----------
    issuer_key : str, optional
        If supplied, return only filings for this issuer key.
    limit : int
        Maximum number of rows to return (default 100).

    Returns
    -------
    list[dict]
        List of filing records as plain dicts, ordered by filed_date DESC.
    """
    if limit < 1:
        raise ValueError("limit must be a positive integer")

    if issuer_key:
        sql = (
            "SELECT * FROM filings WHERE issuer_key = ? "
            "ORDER BY filed_date DESC LIMIT ?"
        )
        args: tuple = (issuer_key, limit)
    else:
        sql = "SELECT * FROM filings ORDER BY filed_date DESC LIMIT ?"
        args = (limit,)

    with _get_connection(db_path) as conn:
        rows = conn.execute(sql, args).fetchall()
    return [dict(row) for row in rows]


def get_latest_filed_date(
    issuer_key: str, db_path: Path = DB_PATH
) -> Optional[str]:
    """
    Return the most recent ``filed_date`` stored for *issuer_key*.

    Useful for incremental EDGAR polling: only fetch filings newer than the
    date returned here.

    Parameters
    ----------
    issuer_key : str
        The issuer registry key (e.g. ``"pagaya"``).

    Returns
    -------
    str or None
        ISO-8601 date string (``YYYY-MM-DD``) or ``None`` if no filings exist.
    """
    sql = (
        "SELECT MAX(filed_date) AS latest FROM filings WHERE issuer_key = ?"
    )
    with _get_connection(db_path) as conn:
        row = conn.execute(sql, (issuer_key,)).fetchone()
    return row["latest"] if row else None


# ---------------------------------------------------------------------------
# exhibits table
# ---------------------------------------------------------------------------


def insert_exhibit(exhibit_dict: dict, db_path: Path = DB_PATH) -> int:
    """
    Insert a new exhibit record.

    Parameters
    ----------
    exhibit_dict : dict
        Must contain ``filing_id``.  Optional keys: ``exhibit_number``,
        ``exhibit_url``, ``exhibit_type``, ``raw_text``, ``parsed_data``
        (dict/list or JSON string).

    Returns
    -------
    int
        The ``id`` of the newly inserted exhibit row.
    """
    if "filing_id" not in exhibit_dict:
        raise ValueError("insert_exhibit: 'filing_id' is required")

    parsed_data = exhibit_dict.get("parsed_data")
    if isinstance(parsed_data, (dict, list)):
        parsed_data = json.dumps(parsed_data)

    sql = """
        INSERT INTO exhibits
            (filing_id, exhibit_number, exhibit_url, exhibit_type,
             raw_text, parsed_data, created_at)
        VALUES
            (:filing_id, :exhibit_number, :exhibit_url, :exhibit_type,
             :raw_text, :parsed_data, :created_at)
    """
    params = {
        "filing_id": exhibit_dict["filing_id"],
        "exhibit_number": exhibit_dict.get("exhibit_number"),
        "exhibit_url": exhibit_dict.get("exhibit_url"),
        "exhibit_type": exhibit_dict.get("exhibit_type"),
        "raw_text": exhibit_dict.get("raw_text"),
        "parsed_data": parsed_data,
        "created_at": _now_utc(),
    }

    with _get_connection(db_path) as conn:
        cur = conn.execute(sql, params)
        return cur.lastrowid


# ---------------------------------------------------------------------------
# aup_results table
# ---------------------------------------------------------------------------


def insert_aup_result(result_dict: dict, db_path: Path = DB_PATH) -> int:
    """
    Insert a parsed AUP procedure finding.

    Parameters
    ----------
    result_dict : dict
        Must contain ``exhibit_id`` and ``issuer_key``.  Optional keys:
        ``filed_date``, ``procedure_number``, ``procedure_description``,
        ``finding``, ``exception_count``, ``pool_size``, ``exception_rate``.

    Returns
    -------
    int
        The ``id`` of the newly inserted row.
    """
    required = {"exhibit_id", "issuer_key"}
    missing = required - result_dict.keys()
    if missing:
        raise ValueError(f"insert_aup_result: missing required fields: {missing}")

    exception_count = result_dict.get("exception_count")
    pool_size = result_dict.get("pool_size")
    exception_rate = result_dict.get("exception_rate")

    # Derive exception_rate if not explicitly provided.
    if exception_rate is None and exception_count is not None and pool_size:
        try:
            exception_rate = round(int(exception_count) / int(pool_size), 6)
        except (ZeroDivisionError, TypeError, ValueError):
            exception_rate = None

    sql = """
        INSERT INTO aup_results
            (exhibit_id, issuer_key, filed_date, procedure_number,
             procedure_description, finding, exception_count, pool_size,
             exception_rate, created_at)
        VALUES
            (:exhibit_id, :issuer_key, :filed_date, :procedure_number,
             :procedure_description, :finding, :exception_count, :pool_size,
             :exception_rate, :created_at)
    """
    params = {
        "exhibit_id": result_dict["exhibit_id"],
        "issuer_key": result_dict["issuer_key"],
        "filed_date": result_dict.get("filed_date"),
        "procedure_number": result_dict.get("procedure_number"),
        "procedure_description": result_dict.get("procedure_description"),
        "finding": result_dict.get("finding"),
        "exception_count": exception_count,
        "pool_size": pool_size,
        "exception_rate": exception_rate,
        "created_at": _now_utc(),
    }

    with _get_connection(db_path) as conn:
        cur = conn.execute(sql, params)
        return cur.lastrowid


# ---------------------------------------------------------------------------
# data_quality_log table
# ---------------------------------------------------------------------------


def log_dqa_check(check_dict: dict, db_path: Path = DB_PATH) -> int:
    """
    Append a data-quality check entry.

    Parameters
    ----------
    check_dict : dict
        Must contain ``issuer_key``, ``check_type``, ``status``.  Optional
        keys: ``check_date`` (defaults to today UTC), ``filing_id``, ``notes``.

    Returns
    -------
    int
        The ``id`` of the newly inserted row.
    """
    required = {"issuer_key", "check_type", "status"}
    missing = required - check_dict.keys()
    if missing:
        raise ValueError(f"log_dqa_check: missing required fields: {missing}")

    valid_statuses = {"pass", "fail", "warn", "skip"}
    status = check_dict["status"].lower()
    if status not in valid_statuses:
        raise ValueError(
            f"log_dqa_check: status must be one of {valid_statuses}, got '{status}'"
        )

    sql = """
        INSERT INTO data_quality_log
            (check_date, issuer_key, filing_id, check_type, status, notes, created_at)
        VALUES
            (:check_date, :issuer_key, :filing_id, :check_type, :status, :notes, :created_at)
    """
    now = _now_utc()
    params = {
        "check_date": check_dict.get("check_date", now[:10]),  # default = today (YYYY-MM-DD)
        "issuer_key": check_dict["issuer_key"],
        "filing_id": check_dict.get("filing_id"),
        "check_type": check_dict["check_type"],
        "status": status,
        "notes": check_dict.get("notes"),
        "created_at": now,
    }

    with _get_connection(db_path) as conn:
        cur = conn.execute(sql, params)
        return cur.lastrowid


# ---------------------------------------------------------------------------
# Convenience query helpers
# ---------------------------------------------------------------------------


def get_aup_results(
    issuer_key: Optional[str] = None,
    filed_date_from: Optional[str] = None,
    limit: int = 500,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """
    Return AUP result rows, optionally filtered.

    Parameters
    ----------
    issuer_key : str, optional
        Restrict to one issuer.
    filed_date_from : str, optional
        ISO-8601 date; only return results on or after this date.
    limit : int
        Row cap (default 500).
    """
    conditions: list[str] = []
    args: list[Any] = []

    if issuer_key:
        conditions.append("f.issuer_key = ?")
        args.append(issuer_key)
    if filed_date_from:
        conditions.append("f.filed_date >= ?")
        args.append(filed_date_from)

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
        SELECT
            p.id, f.issuer_key, f.filed_date, f.accession_no,
            f.deal_name, f.aup_provider, f.asset_type,
            p.procedure_number,
            p.description   AS procedure_description,
            p.findings_json AS finding,
            p.exception_count, p.pool_size, p.sample_size, p.exception_rate,
            p.fields_count
        FROM procedures p
        JOIN filings f ON f.id = p.filing_id
        {where_clause}
        ORDER BY f.filed_date DESC
        LIMIT ?
    """
    args.append(limit)

    with _get_connection(db_path) as conn:
        rows = conn.execute(sql, args).fetchall()
    return [dict(row) for row in rows]


def get_dqa_log(
    issuer_key: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 200,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """
    Return data-quality log entries, optionally filtered by issuer and/or status.
    """
    conditions: list[str] = []
    args: list[Any] = []

    if issuer_key:
        conditions.append("issuer_key = ?")
        args.append(issuer_key)
    if status:
        conditions.append("status = ?")
        args.append(status.lower())

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT * FROM data_quality_log {where_clause} ORDER BY created_at DESC LIMIT ?"
    args.append(limit)

    with _get_connection(db_path) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='data_quality_log'"
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(sql, args).fetchall()
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Entry point — smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    _TEST_DB = Path("aup_data_test.db")
    if _TEST_DB.exists():
        _TEST_DB.unlink()

    init_db(_TEST_DB)
    logger.info("Tables created.")

    filing_id = insert_filing(
        {
            "issuer_key": "pagaya",
            "issuer_name": "Pagaya Technologies",
            "cik": "0001883944",
            "accession_number": "0001883944-24-000001",
            "filed_date": "2024-02-15",
            "period_of_report": "2023-12-31",
            "form_type": "ABS-15G",
            "primary_doc_url": "https://www.sec.gov/Archives/edgar/data/1883944/000188394424000001/0001883944-24-000001-index.htm",
        },
        db_path=_TEST_DB,
    )
    logger.info("Inserted filing id=%s", filing_id)

    exhibit_id = insert_exhibit(
        {
            "filing_id": filing_id,
            "exhibit_number": "33.1",
            "exhibit_type": "AUP Report",
            "raw_text": "Sample AUP text...",
            "parsed_data": {"procedures": 12, "exceptions": 0},
        },
        db_path=_TEST_DB,
    )
    logger.info("Inserted exhibit id=%s", exhibit_id)

    result_id = insert_aup_result(
        {
            "exhibit_id": exhibit_id,
            "issuer_key": "pagaya",
            "filed_date": "2024-02-15",
            "procedure_number": "1",
            "procedure_description": "Verify borrower identity fields are present",
            "finding": "No exceptions noted.",
            "exception_count": 0,
            "pool_size": 5000,
        },
        db_path=_TEST_DB,
    )
    logger.info("Inserted AUP result id=%s", result_id)

    dqa_id = log_dqa_check(
        {
            "issuer_key": "pagaya",
            "filing_id": filing_id,
            "check_type": "completeness",
            "status": "pass",
            "notes": "All 12 procedures present in exhibit 33.1",
        },
        db_path=_TEST_DB,
    )
    logger.info("Logged DQA check id=%s", dqa_id)

    filings = get_filings(issuer_key="pagaya", db_path=_TEST_DB)
    logger.info("get_filings result: %s", filings)

    latest = get_latest_filed_date("pagaya", db_path=_TEST_DB)
    logger.info("Latest filing date for pagaya: %s", latest)

    _TEST_DB.unlink()
    logger.info("Smoke test complete — test database removed.")

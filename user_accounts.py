"""
user_accounts.py

Account storage and authentication for the dashboard's daily-alert
subscriptions.

Storage backends (chosen automatically):

1. Google Sheets — used when service-account credentials are configured.
   Durable across Streamlit Cloud redeploys. Configuration comes from
   Streamlit secrets:

       users_sheet_id = "<spreadsheet id>"

       [gcp_service_account]
       type = "service_account"
       project_id = "..."
       private_key_id = "..."
       private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
       client_email = "...@....iam.gserviceaccount.com"
       client_id = "..."
       token_uri = "https://oauth2.googleapis.com/token"

   or, outside Streamlit (e.g. the alert sender), from environment
   variables GOOGLE_SERVICE_ACCOUNT_JSON (the key file contents) and
   USERS_SHEET_ID.

2. Local SQLite (users.db) — fallback for local development or when no
   credentials are configured. NOTE: on Streamlit Cloud this file is
   ephemeral and resets on every redeploy.

Passwords are hashed with PBKDF2-HMAC-SHA256 (390k iterations) and a
per-user random salt. Only the hash is stored, in either backend.
"""

import hashlib
import hmac
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

USERS_DB = Path(__file__).parent / "users.db"

_PBKDF2_ITERATIONS = 390_000
_SESSION_DAYS = 30
_RE_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

_USER_HEADERS = ["email", "pw_hash", "salt", "first_name", "last_name",
                 "phone", "company", "subscriptions", "created_at", "updated_at"]
_SESSION_HEADERS = ["token", "email", "created_at", "expires_at"]


# ---------------------------------------------------------------------------
# Google Sheets backend
# ---------------------------------------------------------------------------

def _sheets_config():
    """Return (service_account_info, sheet_id) or None if not configured."""
    try:
        import streamlit as st
        if "gcp_service_account" in st.secrets and "users_sheet_id" in st.secrets:
            return dict(st.secrets["gcp_service_account"]), str(st.secrets["users_sheet_id"])
    except Exception:
        pass
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    sheet_id = os.environ.get("USERS_SHEET_ID")
    if sa_json and sheet_id:
        try:
            return json.loads(sa_json), sheet_id
        except Exception:
            return None
    return None


_spreadsheet = None
_sheets_failed = False


def _sheet():
    """Return the gspread Spreadsheet, or None if unavailable."""
    global _spreadsheet, _sheets_failed
    if _spreadsheet is not None:
        return _spreadsheet
    if _sheets_failed:
        return None
    cfg = _sheets_config()
    if not cfg:
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_info(
            cfg[0], scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        _spreadsheet = gspread.authorize(creds).open_by_key(cfg[1])
        return _spreadsheet
    except Exception:
        # Bad credentials / sheet not shared / network — fall back to SQLite
        _sheets_failed = True
        return None


def _ws(name: str, headers: list[str]):
    """Get or create a worksheet with the given header row."""
    sh = _sheet()
    if sh is None:
        return None
    try:
        ws = sh.worksheet(name)
    except Exception:
        ws = sh.add_worksheet(title=name, rows=1000, cols=len(headers))
        ws.append_row(headers, value_input_option="RAW")
        return ws
    if not ws.row_values(1):
        ws.append_row(headers, value_input_option="RAW")
    return ws


def backend_name() -> str:
    """'sheets' when Google Sheets is active, else 'local'."""
    return "sheets" if _sheet() is not None else "local"


def _ws_records(ws) -> list[dict]:
    """All rows as dicts with string values."""
    recs = ws.get_all_records()
    return [{k: ("" if v is None else str(v)) for k, v in r.items()} for r in recs]


def _ws_find_row(ws, col: str, value: str) -> tuple[int, dict] | tuple[None, None]:
    """Return (1-based sheet row number, record) for the first match."""
    for i, rec in enumerate(_ws_records(ws)):
        if rec.get(col, "") == value:
            return i + 2, rec  # +2: header row + 1-based indexing
    return None, None


# ---------------------------------------------------------------------------
# SQLite backend
# ---------------------------------------------------------------------------

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(USERS_DB)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            email         TEXT NOT NULL UNIQUE,
            pw_hash       TEXT NOT NULL,
            salt          TEXT NOT NULL,
            subscriptions TEXT,
            created_at    TEXT,
            updated_at    TEXT
        )
        """
    )
    for col in ("first_name", "last_name", "phone", "company"):
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            token      TEXT PRIMARY KEY,
            email      TEXT NOT NULL,
            created_at TEXT,
            expires_at TEXT
        )
        """
    )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _hash_password(password: str, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS
    ).hex()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_account(
    email: str,
    password: str,
    subscriptions: list[str],
    first_name: str = "",
    last_name: str = "",
    phone: str = "",
    company: str = "",
) -> tuple[bool, str]:
    """Create a new account. Returns (ok, message)."""
    email = (email or "").strip().lower()
    if not (first_name or "").strip():
        return False, "Please enter your first name."
    if not (last_name or "").strip():
        return False, "Please enter your last name."
    if not _RE_EMAIL.match(email):
        return False, "Please enter a valid email address."
    if len(password or "") < 8:
        return False, "Password must be at least 8 characters."

    salt = os.urandom(16)
    pw_hash = _hash_password(password, salt)
    now = _now()
    subs_json = json.dumps(subscriptions or [])

    ws = _ws("users", _USER_HEADERS)
    if ws is not None:
        row_no, _ = _ws_find_row(ws, "email", email)
        if row_no:
            return False, "An account with this email already exists."
        ws.append_row(
            [email, pw_hash, salt.hex(), first_name.strip(), last_name.strip(),
             (phone or "").strip(), (company or "").strip(), subs_json, now, now],
            value_input_option="RAW",
        )
        return True, "Account created."

    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO users (email, pw_hash, salt, subscriptions, created_at, updated_at,"
            " first_name, last_name, phone, company) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (email, pw_hash, salt.hex(), subs_json, now, now,
             first_name.strip(), last_name.strip(),
             (phone or "").strip(), (company or "").strip()),
        )
        conn.commit()
        return True, "Account created."
    except sqlite3.IntegrityError:
        return False, "An account with this email already exists."
    finally:
        conn.close()


def authenticate(email: str, password: str) -> bool:
    email = (email or "").strip().lower()

    ws = _ws("users", _USER_HEADERS)
    if ws is not None:
        _, rec = _ws_find_row(ws, "email", email)
        if not rec:
            return False
        try:
            candidate = _hash_password(password or "", bytes.fromhex(rec["salt"]))
        except Exception:
            return False
        return hmac.compare_digest(candidate, rec["pw_hash"])

    conn = _conn()
    try:
        row = conn.execute("SELECT pw_hash, salt FROM users WHERE email=?", (email,)).fetchone()
    finally:
        conn.close()
    if not row:
        return False
    candidate = _hash_password(password or "", bytes.fromhex(row["salt"]))
    return hmac.compare_digest(candidate, row["pw_hash"])


def _parse_subs(raw: str) -> list[str]:
    try:
        out = json.loads(raw or "[]")
        return out if isinstance(out, list) else []
    except Exception:
        return []


def get_user(email: str) -> dict | None:
    """Return the user's profile (no credential material) or None."""
    email = (email or "").strip().lower()

    ws = _ws("users", _USER_HEADERS)
    if ws is not None:
        _, rec = _ws_find_row(ws, "email", email)
        if not rec:
            return None
        return {
            "email": rec["email"],
            "first_name": rec.get("first_name", ""),
            "last_name": rec.get("last_name", ""),
            "phone": rec.get("phone", ""),
            "company": rec.get("company", ""),
            "subscriptions": _parse_subs(rec.get("subscriptions", "")),
            "created_at": rec.get("created_at", ""),
        }

    conn = _conn()
    try:
        row = conn.execute(
            "SELECT email, first_name, last_name, phone, company, subscriptions, created_at"
            " FROM users WHERE email=?", (email,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "email": row["email"],
        "first_name": row["first_name"] or "",
        "last_name": row["last_name"] or "",
        "phone": row["phone"] or "",
        "company": row["company"] or "",
        "subscriptions": _parse_subs(row["subscriptions"]),
        "created_at": row["created_at"],
    }


def update_profile(email: str, first_name: str, last_name: str, phone: str, company: str) -> None:
    email = (email or "").strip().lower()

    ws = _ws("users", _USER_HEADERS)
    if ws is not None:
        row_no, _ = _ws_find_row(ws, "email", email)
        if row_no:
            ws.update(
                [[
                    (first_name or "").strip(), (last_name or "").strip(),
                    (phone or "").strip(), (company or "").strip(),
                ]],
                f"D{row_no}:G{row_no}",
                value_input_option="RAW",
            )
            ws.update([[_now()]], f"J{row_no}", value_input_option="RAW")
        return

    conn = _conn()
    try:
        conn.execute(
            "UPDATE users SET first_name=?, last_name=?, phone=?, company=?, updated_at=? WHERE email=?",
            ((first_name or "").strip(), (last_name or "").strip(),
             (phone or "").strip(), (company or "").strip(), _now(), email),
        )
        conn.commit()
    finally:
        conn.close()


def get_subscriptions(email: str) -> list[str]:
    user = get_user(email)
    return user["subscriptions"] if user else []


def update_subscriptions(email: str, subscriptions: list[str]) -> None:
    email = (email or "").strip().lower()
    subs_json = json.dumps(subscriptions or [])

    ws = _ws("users", _USER_HEADERS)
    if ws is not None:
        row_no, _ = _ws_find_row(ws, "email", email)
        if row_no:
            ws.update([[subs_json]], f"H{row_no}", value_input_option="RAW")
            ws.update([[_now()]], f"J{row_no}", value_input_option="RAW")
        return

    conn = _conn()
    try:
        conn.execute(
            "UPDATE users SET subscriptions=?, updated_at=? WHERE email=?",
            (subs_json, _now(), email),
        )
        conn.commit()
    finally:
        conn.close()


def all_subscribers() -> list[dict]:
    """Return [{email, subscriptions}] for the daily-alert sender."""
    ws = _ws("users", _USER_HEADERS)
    if ws is not None:
        out = []
        for rec in _ws_records(ws):
            subs = _parse_subs(rec.get("subscriptions", ""))
            if subs:
                out.append({"email": rec["email"], "subscriptions": subs})
        return out

    conn = _conn()
    try:
        rows = conn.execute("SELECT email, subscriptions FROM users").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        subs = _parse_subs(r["subscriptions"])
        if subs:
            out.append({"email": r["email"], "subscriptions": subs})
    return out


# ---------------------------------------------------------------------------
# Login sessions
# ---------------------------------------------------------------------------

def create_session(email: str) -> str:
    """Create a login session and return its token."""
    import secrets
    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires = (now + timedelta(days=_SESSION_DAYS)).isoformat()

    ws = _ws("sessions", _SESSION_HEADERS)
    if ws is not None:
        ws.append_row([token, (email or "").strip().lower(), now.isoformat(), expires],
                      value_input_option="RAW")
        return token

    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO sessions (token, email, created_at, expires_at) VALUES (?,?,?,?)",
            (token, (email or "").strip().lower(), now.isoformat(), expires),
        )
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (now.isoformat(),))
        conn.commit()
    finally:
        conn.close()
    return token


def get_session_email(token: str) -> str | None:
    """Return the email for a valid, unexpired session token, else None."""
    if not token:
        return None

    ws = _ws("sessions", _SESSION_HEADERS)
    if ws is not None:
        _, rec = _ws_find_row(ws, "token", token)
        if not rec:
            return None
        try:
            if datetime.fromisoformat(rec["expires_at"]) < datetime.now(timezone.utc):
                return None
        except Exception:
            return None
        return rec["email"]

    conn = _conn()
    try:
        row = conn.execute(
            "SELECT email, expires_at FROM sessions WHERE token=?", (token,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    try:
        if datetime.fromisoformat(row["expires_at"]) < datetime.now(timezone.utc):
            return None
    except Exception:
        return None
    return row["email"]


def delete_session(token: str) -> None:
    if not token:
        return

    ws = _ws("sessions", _SESSION_HEADERS)
    if ws is not None:
        row_no, _ = _ws_find_row(ws, "token", token)
        if row_no:
            ws.delete_rows(row_no)
        return

    conn = _conn()
    try:
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))
        conn.commit()
    finally:
        conn.close()

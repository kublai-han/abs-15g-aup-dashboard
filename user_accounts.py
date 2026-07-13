"""
user_accounts.py

Account storage and authentication for the dashboard's daily-alert
subscriptions.

Users live in a separate SQLite database (users.db) that is intentionally
NOT committed to git — the dashboard repo is public and credential hashes
must never be pushed there.

Passwords are hashed with PBKDF2-HMAC-SHA256 (390k iterations) and a
per-user random salt.
"""

import hashlib
import hmac
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

USERS_DB = Path(__file__).parent / "users.db"

_PBKDF2_ITERATIONS = 390_000
_RE_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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
            subscriptions TEXT,          -- JSON list of issuer_type strings
            created_at    TEXT,
            updated_at    TEXT
        )
        """
    )
    # Idempotent migrations for the standard profile fields
    for col in ("first_name", "last_name", "phone", "company"):
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
    # Login sessions — Streamlit starts a fresh server session on every full
    # page load (all nav links reload the page), so logins are persisted via
    # a token carried in the URL and validated against this table.
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


def _hash_password(password: str, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, _PBKDF2_ITERATIONS
    ).hex()


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
    now = datetime.now(timezone.utc).isoformat()
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO users (email, pw_hash, salt, subscriptions, created_at, updated_at,"
            " first_name, last_name, phone, company)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (email, _hash_password(password, salt), salt.hex(),
             json.dumps(subscriptions or []), now, now,
             first_name.strip(), last_name.strip(),
             (phone or "").strip(), (company or "").strip()),
        )
        conn.commit()
        return True, "Account created."
    except sqlite3.IntegrityError:
        return False, "An account with this email already exists."
    finally:
        conn.close()


def get_user(email: str) -> dict | None:
    """Return the user's profile (no credential material) or None."""
    email = (email or "").strip().lower()
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
    try:
        subs = json.loads(row["subscriptions"] or "[]")
    except Exception:
        subs = []
    return {
        "email": row["email"],
        "first_name": row["first_name"] or "",
        "last_name": row["last_name"] or "",
        "phone": row["phone"] or "",
        "company": row["company"] or "",
        "subscriptions": subs,
        "created_at": row["created_at"],
    }


def update_profile(email: str, first_name: str, last_name: str, phone: str, company: str) -> None:
    email = (email or "").strip().lower()
    conn = _conn()
    try:
        conn.execute(
            "UPDATE users SET first_name=?, last_name=?, phone=?, company=?, updated_at=?"
            " WHERE email=?",
            ((first_name or "").strip(), (last_name or "").strip(),
             (phone or "").strip(), (company or "").strip(),
             datetime.now(timezone.utc).isoformat(), email),
        )
        conn.commit()
    finally:
        conn.close()


def authenticate(email: str, password: str) -> bool:
    email = (email or "").strip().lower()
    conn = _conn()
    try:
        row = conn.execute("SELECT pw_hash, salt FROM users WHERE email=?", (email,)).fetchone()
    finally:
        conn.close()
    if not row:
        return False
    candidate = _hash_password(password or "", bytes.fromhex(row["salt"]))
    return hmac.compare_digest(candidate, row["pw_hash"])


def get_subscriptions(email: str) -> list[str]:
    email = (email or "").strip().lower()
    conn = _conn()
    try:
        row = conn.execute("SELECT subscriptions FROM users WHERE email=?", (email,)).fetchone()
    finally:
        conn.close()
    if not row or not row["subscriptions"]:
        return []
    try:
        return json.loads(row["subscriptions"])
    except Exception:
        return []


def update_subscriptions(email: str, subscriptions: list[str]) -> None:
    email = (email or "").strip().lower()
    conn = _conn()
    try:
        conn.execute(
            "UPDATE users SET subscriptions=?, updated_at=? WHERE email=?",
            (json.dumps(subscriptions or []),
             datetime.now(timezone.utc).isoformat(), email),
        )
        conn.commit()
    finally:
        conn.close()


_SESSION_DAYS = 30


def create_session(email: str) -> str:
    """Create a login session and return its token."""
    import secrets
    from datetime import timedelta
    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO sessions (token, email, created_at, expires_at) VALUES (?,?,?,?)",
            (token, (email or "").strip().lower(), now.isoformat(),
             (now + timedelta(days=_SESSION_DAYS)).isoformat()),
        )
        # opportunistic cleanup of expired sessions
        conn.execute("DELETE FROM sessions WHERE expires_at < ?", (now.isoformat(),))
        conn.commit()
    finally:
        conn.close()
    return token


def get_session_email(token: str) -> str | None:
    """Return the email for a valid, unexpired session token, else None."""
    if not token:
        return None
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
    conn = _conn()
    try:
        conn.execute("DELETE FROM sessions WHERE token=?", (token,))
        conn.commit()
    finally:
        conn.close()


def all_subscribers() -> list[dict]:
    """Return [{email, subscriptions}] for the daily-alert sender."""
    conn = _conn()
    try:
        rows = conn.execute("SELECT email, subscriptions FROM users").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        try:
            subs = json.loads(r["subscriptions"] or "[]")
        except Exception:
            subs = []
        if subs:
            out.append({"email": r["email"], "subscriptions": subs})
    return out

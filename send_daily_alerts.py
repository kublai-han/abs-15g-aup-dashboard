"""
send_daily_alerts.py

Two jobs, run once a day (see .github/workflows/daily_alerts.yml):

1. Subscriber digests — email each registered user a summary of the AUP
   results ingested in the last 24 hours for the asset classes they chose.
2. Owner digest — email the site owner the same activity summary plus any
   NEW SIGNUPS in the last 24 hours.

Accounts come from user_accounts (Google Sheets when configured, else the
local users.db); filings come from aup_dashboard.db.

Configuration (environment variables; see also user_accounts.smtp_config):
    SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS   mail login
    SMTP_FROM                                       optional From override
    ADMIN_EMAIL                                     owner alerts; default SMTP_USER
    GOOGLE_SERVICE_ACCOUNT_JSON / USERS_SHEET_ID    Google Sheets accounts

Exits 0 even when nothing is sent, so a quiet day is not a failed job.
"""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import user_accounts

DB_PATH = Path(__file__).parent / "aup_dashboard.db"
SITE_URL = "https://bonddataquality.streamlit.app"

ASSET_LABELS = {
    "auto": "Auto", "credit_card": "Credit Card", "consumer_loan": "Consumer Loans",
    "student_loan": "Student Loans", "small_business_loan": "Small Business Loans",
    "datacenter": "Datacenter", "fiber": "Fiber",
    "nqm": "Non-Qualified Mortgage", "second_lien": "Second Lien",
    "rpl": "Re-Performing Loans", "prime_jumbo": "Prime Jumbo",
    "inv_property": "Investment Properties", "npl": "Non-Performing Loans",
    "conduit": "Conduit CMBS", "cre_clo": "CRE-CLO", "large_loan": "Large-Loan CMBS",
}


def label(asset_type: str) -> str:
    return ASSET_LABELS.get(asset_type, asset_type or "Other")


def new_filings_since(cutoff_iso: str) -> list[sqlite3.Row]:
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            """
            SELECT f.issuer_key, f.deal_name, f.filed_date, f.asset_type,
                   f.aup_provider, p.sample_size, p.exception_count
            FROM filings f LEFT JOIN procedures p ON p.filing_id = f.id
            WHERE f.fetched_at >= ?
            ORDER BY f.asset_type, f.filed_date DESC
            """,
            (cutoff_iso,),
        ).fetchall()
    finally:
        conn.close()


def _format_filings(rows) -> list[str]:
    lines, cur = [], None
    for r in rows:
        if r["asset_type"] != cur:
            cur = r["asset_type"]
            lines.append(f"--- {label(cur)} ---")
        exc = r["exception_count"]
        exc_str = f"{exc} finding{'s' if exc != 1 else ''}" if exc is not None else "findings n/a"
        lines.append(
            f"  {r['filed_date']}  {r['deal_name'] or r['issuer_key']}"
            f"  (auditor: {r['aup_provider'] or 'n/a'}, sample: {r['sample_size'] or 'n/a'}, {exc_str})"
        )
    return lines


def build_subscriber_digest(rows, subscribed_types: list[str]) -> str | None:
    matches = [r for r in rows if r["asset_type"] in subscribed_types]
    if not matches:
        return None
    return "\n".join(
        ["New ABS-15G AUP results in the last 24 hours:", ""]
        + _format_filings(matches)
        + ["", f"View details: {SITE_URL}"]
    )


def build_owner_digest(rows, signups: list[dict]) -> str | None:
    if not rows and not signups:
        return None
    lines = ["Bond Data Quality — daily summary", ""]
    if signups:
        lines.append(f"NEW SIGNUPS ({len(signups)}):")
        for u in signups:
            name = f"{u['first_name']} {u['last_name']}".strip() or "(no name)"
            subs = ", ".join(label(t) for t in u["subscriptions"]) or "none selected"
            lines.append(f"  {name} <{u['email']}>")
            lines.append(f"      company: {u['company'] or '-'}   phone: {u['phone'] or '-'}")
            lines.append(f"      alerts:  {subs}")
        lines.append("")
    else:
        lines += ["NEW SIGNUPS: none", ""]
    if rows:
        lines.append(f"NEW AUP RESULTS ({len(rows)}):")
        lines += _format_filings(rows)
    else:
        lines.append("NEW AUP RESULTS: none")
    lines += ["", SITE_URL]
    return "\n".join(lines)


def main() -> int:
    if not user_accounts.mail_configured():
        print("SMTP not configured (SMTP_HOST/SMTP_USER/SMTP_PASS) — nothing sent.")
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    rows = new_filings_since(cutoff)
    try:
        signups = user_accounts.users_created_since(cutoff)
    except Exception as exc:
        print(f"Could not read new signups: {exc}")
        signups = []
    subs = user_accounts.all_subscribers()

    print(f"accounts backend : {user_accounts.backend_name()}")
    print(f"new filings (24h): {len(rows)}")
    print(f"new signups (24h): {len(signups)}")
    print(f"subscribers      : {len(subs)}")

    sent = 0
    for s in subs:
        body = build_subscriber_digest(rows, s["subscriptions"])
        if body and user_accounts.send_email(
            s["email"], "Bond Data Quality — new AUP results", body
        ):
            sent += 1
            print(f"  digest -> {s['email']}")

    owner_body = build_owner_digest(rows, signups)
    if owner_body:
        subject = (
            f"Bond Data Quality — {len(signups)} new signup{'s' if len(signups) != 1 else ''}"
            if signups else "Bond Data Quality — daily summary"
        )
        if user_accounts.send_email(user_accounts.admin_email(), subject, owner_body):
            print(f"  owner summary -> {user_accounts.admin_email()}")
        else:
            print("  owner summary FAILED to send")

    print(f"Done: {sent} subscriber email(s) sent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

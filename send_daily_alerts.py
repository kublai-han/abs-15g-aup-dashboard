"""
send_daily_alerts.py

Email subscribers a daily digest of new AUP results for the asset classes
they selected at sign-up.

Reads subscribers from users.db (see user_accounts.py) and new filings from
aup_dashboard.db (anything fetched in the last 24 hours).

SMTP configuration via environment variables:
    SMTP_HOST   e.g. smtp.gmail.com
    SMTP_PORT   e.g. 587
    SMTP_USER   login / from address
    SMTP_PASS   password or app password
    SMTP_FROM   optional from address (defaults to SMTP_USER)

Run wherever users.db lives (the machine hosting the dashboard).
"""

import os
import smtplib
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

import user_accounts

DB_PATH = Path(__file__).parent / "aup_dashboard.db"

ASSET_LABELS = {
    "auto": "Auto", "credit_card": "Credit Card", "consumer_loan": "Consumer Loans",
    "student_loan": "Student Loans", "small_business_loan": "Small Business Loans",
    "nqm": "Non-Qualified Mortgage", "second_lien": "Second Lien",
    "rpl": "Re-Performing Loans", "prime_jumbo": "Prime Jumbo",
    "inv_property": "Investment Properties", "npl": "Non-Performing Loans",
    "conduit": "Conduit CMBS", "cre_clo": "CRE-CLO",
}


def new_filings_since(hours: int = 24) -> list[sqlite3.Row]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT f.issuer_key, f.deal_name, f.filed_date, f.asset_type,
               f.aup_provider, p.sample_size, p.exception_count
        FROM filings f LEFT JOIN procedures p ON p.filing_id = f.id
        WHERE f.fetched_at >= ?
        ORDER BY f.asset_type, f.filed_date DESC
        """,
        (cutoff,),
    ).fetchall()
    conn.close()
    return rows


def build_digest(rows: list[sqlite3.Row], subscribed_types: list[str]) -> str | None:
    matches = [r for r in rows if r["asset_type"] in subscribed_types]
    if not matches:
        return None
    lines = ["New ABS-15G AUP results in the last 24 hours:", ""]
    cur_type = None
    for r in matches:
        if r["asset_type"] != cur_type:
            cur_type = r["asset_type"]
            lines.append(f"--- {ASSET_LABELS.get(cur_type, cur_type)} ---")
        exc = r["exception_count"]
        exc_str = f"{exc} exception{'s' if exc != 1 else ''}" if exc is not None else "n/a"
        lines.append(
            f"  {r['filed_date']}  {r['deal_name'] or r['issuer_key']}"
            f"  (auditor: {r['aup_provider'] or 'n/a'}, sample: {r['sample_size'] or 'n/a'}, {exc_str})"
        )
    lines += ["", "View details: https://bonddataquality.streamlit.app"]
    return "\n".join(lines)


def main() -> int:
    host = os.environ.get("SMTP_HOST")
    user = os.environ.get("SMTP_USER")
    pw = os.environ.get("SMTP_PASS")
    if not (host and user and pw):
        print("SMTP_HOST / SMTP_USER / SMTP_PASS not configured - nothing sent.")
        return 0
    port = int(os.environ.get("SMTP_PORT", "587"))
    sender = os.environ.get("SMTP_FROM", user)

    rows = new_filings_since(24)
    subs = user_accounts.all_subscribers()
    print(f"New filings (24h): {len(rows)}; subscribers with alerts: {len(subs)}")
    if not rows or not subs:
        return 0

    sent = 0
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(user, pw)
        for s in subs:
            body = build_digest(rows, s["subscriptions"])
            if not body:
                continue
            msg = MIMEText(body)
            msg["Subject"] = "Bond Data Quality - new AUP results"
            msg["From"] = sender
            msg["To"] = s["email"]
            smtp.send_message(msg)
            sent += 1
            print(f"  sent -> {s['email']}")
    print(f"Done: {sent} email(s) sent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

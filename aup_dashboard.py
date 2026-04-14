"""
aup_dashboard.py

Streamlit dashboard for the ABS-15G AUP Aggregator project.
Finsight-inspired dark theme layout.

Tabs
----
1. Market Overview     -- summary cards, issuer profile grid, run-updater button
2. Issuer Profiles     -- filing history + AUP results for a selected issuer
3. AUP Results         -- cross-issuer exception rate comparisons & trend charts
4. DQA Report          -- data quality check history and dqa_report.md viewer
5. Update Log          -- last-run timestamp, new filings, errors
"""

from __future__ import annotations

import importlib
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import sys

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

_DIR = Path(__file__).resolve().parent
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))

import aup_database as db
from issuers import ISSUERS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = _DIR / "aup_dashboard.db"
DQA_REPORT_PATH = _DIR / "dqa_report.md"

EDGAR_FILING_BASE = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcompany&CIK={cik}&type=ABS-15G&dateb=&owner=include&count=40"
)
EDGAR_ACCESSION_BASE = (
    "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_plain}/{acc_dashed}-index.htm"
)

# Sector badge config: issuer type -> (label, hex color)
SECTOR_BADGES: dict[str, tuple[str, str]] = {
    "consumer_loan":       ("CONS", "#7b5ea7"),
    "auto":                ("AUTO", "#2563eb"),
    "student_loan":        ("STUD", "#059669"),
    "mortgage":            ("RMBS", "#d97706"),
    "small_business_loan": ("SMB",  "#db2777"),
}

# Plotly dark palette for multi-issuer charts
ISSUER_PALETTE = [
    "#7b5ea7", "#4f9cf9", "#00c896", "#ff6b6b", "#ffd166",
    "#06d6a0", "#ef476f", "#118ab2", "#a8dadc", "#457b9d",
    "#e63946",
]

# ---------------------------------------------------------------------------
# Page configuration — must be first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ABS-15G AUP Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Global CSS — Finsight-inspired dark theme
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
        /* ── Base ── */
        html, body, [data-testid="stApp"] {
            background-color: #0d0d1a !important;
            color: #e2e8f0 !important;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                         "Helvetica Neue", Arial, sans-serif;
        }

        /* ── Hide default Streamlit chrome ── */
        #MainMenu, footer, header { visibility: hidden; }
        [data-testid="stSidebar"] { display: none !important; }
        .block-container {
            padding-top: 0 !important;
            padding-left: 5rem !important;
            padding-right: 5rem !important;
            max-width: 1380px !important;
            margin-left: auto !important;
            margin-right: auto !important;
        }
        @media (max-width: 900px) {
            .block-container {
                padding-left: 1.25rem !important;
                padding-right: 1.25rem !important;
            }
        }

        /* ── Top header bar ── */
        .finsight-header {
            background: #1e1e3f;
            border-bottom: 1px solid #2d2d5e;
            padding: 0.65rem 2rem;
            display: flex;
            align-items: center;
            gap: 1.25rem;
            position: sticky;
            top: 0;
            z-index: 999;
        }
        .finsight-logo {
            font-size: 1.15rem;
            font-weight: 700;
            color: #ffffff;
            letter-spacing: -0.02em;
            white-space: nowrap;
        }
        .finsight-logo-badge {
            background: #7b5ea7;
            color: #fff;
            font-size: 0.68rem;
            font-weight: 700;
            padding: 2px 7px;
            border-radius: 3px;
            margin-right: 6px;
            letter-spacing: 0.04em;
            vertical-align: middle;
        }
        .finsight-search {
            flex: 1;
            max-width: 340px;
            background: #141428;
            border: 1px solid #2d2d5e;
            border-radius: 5px;
            padding: 0.38rem 0.75rem;
            color: #94a3b8;
            font-size: 0.82rem;
        }
        .header-spacer { flex: 1; }
        .finsight-updated-badge {
            background: #141428;
            border: 1px solid #2d2d5e;
            color: #94a3b8;
            font-size: 0.75rem;
            padding: 4px 10px;
            border-radius: 4px;
            white-space: nowrap;
        }

        /* ── Tab bar ── */
        .finsight-tabbar {
            background: #141428;
            border-bottom: 1px solid #2d2d5e;
            padding: 0 2rem;
            display: flex;
            gap: 0;
        }
        .finsight-tab {
            padding: 0.65rem 1.25rem;
            font-size: 0.82rem;
            font-weight: 500;
            color: #94a3b8;
            border-bottom: 2px solid transparent;
            cursor: pointer;
            white-space: nowrap;
            text-decoration: none;
            transition: color 0.15s;
        }
        .finsight-tab:hover { color: #e2e8f0; }
        .finsight-tab.active {
            color: #a78bfa;
            border-bottom-color: #7b5ea7;
            font-weight: 600;
        }

        /* ── Main content wrapper ── */
        .finsight-content {
            padding: 1.5rem 0;
            background: #0d0d1a;
        }

        /* ── Search bar with magnifying glass ── */
        .search-wrapper {
            position: relative;
            margin-bottom: 1.25rem;
        }
        .search-wrapper svg {
            position: absolute;
            left: 0.75rem;
            top: 50%;
            transform: translateY(-50%);
            color: #64748b;
            pointer-events: none;
            width: 15px;
            height: 15px;
        }
        .search-wrapper input[type="text"] {
            width: 100%;
            box-sizing: border-box;
            background: #141428;
            border: 1px solid #2d2d5e;
            border-radius: 6px;
            padding: 0.5rem 0.9rem 0.5rem 2.2rem;
            color: #e2e8f0;
            font-size: 0.88rem;
            outline: none;
            transition: border-color 0.15s;
        }
        .search-wrapper input[type="text"]::placeholder { color: #64748b; }
        .search-wrapper input[type="text"]:focus { border-color: #7b5ea7; }

        /* ── Section heading ── */
        .finsight-section-title {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.1em;
            color: #64748b;
            text-transform: uppercase;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid #1e1e3f;
        }

        /* ── Metric cards ── */
        .metric-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
            margin-bottom: 2rem;
        }
        .metric-card {
            background: #1e1e3f;
            border: 1px solid #2d2d5e;
            border-radius: 8px;
            padding: 1.1rem 1.4rem;
        }
        .metric-label {
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: #64748b;
            margin-bottom: 0.35rem;
        }
        .metric-value {
            font-size: 1.9rem;
            font-weight: 700;
            color: #f1f5f9;
            line-height: 1.1;
        }
        .metric-sub {
            font-size: 0.72rem;
            color: #94a3b8;
            margin-top: 0.25rem;
        }

        /* ── Issuer profile cards (grid) ── */
        .issuer-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.9rem;
            margin-bottom: 2rem;
        }
        @media (max-width: 1100px) {
            .issuer-grid { grid-template-columns: repeat(2, 1fr); }
        }
        .issuer-card {
            background: #1e1e3f;
            border: 1px solid #2d2d5e;
            border-radius: 8px;
            padding: 1rem 1.1rem 0.9rem;
            transition: border-color 0.15s, transform 0.12s;
            cursor: default;
        }
        .issuer-card:hover {
            border-color: #7b5ea7;
            transform: translateY(-1px);
        }
        .sector-badge {
            display: inline-block;
            font-size: 0.66rem;
            font-weight: 700;
            letter-spacing: 0.06em;
            padding: 2px 7px;
            border-radius: 3px;
            margin-bottom: 0.5rem;
        }
        .issuer-name {
            font-size: 0.9rem;
            font-weight: 700;
            color: #f1f5f9;
            margin-bottom: 0.35rem;
            line-height: 1.25;
        }
        .issuer-meta {
            font-size: 0.73rem;
            color: #94a3b8;
            line-height: 1.6;
        }
        .issuer-meta span.hi { color: #a78bfa; }

        /* ── Status badges (pass/fail/warn/skip) ── */
        .badge-pass { color: #00c896; background: #00c89618; border: 1px solid #00c89640; border-radius: 4px; padding: 2px 8px; font-size: 0.74rem; font-weight: 600; }
        .badge-fail { color: #ff4757; background: #ff475718; border: 1px solid #ff475740; border-radius: 4px; padding: 2px 8px; font-size: 0.74rem; font-weight: 600; }
        .badge-warn { color: #ffd166; background: #ffd16618; border: 1px solid #ffd16640; border-radius: 4px; padding: 2px 8px; font-size: 0.74rem; font-weight: 600; }
        .badge-skip { color: #94a3b8; background: #94a3b818; border: 1px solid #94a3b840; border-radius: 4px; padding: 2px 8px; font-size: 0.74rem; font-weight: 600; }

        /* ── Info / empty state box ── */
        .info-box {
            background: #1e1e3f;
            border-left: 3px solid #7b5ea7;
            border-radius: 4px;
            padding: 0.85rem 1.1rem;
            font-size: 0.85rem;
            color: #94a3b8;
            margin: 1rem 0;
        }

        /* ── Tables ── */
        .styled-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.8rem;
            margin-top: 0.5rem;
        }
        .styled-table thead th {
            background: #141428 !important;
            color: #64748b !important;
            font-size: 0.68rem !important;
            font-weight: 700 !important;
            letter-spacing: 0.08em !important;
            text-transform: uppercase !important;
            padding: 0.6rem 0.85rem !important;
            border-bottom: 1px solid #2d2d5e !important;
            text-align: left !important;
        }
        .styled-table tbody tr {
            border-bottom: 1px solid #1e1e3f;
        }
        .styled-table tbody tr:hover { background: #1e1e3f; }
        .styled-table tbody td {
            padding: 0.55rem 0.85rem;
            color: #cbd5e1;
            vertical-align: middle;
        }
        .styled-table a {
            color: #a78bfa;
            text-decoration: none;
        }
        .styled-table a:hover { text-decoration: underline; }

        /* ── Streamlit native widget dark overrides ── */
        div[data-testid="metric-container"] {
            background: #1e1e3f !important;
            border: 1px solid #2d2d5e !important;
            border-radius: 8px !important;
            padding: 1rem 1.25rem !important;
        }
        div[data-testid="metric-container"] label {
            color: #64748b !important;
            font-size: 0.72rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.07em !important;
        }
        div[data-testid="metric-container"] [data-testid="stMetricValue"] {
            color: #f1f5f9 !important;
            font-size: 1.9rem !important;
        }

        /* Streamlit selectbox / multiselect / number_input dark */
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div {
            background-color: #141428 !important;
            border-color: #2d2d5e !important;
            color: #e2e8f0 !important;
        }
        div[data-baseweb="select"] span,
        div[data-baseweb="menu"] li {
            color: #e2e8f0 !important;
        }
        div[data-baseweb="menu"] {
            background-color: #1e1e3f !important;
            border-color: #2d2d5e !important;
        }
        div[data-baseweb="menu"] li:hover { background-color: #2d2d5e !important; }
        .stTextInput input, .stNumberInput input {
            background-color: #141428 !important;
            color: #e2e8f0 !important;
            border-color: #2d2d5e !important;
        }

        /* Streamlit tabs */
        div[data-testid="stTabs"] [data-testid="stTabBar"] {
            background: #141428 !important;
            border-bottom: 1px solid #2d2d5e !important;
            gap: 0 !important;
        }
        div[data-testid="stTabs"] button[data-testid="stTab"] {
            background: transparent !important;
            color: #94a3b8 !important;
            font-size: 0.82rem !important;
            font-weight: 500 !important;
            border-radius: 0 !important;
            padding: 0.65rem 1.25rem !important;
            border-bottom: 2px solid transparent !important;
        }
        div[data-testid="stTabs"] button[data-testid="stTab"][aria-selected="true"] {
            color: #a78bfa !important;
            border-bottom-color: #7b5ea7 !important;
            font-weight: 600 !important;
        }
        div[data-testid="stTabs"] button[data-testid="stTab"]:hover {
            color: #e2e8f0 !important;
        }
        div[data-testid="stTabPanel"] {
            background: #0d0d1a !important;
            padding: 1.5rem 0 !important;
        }

        /* Streamlit dataframe dark */
        .dataframe, [data-testid="stDataFrameContainer"] {
            background: #1e1e3f !important;
        }
        .dataframe thead th {
            background-color: #141428 !important;
            color: #64748b !important;
        }
        .dataframe tbody td { color: #cbd5e1 !important; }

        /* Multiselect tags */
        span[data-baseweb="tag"] {
            background-color: #7b5ea7 !important;
            color: #fff !important;
        }

        /* Button */
        .stButton > button {
            background: #7b5ea7 !important;
            color: #fff !important;
            border: none !important;
            border-radius: 5px !important;
            font-weight: 600 !important;
            font-size: 0.82rem !important;
            padding: 0.45rem 1.25rem !important;
            transition: background 0.15s !important;
        }
        .stButton > button:hover { background: #6d4f96 !important; }

        /* Expander */
        details { background: #1e1e3f !important; border: 1px solid #2d2d5e !important; border-radius: 6px !important; }
        summary { color: #94a3b8 !important; font-size: 0.82rem !important; }

        /* Spinner */
        .stSpinner > div { border-top-color: #7b5ea7 !important; }

        /* Alert boxes */
        div[data-testid="stAlert"] {
            background: #1e1e3f !important;
            border-color: #2d2d5e !important;
            color: #e2e8f0 !important;
        }

        /* Markdown */
        .stMarkdown p, .stMarkdown li { color: #cbd5e1 !important; }
        .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 { color: #f1f5f9 !important; }

        /* Plotly charts - make containers dark */
        [data-testid="stPlotlyChart"] { background: transparent !important; }

        /* DQA score chip */
        .score-chip {
            display: inline-block;
            font-size: 1.4rem;
            font-weight: 800;
            color: #ff4757;
            background: #ff475714;
            border: 1px solid #ff475740;
            border-radius: 8px;
            padding: 0.3rem 1rem;
            margin-right: 0.75rem;
        }
        .score-chip.mid { color: #ffd166; background: #ffd16614; border-color: #ffd16640; }
        .score-chip.high { color: #00c896; background: #00c89614; border-color: #00c89640; }

        /* CIK badge */
        .cik-badge {
            font-size: 0.72rem;
            background: #141428;
            border: 1px solid #2d2d5e;
            color: #94a3b8;
            padding: 2px 8px;
            border-radius: 4px;
            font-family: monospace;
        }

        /* Issuer detail header */
        .issuer-detail-header {
            background: #1e1e3f;
            border: 1px solid #2d2d5e;
            border-radius: 8px;
            padding: 1.25rem 1.5rem;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 1rem;
            flex-wrap: wrap;
        }
        .issuer-detail-name {
            font-size: 1.3rem;
            font-weight: 700;
            color: #f1f5f9;
        }
        .issuer-detail-links a {
            color: #a78bfa;
            font-size: 0.8rem;
            text-decoration: none;
        }
        .issuer-detail-links a:hover { text-decoration: underline; }

        /* Activity feed items */
        .activity-item {
            display: flex;
            align-items: flex-start;
            gap: 0.85rem;
            padding: 0.75rem 0;
            border-bottom: 1px solid #1e1e3f;
        }
        .activity-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #7b5ea7;
            margin-top: 5px;
            flex-shrink: 0;
        }
        .activity-text { font-size: 0.82rem; color: #cbd5e1; line-height: 1.4; }
        .activity-time { font-size: 0.72rem; color: #64748b; }

        /* Filter bar */
        .filter-bar {
            background: #1e1e3f;
            border: 1px solid #2d2d5e;
            border-radius: 8px;
            padding: 1rem 1.25rem;
            margin-bottom: 1.5rem;
        }
        .filter-label {
            font-size: 0.68rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #64748b;
            margin-bottom: 0.4rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _db_ready() -> bool:
    if not DB_PATH.exists():
        return False
    filings = db.get_filings(limit=1, db_path=DB_PATH)
    return len(filings) > 0


def _no_data_banner() -> None:
    st.markdown(
        '<div class="info-box">No data yet — run the updater to populate the database.'
        "<br>Use the <b>Run Update</b> button above, or execute "
        "<code>python aup_updater.py</code> from the command line.</div>",
        unsafe_allow_html=True,
    )


_FINDING_NOISE = {"findings", "exception", "exception description", "finding",
                  "findings set forth on appendix a", "findings set forth on appendix",
                  "findings set forth on appendix b", "findings based on the procedures performed",
                  "exception description number"}

def _fmt_finding(raw) -> str:
    """Return a clean one-line finding summary from a raw findings_json value."""
    import json, re as _re
    if not raw:
        return "—"
    if isinstance(raw, str):
        try:
            items = json.loads(raw)
        except Exception:
            items = [raw]
    else:
        items = raw if isinstance(raw, list) else [str(raw)]
    seen: set = set()
    clean = []
    for item in items:
        s = str(item).strip()
        low = s.lower()
        if low in _FINDING_NOISE:
            continue
        if len(s) < 15:
            continue
        if _re.match(r'^[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$', s):
            continue
        if s not in seen:
            seen.add(s)
            clean.append(s)
    if not clean:
        return "No exceptions noted"
    return "; ".join(clean[:2])


def _fmt_date(date_str: Optional[str]) -> str:
    if not date_str:
        return "—"
    try:
        return datetime.fromisoformat(date_str[:10]).strftime("%d %b %Y")
    except ValueError:
        return date_str


def _accession_url(cik: str, accession: str) -> str:
    acc_plain = accession.replace("-", "")
    cik_plain = cik.lstrip("0")
    return EDGAR_ACCESSION_BASE.format(
        cik=cik_plain, acc_plain=acc_plain, acc_dashed=accession
    )


def _issuer_edgar_url(cik: str) -> str:
    return EDGAR_FILING_BASE.format(cik=cik.lstrip("0"))


def _badge_html(status: str) -> str:
    cls = f"badge-{status.lower()}"
    return f'<span class="{cls}">{status.upper()}</span>'


def _sector_badge_html(issuer_type: str) -> str:
    label, color = SECTOR_BADGES.get(
        issuer_type, ("ABS", "#7b5ea7")
    )
    return (
        f'<span class="sector-badge" '
        f'style="background:{color}22;color:{color};border:1px solid {color}55;">'
        f"{label}</span>"
    )


def _filing_count_per_issuer() -> dict[str, int]:
    all_filings = db.get_filings(limit=10_000, db_path=DB_PATH)
    counts: dict[str, int] = {}
    for f in all_filings:
        counts[f["issuer_key"]] = counts.get(f["issuer_key"], 0) + 1
    return counts


def _latest_date_per_issuer() -> dict[str, Optional[str]]:
    return {key: db.get_latest_filed_date(key, db_path=DB_PATH) for key in ISSUERS}


def _dark_plotly_layout() -> dict:
    return dict(
        paper_bgcolor="#1e1e3f",
        plot_bgcolor="#141428",
        font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                  size=11, color="#94a3b8"),
        title_font_color="#f1f5f9",
        legend=dict(
            bgcolor="#1e1e3f",
            bordercolor="#2d2d5e",
            borderwidth=1,
            font=dict(color="#94a3b8", size=10),
        ),
        xaxis=dict(
            gridcolor="#2d2d5e", linecolor="#2d2d5e",
            tickfont=dict(color="#94a3b8"), title_font=dict(color="#64748b"),
        ),
        yaxis=dict(
            gridcolor="#2d2d5e", linecolor="#2d2d5e",
            tickfont=dict(color="#94a3b8"), title_font=dict(color="#64748b"),
        ),
        margin=dict(t=40, b=20, l=10, r=10),
    )


_TABLE_CSS = """<style>
.styled-table{width:100%;border-collapse:collapse;font-size:0.8rem;margin-top:0.5rem;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}
.styled-table thead th{background:#141428!important;color:#64748b!important;font-weight:600;font-size:0.72rem;text-transform:uppercase;letter-spacing:0.05em;padding:0.55rem 0.85rem;text-align:left;}
.styled-table tbody tr{border-bottom:1px solid #1e1e3f;}
.styled-table tbody tr:hover{background:#1e1e3f;}
.styled-table tbody td{padding:0.55rem 0.85rem;color:#cbd5e1;vertical-align:middle;}
.styled-table a{color:#a78bfa;text-decoration:none;}
.styled-table a:hover{text-decoration:underline;}
.sector-badge{display:inline-block;font-size:0.66rem;font-weight:700;letter-spacing:0.06em;padding:2px 7px;border-radius:3px;}
.cik-badge{font-family:monospace;font-size:0.78rem;color:#7b5ea7;}
.styled-table thead th.sortable{cursor:pointer;user-select:none;}
.styled-table thead th.sortable:hover{color:#a78bfa!important;}
.styled-table thead th .sort-icon{margin-left:4px;opacity:0.6;font-style:normal;}
.styled-table thead th.asc .sort-icon::after{content:"▲";opacity:1;}
.styled-table thead th.desc .sort-icon::after{content:"▼";opacity:1;}
.styled-table thead th:not(.asc):not(.desc) .sort-icon::after{content:"⇅";}
</style>"""

_SORT_JS = """<script>
document.addEventListener('click', function(e) {
  var th = e.target.closest('thead th.sortable');
  if (!th) return;
  var table = th.closest('table.sortable-table');
  if (!table) return;
  if (!table._sort) table._sort = {col: -1, asc: true};
  var ci = Array.from(th.parentElement.children).indexOf(th);
  if (table._sort.col === ci) { table._sort.asc = !table._sort.asc; }
  else { table._sort.col = ci; table._sort.asc = true; }
  table.querySelectorAll('thead th').forEach(function(h) { h.classList.remove('asc','desc'); });
  th.classList.add(table._sort.asc ? 'asc' : 'desc');
  var tbody = table.querySelector('tbody');
  var rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort(function(a, b) {
    var av = (a.cells[ci] ? a.cells[ci].innerText.trim() : '');
    var bv = (b.cells[ci] ? b.cells[ci].innerText.trim() : '');
    var an = parseFloat(av.replace(/[,%]/g,'')), bn = parseFloat(bv.replace(/[,%]/g,''));
    var cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv);
    return table._sort.asc ? cmp : -cmp;
  });
  rows.forEach(function(r) { tbody.appendChild(r); });
});
</script>"""

# Columns that are right-aligned (numeric)
_COL_RIGHT = {"Pool Size", "Sample", "Fields", "Findings", "Finding %",
              "Avg Exception Rate (%)", "Min (%)", "Max (%)", "# Procedures"}
# Columns that must not wrap
_COL_NOWRAP = {"Filing Date", "Trust Series"}


def _table_html(df: pd.DataFrame, sortable: bool = False) -> str:
    """Render a DataFrame as a dark-styled HTML table (self-contained for st.html)."""
    rows_html = ""
    for _, row in df.iterrows():
        cells = ""
        for col, val in zip(df.columns, row):
            style = ' style="text-align:right"' if col in _COL_RIGHT else ""
            cells += f"<td{style}>{val}</td>"
        rows_html += f"<tr>{cells}</tr>"

    headers = ""
    for col in df.columns:
        parts = []
        if sortable:
            parts.append('class="sortable"')
        th_styles = []
        if col in _COL_RIGHT:
            th_styles.append("text-align:right")
        if col in _COL_NOWRAP:
            th_styles.append("white-space:nowrap")
        if th_styles:
            parts.append(f'style="{";".join(th_styles)}"')
        attrs = (" " + " ".join(parts)) if parts else ""
        icon = " <i class='sort-icon'></i>" if sortable else ""
        headers += f"<th{attrs}>{col}{icon}</th>"

    tbl_class = "styled-table sortable-table" if sortable else "styled-table"
    return (
        f"{_TABLE_CSS}"
        f'<div style="overflow-x:auto;">'
        f'<table class="{tbl_class}"><thead><tr>{headers}</tr></thead>'
        f"<tbody>{rows_html}</tbody></table></div>"
        f"{_SORT_JS if sortable else ''}"
    )


# ---------------------------------------------------------------------------
# Header bar
# ---------------------------------------------------------------------------

now_utc = datetime.now(timezone.utc)
last_updated_str = now_utc.strftime("%d %b %Y %H:%M UTC")

st.markdown(
    f"""
    <div class="finsight-header">
        <div class="page-wrapper" style="display:flex;align-items:center;gap:1.25rem;width:100%;padding-top:0;padding-bottom:0;">
            <div class="finsight-logo">
                <span class="finsight-logo-badge">ABS</span>ABS-15G AUP Monitor
            </div>
            <div class="header-spacer"></div>
            <div class="finsight-updated-badge">Last Updated: {last_updated_str}</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Main tab layout
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Market Overview",
    "Issuer Profiles",
    "AUP Results",
    "DQA Report",
    "Update Log",
])


# ===========================================================================
# TAB 1 — MARKET OVERVIEW
# ===========================================================================

with tab1:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)

    # --- Summary metrics ---
    all_filings = db.get_filings(limit=10_000, db_path=DB_PATH) if DB_PATH.exists() else []
    total_issuers = len(ISSUERS)
    total_filings = len(all_filings)

    if all_filings:
        dates = [f["filed_date"] for f in all_filings if f.get("filed_date")]
        last_filing_date = max(dates) if dates else None
    else:
        last_filing_date = None

    st.markdown(
        f"""
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-label">Issuers Tracked</div>
                <div class="metric-value">{total_issuers}</div>
                <div class="metric-sub">ABS-15G registered filers</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Filings in DB</div>
                <div class="metric-value">{total_filings:,}</div>
                <div class="metric-sub">Across all issuers</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Latest Filing Date</div>
                <div class="metric-value" style="font-size:1.3rem;">{_fmt_date(last_filing_date)}</div>
                <div class="metric-sub">Most recent in database</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Run Update button ---
    col_btn, col_msg = st.columns([1, 5])
    with col_btn:
        run_update = st.button("Run Update", use_container_width=True)

    if run_update:
        try:
            aup_updater = importlib.import_module("aup_updater")
            with st.spinner("Running updater — this may take a few minutes…"):
                aup_updater.update_all_issuers()
            col_msg.success("Update complete. Refresh the page to see new data.")
        except ModuleNotFoundError:
            col_msg.error(
                "aup_updater.py not found. Place it in the same directory as this dashboard."
            )
        except Exception as exc:  # noqa: BLE001
            col_msg.error(f"Updater error: {exc}")
            with st.expander("Traceback"):
                st.code(traceback.format_exc())

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Issuer Profiles grid ---
    st.markdown('<div class="finsight-section-title">Issuer Profiles</div>', unsafe_allow_html=True)

    if not DB_PATH.exists():
        _no_data_banner()
    else:
        filing_counts = _filing_count_per_issuer()
        latest_dates = _latest_date_per_issuer()

        # Build card HTML for each issuer
        _CARD_CSS = """
        <style>
        .issuer-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:0.9rem;margin-bottom:2rem;}
        @media(max-width:1100px){.issuer-grid{grid-template-columns:repeat(2,1fr);}}
        .issuer-card{background:#1e1e3f;border:1px solid #2d2d5e;border-radius:8px;padding:1rem 1.1rem 0.9rem;transition:border-color 0.15s,transform 0.12s;cursor:default;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}
        .issuer-card:hover{border-color:#7b5ea7;transform:translateY(-1px);}
        .sector-badge{display:inline-block;font-size:0.66rem;font-weight:700;letter-spacing:0.06em;padding:2px 7px;border-radius:3px;margin-bottom:0.5rem;}
        .issuer-name{font-size:0.9rem;font-weight:700;color:#f1f5f9;margin-bottom:0.35rem;line-height:1.25;}
        .issuer-meta{font-size:0.73rem;color:#94a3b8;line-height:1.6;}
        .hi{color:#a78bfa;}
        </style>
        """
        cards_html = _CARD_CSS + '<div class="issuer-grid">'
        for key, issuer in ISSUERS.items():
            badge_html = _sector_badge_html(issuer.get("type", ""))
            count = filing_counts.get(key, 0)
            latest = _fmt_date(latest_dates.get(key))
            itype_label = issuer.get("type", "").replace("_", " ").title()
            active_dot = (
                '<span style="color:#00c896;font-size:0.7rem;">&#9679; Active</span>'
                if issuer.get("active")
                else '<span style="color:#64748b;font-size:0.7rem;">&#9679; Inactive</span>'
            )
            cards_html += f"""
            <div class="issuer-card">
                {badge_html}
                <div class="issuer-name">{issuer["name"]}</div>
                <div class="issuer-meta">
                    {active_dot}<br>
                    <span style="color:#64748b;">Type:</span> {itype_label}<br>
                    <span style="color:#64748b;">Latest:</span>
                    <span class="hi">{latest}</span><br>
                    <span style="color:#64748b;">Filings:</span>
                    <span class="hi">{count}</span>
                </div>
            </div>
            """
        cards_html += "</div>"
        st.html(cards_html)

        if total_filings == 0:
            _no_data_banner()

    # --- Full issuer table ---
    if DB_PATH.exists() and total_filings > 0:
        st.markdown(
            '<div class="finsight-section-title" style="margin-top:1.5rem;">All Issuers — Detail Table</div>',
            unsafe_allow_html=True,
        )
        filing_counts = _filing_count_per_issuer()
        latest_dates = _latest_date_per_issuer()

        rows = []
        for key, issuer in ISSUERS.items():
            edgar_url = _issuer_edgar_url(issuer["cik"])
            label, color = SECTOR_BADGES.get(issuer.get("type", ""), ("ABS", "#7b5ea7"))
            badge_cell = (
                f'<span class="sector-badge" style="background:{color}22;color:{color};'
                f'border:1px solid {color}55;">{label}</span>'
            )
            rows.append({
                "Sector": badge_cell,
                "Issuer": f'<b style="color:#f1f5f9;">{issuer["name"]}</b>',
                "CIK": f'<span class="cik-badge">{issuer["cik"]}</span>',
                "Type": issuer.get("type", "").replace("_", " ").title(),
                "Latest Filing": _fmt_date(latest_dates.get(key)),
                "Filings": filing_counts.get(key, 0),
                "EDGAR": f'<a href="{edgar_url}" target="_blank">View &#8599;</a>',
                "Active": "Yes" if issuer.get("active") else "No",
            })

        df_issuers = pd.DataFrame(rows)
        st.html(_table_html(df_issuers))

    st.markdown("</div></div>", unsafe_allow_html=True)


# ===========================================================================
# TAB 2 — ISSUER PROFILES
# ===========================================================================

with tab2:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)
    st.markdown('<div class="finsight-section-title">Issuer Profiles</div>', unsafe_allow_html=True)

    issuer_options = {v["name"]: k for k, v in ISSUERS.items()}

    col_sel, col_spacer = st.columns([2, 5])
    with col_sel:
        st.markdown('<div class="filter-label">Select Issuer</div>', unsafe_allow_html=True)
        selected_name = st.selectbox(
            "Issuer", list(issuer_options.keys()), label_visibility="collapsed"
        )

    selected_key = issuer_options[selected_name]
    issuer_info = ISSUERS[selected_key]
    edgar_url = _issuer_edgar_url(issuer_info["cik"])
    label, color = SECTOR_BADGES.get(issuer_info.get("type", ""), ("ABS", "#7b5ea7"))
    badge_html_str = (
        f'<span class="sector-badge" style="background:{color}22;color:{color};'
        f'border:1px solid {color}55;font-size:0.8rem;padding:3px 10px;">{label}</span>'
    )

    st.markdown(
        f"""
        <div class="issuer-detail-header">
            {badge_html_str}
            <div class="issuer-detail-name">{issuer_info["name"]}</div>
            <span class="cik-badge">CIK {issuer_info["cik"]}</span>
            <div class="header-spacer"></div>
            <div class="issuer-detail-links">
                <a href="{edgar_url}" target="_blank">View ABS-15G filings on EDGAR &#8599;</a>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not DB_PATH.exists():
        _no_data_banner()
        st.stop()

    filings = db.get_filings(issuer_key=selected_key, limit=200, db_path=DB_PATH)

    # --- Filing History ---
    st.markdown('<div class="finsight-section-title">Filing History</div>', unsafe_allow_html=True)

    if not filings:
        st.markdown(
            f'<div class="info-box">No filings found for {selected_name}. '
            "Run the updater to fetch from EDGAR.</div>",
            unsafe_allow_html=True,
        )
    else:
        filing_rows = []
        for f in filings:
            acc = f.get("accession_number", "")
            cik = f.get("cik", issuer_info["cik"])
            filing_url = _accession_url(cik, acc) if acc else ""
            filing_rows.append({
                "Filing Date": _fmt_date(f.get("filed_date")),
                "Period of Report": _fmt_date(f.get("period_of_report")),
                "Form Type": f.get("form_type", "ABS-15G"),
                "Accession Number": f'<span class="cik-badge">{acc}</span>' if acc else "—",
                "Filing Index": (
                    f'<a href="{filing_url}" target="_blank">View &#8599;</a>'
                    if filing_url else "—"
                ),
            })
        st.html(_table_html(pd.DataFrame(filing_rows)))

    st.markdown("<br>", unsafe_allow_html=True)

    # --- AUP Procedure Findings ---
    st.markdown(
        '<div class="finsight-section-title">AUP Procedure Findings</div>',
        unsafe_allow_html=True,
    )

    aup_results = db.get_aup_results(issuer_key=selected_key, limit=1000, db_path=DB_PATH)

    if not aup_results:
        st.markdown(
            '<div class="info-box">No AUP findings stored yet for this issuer.</div>',
            unsafe_allow_html=True,
        )
    else:
        result_rows = []
        for r in aup_results:
            exc_rate = r.get("exception_rate")
            exc_rate_str = f"{exc_rate * 100:.2f}%" if exc_rate is not None else "—"
            pool_raw = r.get("pool_size")
            pool_str = f"{int(float(pool_raw)):,}" if pool_raw and str(pool_raw).strip() not in ("", "—", "None", "nan") else "—"
            fields_raw = r.get("fields_count")
            result_rows.append({
                "Trust Series": r.get("deal_name") or "—",
                "Filing Date": _fmt_date(r.get("filed_date")),
                "Auditor": r.get("aup_provider") or "—",
                "Pool Size": pool_str,
                "Sample": r.get("sample_size") or "—",
                "Fields": str(int(fields_raw)) if fields_raw is not None else "—",
                "Findings": r.get("exception_count") if r.get("exception_count") is not None else "—",
                "Finding %": exc_rate_str,
                "Finding Details": _fmt_finding(r.get("finding")),
            })
        df_res = pd.DataFrame(result_rows)
        st.html(_table_html(df_res))

    st.markdown("</div></div>", unsafe_allow_html=True)


# ===========================================================================
# TAB 3 — AUP RESULTS
# ===========================================================================

with tab3:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)
    st.markdown('<div class="finsight-section-title">AUP Results — Cross-Issuer Analysis</div>', unsafe_allow_html=True)

    if not _db_ready():
        _no_data_banner()
    else:
        all_results = db.get_aup_results(limit=5000, db_path=DB_PATH)

        if not all_results:
            _no_data_banner()
        else:
            df = pd.DataFrame(all_results)
            df["filed_date"] = pd.to_datetime(df["filed_date"], errors="coerce")
            df["exception_rate_pct"] = df["exception_rate"].apply(
                lambda x: round(x * 100, 4) if pd.notna(x) else 0.0
            )
            name_map = {k: v["name"] for k, v in ISSUERS.items()}
            df["Issuer"] = df["issuer_key"].map(name_map).fillna(df["issuer_key"])

            # Normalize asset_type label
            _ASSET_LABELS = {"consumer_loan": "Consumer Loan", "auto": "Auto Loan"}
            df["asset_type"] = df["asset_type"].fillna("consumer_loan")
            df["Asset Type"] = df["asset_type"].map(_ASSET_LABELS).fillna(df["asset_type"])

            # Build filter options from full data BEFORE search so selections persist
            all_issuer_opts = ["All Issuers"] + sorted(df["Issuer"].dropna().unique().tolist())
            all_asset_opts = ["All Asset Types"] + sorted(df["Asset Type"].dropna().unique().tolist())

            # --- Search bar ---
            search_query = st.text_input(
                "Search",
                placeholder="🔍  Search by issuer, deal name, auditor, finding details…",
                label_visibility="collapsed",
            )

            # --- Filters ---
            fcol1, fcol2 = st.columns(2)
            with fcol1:
                st.markdown('<div class="filter-label">Issuer</div>', unsafe_allow_html=True)
                issuer_filter_sel = st.selectbox(
                    "IssuerFilter", all_issuer_opts, label_visibility="collapsed", key="issuer_filter"
                )
            with fcol2:
                st.markdown('<div class="filter-label">Asset Type</div>', unsafe_allow_html=True)
                asset_filter_sel = st.selectbox(
                    "AssetFilter", all_asset_opts, label_visibility="collapsed", key="asset_filter"
                )

            # Apply search
            if search_query:
                q = search_query.lower()
                mask = (
                    df["Issuer"].str.lower().str.contains(q, na=False)
                    | df["deal_name"].fillna("").str.lower().str.contains(q, na=False)
                    | df["aup_provider"].fillna("").str.lower().str.contains(q, na=False)
                    | df["finding"].fillna("").str.lower().str.contains(q, na=False)
                    | df["issuer_key"].str.lower().str.contains(q, na=False)
                )
                df = df[mask]

            # Apply filters
            df_filtered = df.copy()
            if issuer_filter_sel != "All Issuers":
                df_filtered = df_filtered[df_filtered["Issuer"] == issuer_filter_sel]
            if asset_filter_sel != "All Asset Types":
                df_filtered = df_filtered[df_filtered["Asset Type"] == asset_filter_sel]

            st.markdown("<br>", unsafe_allow_html=True)

            # --- Trend chart ---
            st.markdown(
                '<div class="finsight-section-title">Exception Rate Trend Over Time</div>',
                unsafe_allow_html=True,
            )

            issuers_with_data = sorted(df_filtered["Issuer"].dropna().unique())
            default_sel = issuers_with_data[:5] if len(issuers_with_data) > 5 else issuers_with_data

            st.markdown('<div class="filter-label">Select Issuers for Trend</div>', unsafe_allow_html=True)
            selected_trend_issuers = st.multiselect(
                "TrendIssuers",
                options=issuers_with_data,
                default=list(default_sel),
                label_visibility="collapsed",
            )

            df_trend = (
                df_filtered[df_filtered["Issuer"].isin(selected_trend_issuers)]
                .dropna(subset=["filed_date", "Issuer"])
                .groupby(["Issuer", "filed_date"], as_index=False)["exception_rate_pct"]
                .mean()
                .sort_values("filed_date")
            )

            if df_trend.empty:
                st.markdown(
                    '<div class="info-box">No trend data for the selected issuers.</div>',
                    unsafe_allow_html=True,
                )
            else:
                df_trend_plot = df_trend[["Issuer", "filed_date", "exception_rate_pct"]].copy()
                df_trend_plot["Issuer"] = df_trend_plot["Issuer"].astype(str)
                df_trend_plot["filed_date"] = pd.to_datetime(df_trend_plot["filed_date"], errors="coerce")
                df_trend_plot["exception_rate_pct"] = pd.to_numeric(df_trend_plot["exception_rate_pct"], errors="coerce").fillna(0.0)
                df_trend_plot = df_trend_plot.dropna(subset=["filed_date"])
                fig_trend = px.line(
                    df_trend_plot,
                    x="filed_date",
                    y="exception_rate_pct",
                    color="Issuer",
                    markers=True,
                    hover_data={"filed_date": False, "Issuer": False, "exception_rate_pct": ":.4f"},
                    labels={
                        "filed_date": "Filing Date",
                        "exception_rate_pct": "Avg Exception Rate (%)",
                    },
                    color_discrete_sequence=ISSUER_PALETTE,
                )
                fig_trend.update_layout(**_dark_plotly_layout())
                fig_trend.update_layout(
                    legend=dict(
                        orientation="h", y=-0.18,
                        bgcolor="#1e1e3f", bordercolor="#2d2d5e", borderwidth=1,
                        font=dict(color="#94a3b8", size=10),
                    ),
                    height=380,
                )
                fig_trend.update_traces(line=dict(width=2), marker=dict(size=6))
                st.plotly_chart(fig_trend, use_container_width=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # --- Summary statistics ---
            st.markdown(
                '<div class="finsight-section-title">Summary Statistics by Issuer</div>',
                unsafe_allow_html=True,
            )

            df_summary = (
                df_filtered.dropna(subset=["exception_rate_pct"])
                .groupby("Issuer")["exception_rate_pct"]
                .agg(Avg="mean", Min="min", Max="max", Count="count")
                .reset_index()
                .rename(columns={
                    "Avg": "Avg Exception Rate (%)",
                    "Min": "Min (%)",
                    "Max": "Max (%)",
                    "Count": "# Procedures",
                })
                .sort_values("Avg Exception Rate (%)", ascending=False)
            )
            for col in ["Avg Exception Rate (%)", "Min (%)", "Max (%)"]:
                df_summary[col] = df_summary[col].map(lambda x: f"{x:.4f}")

            st.html(_table_html(df_summary))

            st.markdown("<br>", unsafe_allow_html=True)

            # --- Full data table ---
            st.markdown(
                '<div class="finsight-section-title">All AUP Findings</div>',
                unsafe_allow_html=True,
            )

            df_filtered["deal_name"] = df_filtered["deal_name"].fillna("—")
            df_filtered["aup_provider"] = df_filtered["aup_provider"].fillna("—")
            display_cols = ["Issuer", "Asset Type", "deal_name", "filed_date", "aup_provider",
                            "pool_size", "sample_size", "fields_count",
                            "exception_count", "exception_rate_pct", "finding"]
            df_display = df_filtered[display_cols].copy()
            df_display.columns = [
                "Company", "Asset Type", "Trust Series", "Filing Date", "Auditor",
                "Pool Size", "Sample", "Fields",
                "Findings", "Finding %", "Finding Details",
            ]
            df_display["Finding Details"] = df_display["Finding Details"].apply(_fmt_finding)
            df_display["Filing Date"] = df_display["Filing Date"].apply(
                lambda x: x.strftime("%d %b %Y") if pd.notna(x) else "—"
            )
            df_display["Pool Size"] = df_display["Pool Size"].apply(
                lambda x: f"{int(float(x)):,}" if x is not None and str(x).strip() not in ("", "—", "None", "nan") else "—"
            )
            df_display["Fields"] = df_display["Fields"].apply(
                lambda x: str(int(x)) if pd.notna(x) and x not in (None, "") else "—"
            )
            df_display["Findings"] = df_display["Findings"].apply(
                lambda x: str(int(x)) if pd.notna(x) and x not in (None, "") else "—"
            )
            df_display["Finding %"] = df_display["Finding %"].apply(
                lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0.0 else ("0.00%" if x == 0.0 else "—")
            )
            # Use components.html so JavaScript click events work (st.html iframes block them)
            tbl_height = min(40 + len(df_display) * 38 + 20, 600)
            components.html(_table_html(df_display, sortable=True), height=tbl_height, scrolling=True)

    st.markdown("</div></div>", unsafe_allow_html=True)


# ===========================================================================
# TAB 4 — DQA REPORT
# ===========================================================================

with tab4:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)
    st.markdown('<div class="finsight-section-title">Data Quality Assurance Report</div>', unsafe_allow_html=True)

    # --- Status summary from DB ---
    if DB_PATH.exists():
        all_dqa = db.get_dqa_log(limit=2000, db_path=DB_PATH)
    else:
        all_dqa = []

    if all_dqa:
        df_dqa_all = pd.DataFrame(all_dqa)
        status_counts = df_dqa_all["status"].value_counts().to_dict()

        pass_n  = status_counts.get("pass", 0)
        fail_n  = status_counts.get("fail", 0)
        warn_n  = status_counts.get("warn", 0)
        skip_n  = status_counts.get("skip", 0)
        total_n = sum(status_counts.values())

        st.markdown(
            f"""
            <div class="metric-grid">
                <div class="metric-card" style="border-color:#00c89640;">
                    <div class="metric-label" style="color:#00c896;">Pass</div>
                    <div class="metric-value" style="color:#00c896;">{pass_n}</div>
                    <div class="metric-sub">of {total_n} checks</div>
                </div>
                <div class="metric-card" style="border-color:#ff475740;">
                    <div class="metric-label" style="color:#ff4757;">Fail</div>
                    <div class="metric-value" style="color:#ff4757;">{fail_n}</div>
                    <div class="metric-sub">of {total_n} checks</div>
                </div>
                <div class="metric-card" style="border-color:#ffd16640;">
                    <div class="metric-label" style="color:#ffd166;">Warn / Skip</div>
                    <div class="metric-value" style="color:#ffd166;">{warn_n + skip_n}</div>
                    <div class="metric-sub">of {total_n} checks</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Donut chart
        fig_pie = px.pie(
            pd.DataFrame([
                {"Status": k.upper(), "Count": v}
                for k, v in status_counts.items()
            ]),
            names="Status",
            values="Count",
            hole=0.5,
            color="Status",
            color_discrete_map={
                "PASS": "#00c896",
                "FAIL": "#ff4757",
                "WARN": "#ffd166",
                "SKIP": "#94a3b8",
            },
        )
        fig_pie.update_traces(
            textinfo="label+percent",
            textfont=dict(color="#f1f5f9", size=11),
        )
        pie_layout = _dark_plotly_layout()
        pie_layout["showlegend"] = False
        pie_layout["height"] = 300
        fig_pie.update_layout(**pie_layout)

        col_pie, col_filt = st.columns([1, 2])
        with col_pie:
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_filt:
            st.markdown('<div class="finsight-section-title">Filter DQA Log</div>', unsafe_allow_html=True)
            d_col1, d_col2, d_col3 = st.columns(3)
            with d_col1:
                st.markdown('<div class="filter-label">Issuer</div>', unsafe_allow_html=True)
                dqa_issuer_opts = ["All"] + sorted(ISSUERS.keys())
                dqa_issuer = st.selectbox(
                    "DQAIssuer", dqa_issuer_opts, label_visibility="collapsed"
                )
            with d_col2:
                st.markdown('<div class="filter-label">Status</div>', unsafe_allow_html=True)
                dqa_status = st.selectbox(
                    "DQAStatus", ["All", "pass", "fail", "warn", "skip"],
                    label_visibility="collapsed"
                )
            with d_col3:
                st.markdown('<div class="filter-label">Max Rows</div>', unsafe_allow_html=True)
                dqa_limit = st.number_input(
                    "DQALimit", min_value=10, max_value=2000, value=200, step=10,
                    label_visibility="collapsed"
                )

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown(
            '<div class="finsight-section-title">DQA Check Log</div>', unsafe_allow_html=True
        )

        dqa_rows = db.get_dqa_log(
            issuer_key=None if dqa_issuer == "All" else dqa_issuer,
            status=None if dqa_status == "All" else dqa_status,
            limit=int(dqa_limit),
            db_path=DB_PATH,
        )

        if dqa_rows:
            display_rows = []
            for r in dqa_rows:
                iname = ISSUERS.get(r["issuer_key"], {}).get("name", r["issuer_key"])
                display_rows.append({
                    "Date": _fmt_date(r.get("check_date")),
                    "Issuer": iname,
                    "Check Type": r.get("check_type", "—"),
                    "Status": _badge_html(r.get("status", "skip")),
                    "Filing ID": r.get("filing_id", "—"),
                    "Notes": r.get("notes", "—"),
                })
            st.html(_table_html(pd.DataFrame(display_rows)))
        else:
            st.markdown(
                '<div class="info-box">No records match the selected filters.</div>',
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            '<div class="info-box">No DQA log entries found. Run the updater to generate check records.</div>',
            unsafe_allow_html=True,
        )

    # --- DQA Report markdown file ---
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        '<div class="finsight-section-title">Data Quality Assurance Report (dqa_report.md)</div>',
        unsafe_allow_html=True,
    )

    if DQA_REPORT_PATH.exists():
        dqa_md_text = DQA_REPORT_PATH.read_text(encoding="utf-8")
        with st.expander("Expand full DQA report", expanded=True):
            st.markdown(
                f'<div style="background:#1e1e3f;border:1px solid #2d2d5e;border-radius:8px;'
                f'padding:1.25rem 1.5rem;font-size:0.84rem;line-height:1.7;color:#cbd5e1;">'
                f"</div>",
                unsafe_allow_html=True,
            )
            st.markdown(dqa_md_text)
    else:
        st.markdown(
            '<div class="info-box">dqa_report.md not found in the project directory.</div>',
            unsafe_allow_html=True,
        )

    st.markdown("</div></div>", unsafe_allow_html=True)


# ===========================================================================
# TAB 5 — UPDATE LOG
# ===========================================================================

with tab5:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)
    st.markdown('<div class="finsight-section-title">Update Log</div>', unsafe_allow_html=True)

    if not DB_PATH.exists():
        _no_data_banner()
    else:
        all_filings_log = db.get_filings(limit=10_000, db_path=DB_PATH)

        if not all_filings_log:
            _no_data_banner()
        else:
            # Last run approximated by newest created_at
            created_ats = [f.get("created_at") for f in all_filings_log if f.get("created_at")]
            last_run_ts = max(created_ats) if created_ats else None

            def _is_recent(ts_str: str) -> bool:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    return (now_utc - ts).total_seconds() < 86_400
                except Exception:
                    return False

            recent_filings = [
                f for f in all_filings_log if _is_recent(f.get("created_at", ""))
            ]
            all_dqa_log = db.get_dqa_log(limit=500, db_path=DB_PATH)
            recent_errors = [
                r for r in all_dqa_log
                if r.get("status") in ("fail", "warn") and _is_recent(r.get("created_at", ""))
            ]

            st.markdown(
                f"""
                <div class="metric-grid">
                    <div class="metric-card">
                        <div class="metric-label">Last Run</div>
                        <div class="metric-value" style="font-size:1.1rem;">
                            {_fmt_date(last_run_ts[:10]) if last_run_ts else "Unknown"}
                        </div>
                        <div class="metric-sub">{last_run_ts or "No timestamp"}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">New Filings (24h)</div>
                        <div class="metric-value" style="color:#00c896;">{len(recent_filings)}</div>
                        <div class="metric-sub">Ingested in last 24 hours</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Errors / Warnings (24h)</div>
                        <div class="metric-value" style="color:{'#ff4757' if recent_errors else '#00c896'};">
                            {len(recent_errors)}
                        </div>
                        <div class="metric-sub">DQA checks flagged</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # --- New filings feed ---
            st.markdown(
                '<div class="finsight-section-title">New Filings — Last 24 Hours</div>',
                unsafe_allow_html=True,
            )

            if not recent_filings:
                st.markdown(
                    '<div class="info-box">No new filings ingested in the last 24 hours.</div>',
                    unsafe_allow_html=True,
                )
            else:
                feed_html = ""
                for f in recent_filings:
                    iname = ISSUERS.get(f["issuer_key"], {}).get("name", f["issuer_key"])
                    acc = f.get("accession_number", "")
                    cik = f.get("cik", "")
                    url = _accession_url(cik, acc) if acc and cik else ""
                    link = f' — <a href="{url}" target="_blank" style="color:#a78bfa;">View &#8599;</a>' if url else ""
                    feed_html += f"""
                    <div class="activity-item">
                        <div class="activity-dot"></div>
                        <div>
                            <div class="activity-text">
                                <b style="color:#f1f5f9;">{iname}</b> filed
                                <span style="color:#a78bfa;">{f.get("form_type","ABS-15G")}</span>
                                for period {_fmt_date(f.get("period_of_report"))}{link}
                            </div>
                            <div class="activity-time">Ingested: {f.get("created_at","—")} &bull; Filing date: {_fmt_date(f.get("filed_date"))}</div>
                        </div>
                    </div>
                    """
                st.markdown(
                    f'<div style="background:#1e1e3f;border:1px solid #2d2d5e;border-radius:8px;padding:0.5rem 1rem;">'
                    f"{feed_html}</div>",
                    unsafe_allow_html=True,
                )

            st.markdown("<br>", unsafe_allow_html=True)

            # --- Errors / warnings ---
            st.markdown(
                '<div class="finsight-section-title">Recent Errors and Warnings</div>',
                unsafe_allow_html=True,
            )

            if not recent_errors:
                st.markdown(
                    '<div class="info-box" style="border-left-color:#00c896;">'
                    "No errors or warnings recorded in the last 24 hours.</div>",
                    unsafe_allow_html=True,
                )
            else:
                err_rows = []
                for r in recent_errors:
                    iname = ISSUERS.get(r["issuer_key"], {}).get("name", r["issuer_key"])
                    err_rows.append({
                        "Issuer": iname,
                        "Check Type": r.get("check_type", "—"),
                        "Status": _badge_html(r.get("status", "warn")),
                        "Notes": r.get("notes", "—"),
                        "Logged At": r.get("created_at", "—"),
                    })
                st.html(_table_html(pd.DataFrame(err_rows)))

            st.markdown("<br>", unsafe_allow_html=True)

            # --- Coverage bar chart ---
            st.markdown(
                '<div class="finsight-section-title">All-Time Filing Coverage by Issuer</div>',
                unsafe_allow_html=True,
            )

            filing_counts_log = _filing_count_per_issuer()
            if filing_counts_log:
                cov_rows = [
                    {"Issuer": ISSUERS.get(k, {}).get("name", k), "Filings": v}
                    for k, v in sorted(filing_counts_log.items(), key=lambda x: -x[1])
                ]
                df_cov = pd.DataFrame(cov_rows)
                fig_cov = px.bar(
                    df_cov,
                    x="Issuer",
                    y="Filings",
                    color="Filings",
                    color_continuous_scale=[[0, "#2d2d5e"], [0.5, "#7b5ea7"], [1, "#a78bfa"]],
                    labels={"Filings": "Total Filings"},
                )
                fig_cov.update_layout(**_dark_plotly_layout())
                fig_cov.update_layout(
                    coloraxis_showscale=False,
                    xaxis_tickangle=-30,
                    height=360,
                    xaxis_title="",
                    yaxis_title="Total Filings",
                )
                st.plotly_chart(fig_cov, use_container_width=True)
            else:
                st.markdown(
                    '<div class="info-box">No filing data available for coverage chart.</div>',
                    unsafe_allow_html=True,
                )

    st.markdown("</div></div>", unsafe_allow_html=True)

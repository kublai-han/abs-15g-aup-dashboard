"""
aup_dashboard.py

Streamlit dashboard for the ABS-15G AUP Aggregator project.
Finsight-inspired dark theme layout.

Tabs
----
1. AUP Results         -- cross-issuer exception rate comparisons & trend charts
2. ABS-15G Filings     -- filing history + AUP results for a selected issuer
3. Edgar Filings       -- summary cards, issuer profile grid
4. Rating Methodology  -- scoring approach documentation
"""

from __future__ import annotations

import importlib
import re
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
    "nqm":                 ("NQM",  "#d97706"),
    "second_lien":         ("2ND",  "#b45309"),
    "rpl":                 ("RPL",  "#c2410c"),
    "prime_jumbo":         ("PJ",   "#0d9488"),
    "inv_property":        ("INV",  "#7c3aed"),
    "npl":                 ("NPL",  "#92400e"),
    "sfr":                 ("SFR",  "#78350f"),
    "rtl":                 ("RTL",  "#a16207"),
    "small_business_loan": ("SMB",  "#db2777"),
    "credit_card":         ("CC",   "#0ea5e9"),
    "aircraft":            ("AIR",  "#0891b2"),
    "datacenter":          ("DC",   "#6366f1"),
    "fiber":               ("FBR",  "#16a34a"),
    "fleet_lease":         ("FLT",  "#ea580c"),
    "conduit":             ("CNDT", "#d97706"),
    "cre_clo":             ("CRE",  "#b45309"),
    "large_loan":          ("SASB", "#f59e0b"),
    "cmbs_other":          ("CMBS", "#92400e"),
}

ASSET_TYPE_LABELS: dict[str, str] = {
    "consumer_loan":       "Consumer Loan",
    "auto":                "Auto",
    "student_loan":        "Student Loan",
    "mortgage":            "Mortgage",
    "nqm":                 "Non-QM Mortgage",
    "second_lien":         "Second Lien",
    "rpl":                 "Re-Performing Loan",
    "prime_jumbo":         "Prime Jumbo",
    "inv_property":        "Investment Property",
    "npl":                 "Non-Performing Loan",
    "sfr":                 "Single Family Rental",
    "rtl":                 "Residential Transition Loan",
    "small_business_loan": "Small Business Loan",
    "credit_card":         "Credit Card",
    "aircraft":            "Aircraft Receivables",
    "datacenter":          "Datacenter",
    "fiber":               "Fiber",
    "fleet_lease":         "Fleet Lease",
    "conduit":             "Conduit CMBS",
    "cre_clo":             "CRE-CLO",
    "large_loan":          "Large-Loan CMBS",
    "cmbs_other":          "Other CMBS",
}

# Top-level navigation tree — has_data controls landing card interactivity
NAV_STRUCTURE: dict[str, dict] = {
    "abs": {
        "label": "Asset Backed Securities", "short": "ABS", "color": "#7b5ea7",
        "subs": [
            {"key": "auto",           "label": "Auto",           "issuer_type": "auto",          "has_data": True},
            {"key": "credit_card",    "label": "Credit Card",    "issuer_type": "credit_card",   "has_data": True},
            {"key": "consumer_loans", "label": "Consumer Loans", "issuer_type": "consumer_loan", "has_data": True},
            {"key": "student_loans",  "label": "Student Loans",  "issuer_type": "student_loan",  "has_data": True},
            {"key": "small_business", "label": "Small Business Loans", "issuer_type": "small_business_loan", "has_data": True},
            {"key": "aircraft",       "label": "Aircraft",             "issuer_type": "aircraft",            "has_data": False},
            {"key": "datacenter",     "label": "Datacenter",           "issuer_type": "datacenter",          "has_data": False},
            {"key": "fiber",          "label": "Fiber",                "issuer_type": "fiber",               "has_data": False},
            {"key": "fleet_lease",    "label": "Fleet Lease",          "issuer_type": "fleet_lease",         "has_data": False},
            {"key": "esoteric",       "label": "Esoteric",             "issuer_type": "esoteric",            "has_data": False},
        ],
    },
    "mbs": {
        "label": "Mortgage Backed Securities", "short": "MBS", "color": "#2563eb",
        "subs": [
            {"key": "nqm",           "label": "Non-Qualified Mortgage",       "issuer_type": "nqm",           "has_data": True},
            {"key": "second_lien",   "label": "Second Lien",                  "issuer_type": "second_lien",   "has_data": True},
            {"key": "rpl",           "label": "Re-Performing Loans",          "issuer_type": "rpl",           "has_data": True},
            {"key": "prime_jumbo",   "label": "Prime Jumbo",                  "issuer_type": "prime_jumbo",   "has_data": True},
            {"key": "inv_property",  "label": "Investment Properties",        "issuer_type": "inv_property",  "has_data": True},
            {"key": "npl",           "label": "Non-Performing Loans",         "issuer_type": "npl",           "has_data": True},
            {"key": "sfr",           "label": "Single Family Rental",         "issuer_type": "sfr",           "has_data": False},
            {"key": "rtl",           "label": "Residential Transition Loans", "issuer_type": "rtl",           "has_data": False},
            {"key": "agency",        "label": "Agency MBS",                   "issuer_type": "agency",        "has_data": False},
            {"key": "crt",           "label": "Credit Risk Transfer",         "issuer_type": "crt",           "has_data": False},
            {"key": "hei",           "label": "Home Equity Investments",      "issuer_type": "hei",           "has_data": False},
        ],
    },
    "cmbs": {
        "label": "Commercial MBS", "short": "CMBS", "color": "#d97706",
        "subs": [
            {"key": "conduit",    "label": "Conduit",    "issuer_type": "conduit",    "has_data": True},
            {"key": "cre_clo",    "label": "CRE-CLO",    "issuer_type": "cre_clo",    "has_data": True},
            {"key": "large_loan", "label": "Large-Loan", "issuer_type": "large_loan", "has_data": True},
            {"key": "cmbs_other", "label": "Other",      "issuer_type": "cmbs_other", "has_data": False},
        ],
    },
    "hyc": {
        "label": "High Yield Corporate", "short": "HYC", "color": "#dc2626",
        "subs": [
            {"key": "hy_bonds",  "label": "HY Bonds",        "issuer_type": "hy_bonds",  "has_data": False},
            {"key": "lev_loans", "label": "Leveraged Loans", "issuer_type": "lev_loans", "has_data": False},
            {"key": "clo",       "label": "CLOs",             "issuer_type": "clo",       "has_data": False},
            {"key": "bdc",       "label": "BDCs",             "issuer_type": "bdc",       "has_data": False},
        ],
    },
    "igc": {
        "label": "Investment Grade Corporate", "short": "IGC", "color": "#059669",
        "subs": [
            {"key": "ig_bonds",  "label": "IG Bonds",      "issuer_type": "ig_bonds",  "has_data": False},
            {"key": "ig_loans",  "label": "IG Loans",      "issuer_type": "ig_loans",  "has_data": False},
            {"key": "covered",   "label": "Covered Bonds", "issuer_type": "covered",   "has_data": False},
        ],
    },
}

_SUBCAT_ICONS: dict[str, str] = {
    "auto": "🚗", "credit_card": "💳", "consumer_loans": "👤",
    "student_loans": "🎓", "small_business": "🏪", "aircraft": "✈️",
    "datacenter": "🖥️", "fiber": "🌐", "fleet_lease": "🚛", "esoteric": "⚡",
    "nqm": "🏡", "second_lien": "🔗", "rpl": "🔄", "prime_jumbo": "🏠", "inv_property": "🏢", "npl": "📋", "sfr": "🏘️", "rtl": "🔨",
    "agency": "🏛️", "crt": "🛡️",
    "hei": "🏠",
    "conduit": "🏢", "cre_clo": "🏗️", "large_loan": "🏨", "cmbs_other": "🌆",
    "hy_bonds": "📈",
    "lev_loans": "💰", "clo": "🔗", "bdc": "💼", "ig_bonds": "🏅",
    "ig_loans": "🤝", "covered": "🔒",
}
_SUBCAT_DESCS: dict[str, str] = {
    "auto": "Auto loan and floorplan ABS",
    "credit_card": "Credit card receivables ABS",
    "consumer_loans": "Personal and installment loan ABS",
    "student_loans": "Student loan and refinancing ABS",
    "small_business": "Small business loan and SBA ABS",
    "aircraft":       "Aircraft lease and loan receivables ABS",
    "datacenter":     "Datacenter infrastructure receivables ABS",
    "fiber":          "Fiber network receivables ABS",
    "fleet_lease":    "Commercial fleet lease ABS",
    "esoteric":       "Non-traditional collateral ABS",
    "agency": "FNMA, FHLMC, GNMA MBS pools",
    "crt": "GSE credit risk sharing transactions",
    "hei": "Home equity investment products",
    "nqm": "Non-QM and alternative mortgage",
    "second_lien": "Second lien and HELOC securitizations",
    "rpl": "Re-performing mortgage loan pools",
    "prime_jumbo": "Prime jumbo residential mortgage",
    "inv_property": "Investment property mortgage loans",
    "npl": "Non-performing mortgage loan pools",
    "sfr": "Single family rental securitizations",
    "rtl": "Fix-and-flip and bridge loan securitizations",
    "conduit":    "Multi-borrower conduit CMBS",
    "cre_clo":    "Commercial real estate CLOs",
    "large_loan": "Single-asset single-borrower and large-loan CMBS",
    "cmbs_other": "Other commercial mortgage securitizations",
    "hy_bonds": "Below investment-grade corporate bonds",
    "lev_loans": "Syndicated leveraged loans",
    "clo": "Collateralized loan obligations",
    "bdc": "Business development companies",
    "ig_bonds": "Investment-grade corporate bonds",
    "ig_loans": "Investment-grade syndicated loans",
    "covered": "Covered bonds and structured notes",
}

# Plotly dark palette for multi-issuer charts (fallback for unknown issuers)
ISSUER_PALETTE = [
    "#7b5ea7", "#4f9cf9", "#00c896", "#ff6b6b", "#ffd166",
    "#06d6a0", "#ef476f", "#118ab2", "#a8dadc", "#457b9d",
    "#e63946",
]

# Explicit per-issuer color map — guarantees Santander and Avis Budget (and others)
# always get distinct, consistent colors regardless of sort order in the chart.
ISSUER_COLOR_MAP: dict[str, str] = {
    # Auto
    "Santander Consumer USA":        "#4f9cf9",   # blue
    "Avis Budget Group":             "#ff6b6b",   # coral-red  ← distinct from Santander
    "Ford Credit":                   "#ffd166",   # amber
    "Ally Financial":                "#06d6a0",   # teal-green
    "Consumer Portfolio Services":   "#a8dadc",   # light-blue
    "Prestige Financial Services":   "#c77dff",   # violet
    "Westlake Financial":            "#f4845f",   # orange
    "Stellantis Financial":          "#e63946",   # crimson
    # Consumer loan — spread across the full color wheel for legibility
    "Affirm":                        "#ff9f43",   # orange
    "Upstart Network":               "#c77dff",   # violet
    "LendingClub Corporation":       "#26de81",   # bright green
    "SoFi Technologies":             "#fd79a8",   # pink
    "Prosper Marketplace":           "#fdcb6e",   # golden yellow
    "OneMain Financial":             "#e17055",   # terra cotta
    "Avant":                         "#00cec9",   # teal
    "Marlette Funding (Best Egg)":   "#a29bfe",   # lavender
    "Lendmark Financial Services":   "#55efc4",   # mint
    "GreenSky":                      "#74b9ff",   # sky blue
    "Oportun":                       "#fab1a0",   # peach
    "Pagaya Technologies":           "#6c5ce7",   # indigo
    "Achieve (Freedom Financial Networks)": "#ff7675",   # coral red
    "Funding Circle / Lendio":       "#00b894",   # emerald
    "Enova International":           "#ffeaa7",   # cream
    "Baker Hill (Fintechs)":         "#b2bec3",   # light gray
    "Regional Management":           "#fd9644",   # warm amber-orange
    # Credit card — spread across a distinct palette
    "Mission Lane":                  "#0ea5e9",   # sky blue
    "Mercury Financial":             "#8b5cf6",   # violet
    "Continental Finance":           "#10b981",   # emerald
    "NewDay Funding":                "#f59e0b",   # amber
    "Genesis Financial Solutions":   "#ef4444",   # red
    "Avant (Credit Card)":           "#06b6d4",   # cyan
    "Access Financial Holdings":     "#84cc16",   # lime
    "Imprint Payments":              "#f97316",   # orange
    "Fair Square Financial":         "#ec4899",   # pink
    "Prosper (Credit Card)":         "#14b8a6",   # teal
    "CW Nexus Credit Card":          "#6366f1",   # indigo
}

# ---------------------------------------------------------------------------
# Page configuration — must be first Streamlit call
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Bond Data Quality",
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
            display: flex;
            align-items: center;
            gap: 0;
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
            flex-shrink: 0;
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
            align-items: start;
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

        /* ── Top navigation bar ── */
        .top-nav-bar {
            background: #141428;
            border-bottom: 1px solid #2d2d5e;
            padding: 0;
            display: flex;
            align-items: center;
            overflow-x: auto;
        }
        .top-nav-item {
            padding: 0.9rem 1.6rem;
            font-size: 0.84rem;
            font-weight: 500;
            color: #64748b;
            border-bottom: 2px solid transparent;
            text-decoration: none !important;
            white-space: nowrap;
            transition: color 0.15s;
            display: flex;
            align-items: center;
            cursor: pointer;
        }
        .top-nav-item:hover { color: #e2e8f0; }
        .top-nav-item.nav-active {
            color: #a78bfa;
            border-bottom-color: #7b5ea7;
            font-weight: 600;
        }

        /* ── Sub navigation bar (shown inside a subcategory) ── */
        .sub-nav-bar {
            background: #0f0f24;
            border-bottom: 1px solid #1e1e3f;
            padding: 0;
            display: flex;
        }
        .sub-nav-item {
            padding: 0.55rem 1.2rem;
            font-size: 0.78rem;
            font-weight: 500;
            color: #64748b;
            border-bottom: 2px solid transparent;
            text-decoration: none !important;
            white-space: nowrap;
            transition: color 0.15s;
            display: inline-block;
            cursor: pointer;
        }
        .sub-nav-item:hover { color: #94a3b8; }
        .sub-nav-item.sub-nav-active {
            color: #a78bfa;
            border-bottom-color: #7b5ea7;
            font-weight: 600;
        }
        .sub-nav-item.sub-nav-disabled {
            opacity: 0.38;
            pointer-events: none;
            cursor: default;
        }

        /* ── Section hero (landing page header) ── */
        .section-hero {
            padding: 2.5rem 0 1.75rem;
            border-bottom: 1px solid #1e1e3f;
            margin-bottom: 2rem;
        }
        .section-hero-badge {
            display: inline-block;
            font-size: 0.68rem;
            font-weight: 700;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            padding: 3px 10px;
            border-radius: 4px;
            margin-bottom: 0.75rem;
        }
        .section-hero-title {
            font-size: 1.85rem;
            font-weight: 700;
            color: #f1f5f9;
            letter-spacing: -0.02em;
            line-height: 1.15;
            margin-bottom: 0.4rem;
        }
        .section-hero-sub {
            font-size: 0.84rem;
            color: #64748b;
        }

        /* ── Subcategory card grid ── */
        .subcat-grid {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 1rem;
            margin: 1rem 0 2.5rem;
        }
        @media (max-width: 1050px) { .subcat-grid { grid-template-columns: repeat(3, 1fr); } }
        @media (max-width: 640px)  { .subcat-grid { grid-template-columns: repeat(2, 1fr); } }
        .subcat-card {
            background: #1e1e3f;
            border: 1px solid #2d2d5e;
            border-radius: 10px;
            padding: 1.4rem 1.2rem 1.2rem;
            text-decoration: none !important;
            display: block;
            cursor: pointer;
            transition: border-color 0.15s, transform 0.12s, box-shadow 0.15s;
        }
        .subcat-card:hover {
            border-color: #7b5ea7;
            transform: translateY(-2px);
            box-shadow: 0 6px 24px #7b5ea722;
        }
        .subcat-card.no-data { opacity: 0.42; cursor: default; }
        .subcat-card.no-data:hover {
            border-color: #2d2d5e;
            transform: none;
            box-shadow: none;
        }
        .subcat-card-icon { font-size: 1.6rem; display: block; margin-bottom: 0.75rem; }
        .subcat-card-title { font-size: 0.95rem; font-weight: 700; color: #f1f5f9; margin-bottom: 0.35rem; line-height: 1.25; }
        .subcat-card-meta { font-size: 0.73rem; color: #64748b; line-height: 1.5; margin-bottom: 0.55rem; }
        .subcat-card-live { display: inline-block; font-size: 0.64rem; font-weight: 700; color: #00c896; background: #00c89614; border: 1px solid #00c89638; border-radius: 3px; padding: 2px 7px; }
        .subcat-card-soon { display: inline-block; font-size: 0.64rem; font-weight: 700; color: #64748b; background: #64748b14; border: 1px solid #64748b38; border-radius: 3px; padding: 2px 7px; }

        /* ── Breadcrumb band (CreditFlow style) ── */
        .bq-breadcrumb-band {
            background: #1a1a38;
            border-bottom: 1px solid #2d2d5e;
            width: 100%;
        }
        .nav-breadcrumb {
            font-size: 0.8rem;
            color: #e2e8f0;
            padding: 0.55rem 0;
            display: flex;
            align-items: center;
            gap: 0.6rem;
            font-weight: 500;
        }
        .nav-breadcrumb a { color: #e2e8f0; text-decoration: none !important; }
        .nav-breadcrumb a:hover { color: #a78bfa; }
        .nav-breadcrumb .sep { color: #64748b; font-size: 0.75rem; }
        .nav-breadcrumb .bc-current { color: #e2e8f0; }
        /* ── Rating badges (used in Summary Stats & Methodology) ── */
        .rating-badge { display:inline-block; padding:2px 10px; border-radius:4px;
                        font-weight:700; font-size:0.82rem; min-width:42px; text-align:center; }
        .r-aaa { background:#0d4f30; color:#34d399; }
        .r-aa  { background:#134033; color:#6ee7b7; }
        .r-a   { background:#1e3a5f; color:#60a5fa; }
        .r-bbb { background:#2d3a1e; color:#a3e635; }
        .r-bb  { background:#3d3010; color:#fbbf24; }
        .r-b   { background:#3d1e10; color:#fb923c; }
        .r-ccc { background:#4a1010; color:#f87171; }
        .r-cc  { background:#3d0a0a; color:#ef4444; }
        .r-c   { background:#2d0505; color:#dc2626; }
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
        '<div class="info-box">No data yet for this asset class. '
        "Data is refreshed automatically each day via the scheduled GitHub Actions workflow. "
        "To trigger an update immediately, go to the <b>Actions</b> tab in the GitHub repo "
        "and run <b>Daily AUP Database Update</b> manually.</div>",
        unsafe_allow_html=True,
    )


_FINDING_NOISE = {"findings", "exception", "exceptions", "exception description", "finding",
                  "findings set forth on appendix a", "findings set forth on appendix",
                  "findings set forth on appendix b", "findings based on the procedures performed",
                  "exception description number", "findings based", "no exceptions noted",
                  # SoFi-era bare label with colon
                  "findings:",
                  # RSM-era fragments (means "no exceptions in our comparison/procedures")
                  "exception in our comparison:",
                  "exceptions in our",
                  # Protiviti/narrative-style boilerplate
                  "noting no differences",
                  # PDF table column headers mistakenly extracted as findings
                  "sample loan number", "loan number", "loan id",
                  }

# SEC cover-form section headers that contain "finding" but are NOT AUP findings
_COVER_FORM_FINDING_RE = re.compile(
    r"findings?\s+and\s+conclusions?\s+of\s+(third[- ]party|a\s+third)"
    r"|due\s+diligence\s+report(s)?\s+obtained\s+by"
    r"|findings?\s+are\s+as\s+follows\s*:?\s*$"
    r"|exception\s+list\s*$",
    re.IGNORECASE,
)

# Phrases that indicate clean/no-exception outcome — not real findings.
# IMPORTANT: do NOT use broad "agreed upon threshold" here because real exception
# descriptions like "did not agree...by more than the agreed upon threshold" would
# be incorrectly filtered.  Use targeted sub-phrases instead.
_FINDING_AGREEMENT_RE = re.compile(
    r"found\s+to\s+be\s+in\s+agreement"
    r"|in\s+agreement\s+with"
    r"|no\s+exception"
    # RSM-style: "exceptions that exceeded the agreed upon threshold" (extracted from "no exceptions...")
    r"|exception[s]?\s+that\s+exceeded\s+the\s+agreed"
    # RSM-style: "...within the agreed upon threshold" (within = below = no material exception)
    r"|within\s+the\s+agreed\s+upon\s+threshold"
    # SoFi 2019-era: "differences less than the thresholds"
    r"|less\s+than\s+the\s+threshold"
    # RSM fragment: "exceptions in our procedures/comparison outlined above" (N=0)
    r"|exception[s]?\s+in\s+our\s+(procedures|comparison)"
    # Protiviti / narrative-style: entire doc has "noting no differences" → clean
    r"|noting\s+no\s+differences?"
    # "No differences were noted" (clean finding)
    r"|no\s+differences?\s+(?:were\s+)?noted",
    re.IGNORECASE,
)

def _fmt_finding(raw) -> str:
    """Return a clean one-line finding summary from a raw findings_json value."""
    import json, re as _re
    if not raw:
        return "—"
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = [raw]
    else:
        parsed = raw

    # Normalise to a flat list of strings
    if isinstance(parsed, dict):
        # e.g. {"finding": "...", "characteristics_checked": [...]}
        items = [str(v) for v in parsed.values() if isinstance(v, str)]
    elif isinstance(parsed, list):
        items = parsed
    else:
        items = [str(parsed)]

    seen: set = set()
    clean = []
    for item in items:
        s = str(item).strip()
        low = s.lower()
        if low in _FINDING_NOISE:
            continue
        if len(s) < 4:
            continue
        if _re.match(r'^[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$', s):
            continue
        # Skip bare loan/account reference IDs (no spaces, alphanumeric+hyphens, ≥5 chars)
        if ' ' not in s and _re.match(r'^[\dA-Za-z][\dA-Za-z-]{4,}$', s):
            continue
        # Skip "found to be in agreement" / "no exception" — these are clean confirmations
        if _FINDING_AGREEMENT_RE.search(s):
            continue
        # Skip SEC cover-form section headers (e.g. "Findings and Conclusions of Third-Party...")
        if _COVER_FORM_FINDING_RE.search(s):
            continue
        if s not in seen:
            seen.add(s)
            clean.append(s)

    # De-duplicate: drop bare field names that are already embedded in a longer
    # "N difference(s) in <field>" entry (Ally AUP format produces both forms).
    # Normalise whitespace (including non-breaking spaces \xa0) before comparing.
    def _norm(x: str) -> str:
        return _re.sub(r'[\s\xa0]+', ' ', x).lower().strip()

    def _is_redundant(s: str, others: list) -> bool:
        sl = _norm(s)
        return any(sl in _norm(other) and _norm(other) != sl for other in others)

    # Separate "N difference(s) in X" entries from bare field names
    has_quantified = any(
        _re.search(r'\b\d+\b|\bone\b|two\b|three\b|four\b|five\b|six\b|seven\b|eight\b|nine\b|ten\b',
                   c, _re.IGNORECASE)
        for c in clean
    )
    if has_quantified:
        clean = [c for c in clean if not _is_redundant(c, clean)]

    if not clean:
        return "—"
    return "; ".join(c.rstrip(".") for c in clean)


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


def _primary_cik(issuer: dict) -> str:
    """Return the primary CIK string for an issuer, supporting both 'cik' and 'ciks' fields."""
    ciks = issuer.get("ciks")
    return (ciks[0] if ciks else issuer.get("cik", ""))


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
body{margin:0;padding:0;}
.styled-table{width:100%;border-collapse:separate;border-spacing:0;font-size:0.8rem;margin-top:0.5rem;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}
.styled-table thead th{position:sticky;top:0;z-index:2;background:#141428!important;color:#64748b!important;font-weight:600;font-size:0.72rem;text-transform:uppercase;letter-spacing:0.05em;padding:0.55rem 0.85rem;text-align:left;box-shadow:0 1px 0 #2d2d5e;}
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
    var av = (a.cells[ci] ? (a.cells[ci].dataset.sort || a.cells[ci].innerText.trim()) : '');
    var bv = (b.cells[ci] ? (b.cells[ci].dataset.sort || b.cells[ci].innerText.trim()) : '');
    var an = parseFloat(av.replace(/[,%]/g,'')), bn = parseFloat(bv.replace(/[,%]/g,''));
    var cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv);
    return table._sort.asc ? cmp : -cmp;
  });
  rows.forEach(function(r) { tbody.appendChild(r); });
});
</script>"""

# Columns that are right-aligned (numeric)
_COL_RIGHT = {"Pool Size", "Sample", "Fields", "Findings", "Finding %",
              "Avg Exception Rate (%)", "Min (%)", "Max (%)", "# of Deals"}
# Columns that use center alignment (headers + cells)
_COL_CENTER = {"AUP Rating", "Collateral Rating", "# of Deals",
               "Avg Exception Rate (%)", "Min (%)", "Max (%)",
               "A%", "B%", "C%", "D%", "Avg A%", "Avg B%", "Avg C%", "Avg D%"}
# Columns that must not wrap
_COL_NOWRAP = {"Filing Date", "Trust Series"}


def _table_html(df: pd.DataFrame, sortable: bool = False, sort_overrides: dict = None) -> str:
    """Render a DataFrame as a dark-styled HTML table (self-contained for st.html)."""
    sort_overrides = sort_overrides or {}
    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        cells = ""
        for col, val in zip(df.columns, row):
            if col in _COL_CENTER:
                style = ' style="text-align:center!important"'
            elif col in _COL_RIGHT:
                style = ' style="text-align:right!important"'
            else:
                style = ""
            if col in sort_overrides:
                sv = sort_overrides[col][i] if i < len(sort_overrides[col]) else ""
                cells += f'<td{style} data-sort="{sv}">{val}</td>'
            else:
                cells += f"<td{style}>{val}</td>"
        rows_html += f"<tr>{cells}</tr>"

    headers = ""
    for col in df.columns:
        parts = []
        if sortable:
            parts.append('class="sortable"')
        th_styles = []
        if col in _COL_CENTER:
            th_styles.append("text-align:center!important")
        elif col in _COL_RIGHT:
            th_styles.append("text-align:right!important")
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

# ---------------------------------------------------------------------------
# Navigation state (query params — keeps URL shareable)
# ---------------------------------------------------------------------------
_qp = st.query_params
nav_main = _qp.get("nav", "abs")
nav_sub  = _qp.get("sub", "")
if nav_main not in NAV_STRUCTURE:
    nav_main = "abs"

_section  = NAV_STRUCTURE[nav_main]
_sub_info = next((s for s in _section["subs"] if s["key"] == nav_sub), None)

_cur_type = _sub_info["issuer_type"] if _sub_info else None

# DB stores asset_type as e.g. "auto_loan"; nav sections use "auto".
_DB_TYPE_TO_NAV: dict[str, str] = {"auto_loan": "auto"}

def _filing_nav_type(f: dict) -> str:
    """Return the nav section type for a filing, using per-filing asset_type first."""
    db_type = (f.get("asset_type") or "").strip()
    if db_type:
        return _DB_TYPE_TO_NAV.get(db_type, db_type)
    return ISSUERS.get(f.get("issuer_key", ""), {}).get("type", "")


def _build_page_issuers(cur_type: str | None) -> dict:
    """Return the issuers dict for the current nav section.

    Includes issuers registered with this type AND any issuer that has at
    least one filing with a per-filing asset_type override for this section
    (e.g. SoFi's 2015-C is student_loan even though SoFi's default is consumer_loan).
    """
    if not cur_type:
        return ISSUERS
    # Start with issuers whose default type matches
    result = {k: v for k, v in ISSUERS.items() if v.get("type") == cur_type}
    # Also pull in issuers that have per-filing overrides for this section
    if DB_PATH.exists():
        try:
            import sqlite3 as _sqlite3
            _conn = _sqlite3.connect(DB_PATH)
            _conn.row_factory = _sqlite3.Row
            _rows = _conn.execute(
                "SELECT DISTINCT issuer_key, asset_type FROM filings WHERE asset_type IS NOT NULL AND asset_type != ''"
            ).fetchall()
            _conn.close()
            for row in _rows:
                ik = row["issuer_key"]
                mapped = _DB_TYPE_TO_NAV.get(row["asset_type"], row["asset_type"])
                if mapped == cur_type and ik in ISSUERS and ik not in result:
                    result[ik] = ISSUERS[ik]
        except Exception:
            pass
    return result


_page_issuers = _build_page_issuers(_cur_type)

# ---------------------------------------------------------------------------
# Header + Navigation — st.button + st.query_params gives smooth same-page
# navigation without page reloads or new tabs.
# ---------------------------------------------------------------------------

def _go(nav_key=None, sub_key=None):
    """Navigate to a section/subsection without opening a new tab."""
    if nav_key:
        st.query_params["nav"] = nav_key
    if sub_key:
        st.query_params["sub"] = sub_key
    else:
        st.query_params.pop("sub", None)
    st.rerun()

# ── Build HTML top-nav with CSS hover dropdowns ──
def _nav_html(nav_main: str, nav_sub: str) -> str:
    items = []
    for nk, nsec in NAV_STRUCTURE.items():
        is_active = nk == nav_main
        active_cls = " bq-nav-active" if is_active else ""
        subs = nsec.get("subs", [])
        if subs:
            dd_items = ""
            for s in subs:
                sub_active_cls = " bq-dd-active" if (is_active and s["key"] == nav_sub) else ""
                disabled_style = "" if s.get("has_data") else "opacity:0.4;pointer-events:none;"
                dd_items += (
                    f'<a class="bq-dd-item{sub_active_cls}" '
                    f'href="?nav={nk}&sub={s["key"]}" target="_self" style="{disabled_style}">'
                    f'{s["label"]}</a>'
                )
            items.append(
                f'<div class="bq-nav-item{active_cls}">'
                f'<a class="bq-nav-link" href="?nav={nk}" target="_self">{nsec["label"]}</a>'
                f'<div class="bq-dropdown">{dd_items}</div>'
                f'</div>'
            )
        else:
            items.append(
                f'<div class="bq-nav-item{active_cls}">'
                f'<a class="bq-nav-link" href="?nav={nk}" target="_self">{nsec["label"]}</a>'
                f'</div>'
            )
    return "".join(items)

st.markdown("""<style>
/* ── HTML top-nav bar ── */
.bq-topnav {
    background: #141428;
    border-bottom: 1px solid #2d2d5e;
    display: flex;
    align-items: stretch;
    padding: 0;
    margin: 0;
    position: relative;
    z-index: 1000;
}
.bq-nav-item {
    position: relative;
    display: flex;
    align-items: stretch;
}
.bq-nav-link {
    display: flex;
    align-items: center;
    padding: .9rem 1.6rem;
    font-size: .84rem;
    font-weight: 500;
    color: #64748b;
    text-decoration: none !important;
    border-bottom: 2px solid transparent;
    white-space: nowrap;
    transition: color .15s;
    cursor: pointer;
}
.bq-nav-item:hover > .bq-nav-link,
.bq-nav-item.bq-nav-active > .bq-nav-link { color: #e2e8f0; }
.bq-nav-item.bq-nav-active > .bq-nav-link {
    color: #a78bfa;
    border-bottom-color: #7b5ea7;
    font-weight: 600;
}
/* ── Dropdown ── */
.bq-dropdown {
    display: none;
    position: absolute;
    top: 100%;
    left: 0;
    min-width: 200px;
    background: #141428;
    border: 1px solid #2d2d5e;
    border-top: none;
    box-shadow: 0 8px 24px rgba(0,0,0,.5);
    z-index: 2000;
    padding: .25rem 0;
}
.bq-nav-item:hover .bq-dropdown { display: block; }
.bq-dd-item {
    display: block;
    padding: .55rem 1.2rem;
    font-size: .8rem;
    color: #94a3b8;
    text-decoration: none !important;
    white-space: nowrap;
    transition: background .1s, color .1s;
}
.bq-dd-item:hover { background: #1e1e3f; color: #e2e8f0; }
.bq-dd-item.bq-dd-active { color: #a78bfa; font-weight: 600; }
</style>""", unsafe_allow_html=True)

# ── Header bar + HTML top nav (single markdown block keeps them flush) ──
st.markdown(f"""
<div class="finsight-header">
  <div class="page-wrapper" style="display:flex;align-items:center;gap:1.25rem;width:100%;padding:0;">
    <div class="finsight-logo"><span class="finsight-logo-badge">BDQ</span>Bond Data Quality</div>
    <div class="header-spacer"></div>
    <div class="finsight-updated-badge">Updated: {last_updated_str}</div>
  </div>
</div>
<nav class="bq-topnav">
  <div class="page-wrapper" style="display:flex;align-items:stretch;padding:0;width:100%;">
    {_nav_html(nav_main, nav_sub)}
  </div>
</nav>""", unsafe_allow_html=True)

# ── Account / daily-alert signup (top right) ──
import user_accounts as _ua

# label -> issuer_type for every sub-category that has data
_ALERT_OPTIONS: dict[str, str] = {}
for _sec_a in NAV_STRUCTURE.values():
    for _s_a in _sec_a["subs"]:
        if _s_a.get("has_data"):
            _ALERT_OPTIONS[f"{_sec_a['short']} — {_s_a['label']}"] = _s_a["issuer_type"]
_TYPE_TO_LABEL = {v: k for k, v in _ALERT_OPTIONS.items()}

st.markdown("""<style>
/* Compact top-right account popover row */
[data-testid="stHorizontalBlock"]:has(.acct-sentinel) { margin: -0.4rem 0 -0.6rem; }
[data-testid="stHorizontalBlock"]:has(.acct-sentinel) [data-testid="stPopover"] button {
    background: #1e1e3f !important; border: 1px solid #2d2d5e !important;
    color: #a78bfa !important; font-size: .78rem !important;
    padding: .25rem .8rem !important; border-radius: 6px !important;
}
</style>""", unsafe_allow_html=True)

_acct_sp, _acct_col = st.columns([8.5, 1.5])
with _acct_sp:
    st.markdown('<span class="acct-sentinel" style="display:none;"></span>', unsafe_allow_html=True)
with _acct_col:
    _user_email = st.session_state.get("user_email")
    with st.popover(f"👤 {_user_email}" if _user_email else "👤 Log In / Sign Up",
                    use_container_width=True):
        if not _user_email:
            _tab_li, _tab_su = st.tabs(["Log In", "Create Account"])
            with _tab_li:
                _li_email = st.text_input("Email", key="li_email")
                _li_pw = st.text_input("Password", type="password", key="li_pw")
                if st.button("Log In", key="li_btn", use_container_width=True):
                    if _ua.authenticate(_li_email, _li_pw):
                        st.session_state["user_email"] = _li_email.strip().lower()
                        st.rerun()
                    else:
                        st.error("Invalid email or password.")
            with _tab_su:
                _su_email = st.text_input("Email", key="su_email")
                _su_pw = st.text_input("Password (8+ characters)", type="password", key="su_pw")
                _su_subs = st.multiselect(
                    "Send me daily updates on new AUP results for:",
                    list(_ALERT_OPTIONS.keys()), key="su_subs",
                )
                if st.button("Create Account", key="su_btn", use_container_width=True):
                    _ok, _msg = _ua.create_account(
                        _su_email, _su_pw, [_ALERT_OPTIONS[l] for l in _su_subs]
                    )
                    if _ok:
                        st.session_state["user_email"] = _su_email.strip().lower()
                        st.rerun()
                    else:
                        st.error(_msg)
        else:
            st.markdown(f"Signed in as **{_user_email}**")
            _cur_subs = _ua.get_subscriptions(_user_email)
            _cur_labels = [_TYPE_TO_LABEL[t] for t in _cur_subs if t in _TYPE_TO_LABEL]
            _mg_subs = st.multiselect(
                "Daily updates on new AUP results for:",
                list(_ALERT_OPTIONS.keys()), default=_cur_labels, key="mg_subs",
            )
            if st.button("Save Preferences", key="mg_save", use_container_width=True):
                _ua.update_subscriptions(_user_email, [_ALERT_OPTIONS[l] for l in _mg_subs])
                st.success("Preferences saved.")
            if st.button("Log Out", key="mg_logout", use_container_width=True):
                del st.session_state["user_email"]
                st.rerun()

# ── Breadcrumb (shown when inside a subcategory) ──
if nav_sub and _sub_info:
    st.markdown(
        f'<div class="bq-breadcrumb-band">'
        f'<div class="nav-breadcrumb page-wrapper">'
        f'<a href="?nav={nav_main}" target="_self">{_section["label"]}</a>'
        f'<span class="sep">&gt;</span>'
        f'<span class="bc-current">{_sub_info["label"]}</span>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Content routing
# ---------------------------------------------------------------------------

if not nav_sub:
    # ── Subcategory landing page ──────────────────────────────────────────
    _c = _section["color"]
    st.markdown(f"""
    <div class="finsight-content"><div class="page-wrapper">
      <div class="section-hero">
        <div class="section-hero-badge" style="background:{_c}22;color:{_c};border:1px solid {_c}55;">{_section["short"]}</div>
        <div class="section-hero-title">{_section["label"]}</div>
        <div class="section-hero-sub">SEC ABS-15G AUP Procedure Monitor — select an asset class below</div>
      </div>
    </div></div>""", unsafe_allow_html=True)

    # CSS: make the card-row buttons look like the original .subcat-card design
    st.markdown("""<style>
[data-testid="stHorizontalBlock"]:has(.card-sentinel) {
    gap:.8rem!important; padding:0!important; margin-top:-1.2rem!important;
    align-items:stretch!important; flex-wrap:wrap!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel)
    [data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stHorizontalBlock"]:has(.card-sentinel)
    [data-testid="stVerticalBlock"] { height:100%; }
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton { height:100%; }
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button {
    background:#1e1e3f!important; border:1px solid #2d2d5e!important;
    border-radius:10px!important; padding:1.4rem 1.2rem 1.2rem!important;
    text-align:left!important; color:#f1f5f9!important;
    width:100%!important; height:200px!important; min-height:200px!important;
    box-shadow:none!important;
    transition:border-color .15s,transform .12s,box-shadow .15s!important;
    display:flex!important; flex-direction:column!important;
    align-items:flex-start!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button:hover {
    background:#1e1e3f!important; border-color:#7b5ea7!important;
    transform:translateY(-2px)!important; box-shadow:0 6px 24px #7b5ea722!important;
    color:#f1f5f9!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button[disabled] {
    background:#1e1e3f!important; border-color:#2d2d5e!important;
    color:#94a3b8!important; opacity:.42!important;
    cursor:default!important; transform:none!important; box-shadow:none!important;
}
/* Per-paragraph styling inside each card button */
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button p {
    margin:0 0 .3rem 0!important; line-height:1.4!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button p:first-child {
    font-size:1.55rem!important; margin-bottom:.7rem!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button p:nth-child(2) strong {
    font-size:.94rem!important; font-weight:700!important; color:#f1f5f9!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button p:nth-child(3) {
    font-size:.73rem!important; color:#64748b!important;
    margin-bottom:.55rem!important; flex-grow:1!important;
    overflow:hidden!important; display:-webkit-box!important; -webkit-line-clamp:2!important; -webkit-box-orient:vertical!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button p:last-child {
    font-size:.64rem!important; font-weight:700!important; color:#00c896!important;
    background:#00c89614!important; border:1px solid #00c89638!important;
    border-radius:3px!important; padding:2px 7px!important;
    display:inline-block!important; margin:0!important;
}
[data-testid="stHorizontalBlock"]:has(.card-sentinel) .stButton>button[disabled] p:last-child {
    color:#64748b!important; background:#64748b14!important;
    border-color:#64748b38!important;
}
</style>""", unsafe_allow_html=True)

    _subs_list = _section["subs"]
    for _row_start in range(0, len(_subs_list), 5):
        _row_subs = _subs_list[_row_start:_row_start + 5]
        _card_cols = st.columns(5, gap="small")
        for _ci, _s in enumerate(_row_subs):
            _icon = _SUBCAT_ICONS.get(_s["key"], "📋")
            _desc = _SUBCAT_DESCS.get(_s["key"], "")
            with _card_cols[_ci]:
                if _s.get("has_data"):
                    if st.button(
                        f"{_icon}\n\n**{_s['label']}**\n\n{_desc}\n\n● Live Data",
                        key=f"card_{_s['key']}",
                        use_container_width=True,
                    ):
                        _go(nav_main, _s["key"])
                else:
                    st.button(
                        f"{_icon}\n\n{_s['label']}\n\n{_desc}\n\nComing Soon",
                        key=f"card_{_s['key']}",
                        use_container_width=True,
                        disabled=True,
                    )
                if _ci == 0:
                    st.markdown('<span class="card-sentinel"></span><style>[data-testid~="stMarkdown"]:has(.card-sentinel){position:absolute!important;width:0!important;height:0!important;overflow:hidden!important;top:0!important;left:0!important;pointer-events:none!important;}</style>', unsafe_allow_html=True)
    st.stop()

elif _sub_info is None:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)
    st.error(f"Unknown subcategory: {nav_sub!r}")
    st.markdown('</div></div>', unsafe_allow_html=True)
    st.stop()

elif not _sub_info.get("has_data"):
    # ── Not-available page (Coming Soon or structural explanation) ─────────
    _c = _section["color"]
    if nav_sub == "credit_card":
        _hero_sub = "Asset Backed Securities — AUP Not Applicable"
        _body_html = """
          <div class="info-box">
            <b>Why are there no AUP results for Credit Card ABS?</b><br><br>
            Under SEC rules, sponsors of publicly-offered ABS are subject to two separate
            reporting obligations on Form ABS-15G:<br><br>
            <ul>
              <li><b>Rule 15Ga-1</b> — Annual report of loan repurchase demands received and
              fulfilled. Filed by all ABS securitizers including credit card issuers.</li>
              <li><b>Rule 15Ga-2</b> — Agreed-Upon Procedures (AUP) certification, filed each
              time a new deal is brought to market, accompanied by an independent auditor's
              AUP letter as Exhibit 99.1. <em>This is the report this dashboard tracks.</em></li>
            </ul>
            Major credit card ABS issuers — including <b>American Express, Capital One, Discover,
            JPMorgan Chase,</b> and <b>Synchrony</b> — operate <em>master trust</em> structures that
            continuously issue new series without triggering individual 15Ga-2 filings. Their
            ABS-15G submissions check the 15Ga-1 box only. As a result, no Exhibit 99.1 AUP
            letter is filed with the SEC for these transactions, and there is no third-party
            due diligence data to display.
          </div>
        """
    else:
        _hero_sub = f"{_section['label']} — Coverage Coming Soon"
        _body_html = f"""
          <div class="info-box">
            AUP procedure data for <b>{_sub_info["label"]}</b> is not yet available.
            Coverage will be added as SEC ABS-15G filers are onboarded for this asset class.
          </div>
        """
    st.markdown(
        f"""
        <div class="finsight-content"><div class="page-wrapper">
          <div class="section-hero">
            <div class="section-hero-badge" style="background:{_c}22;color:{_c};border:1px solid {_c}55;">{_section["short"]}</div>
            <div class="section-hero-title">{_sub_info["label"]}</div>
            <div class="section-hero-sub">{_hero_sub}</div>
          </div>
          {_body_html}
        </div></div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

else:
    # ── Live data page — tabs (breadcrumb is already in the nav component) ──
    # ---------------------------------------------------------------------------
    # Main tab layout — AUP Results is first (default selected tab)
    # Tab variables reassigned so no content blocks need changing:
    #   tab3 → "AUP Results" | tab2 → "ABS-15G Filings" | tab1 → "Edgar Filings"
    # ---------------------------------------------------------------------------
    tab3, tab2, tab1, tab6 = st.tabs([
        "AUP Results",
        "ABS-15G Filings",
        "Edgar Filings",
        "Rating Methodology",
    ])


# ===========================================================================
# TAB 1 — EDGAR FILINGS
# ===========================================================================

with tab1:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)

    # --- Summary metrics ---
    all_filings = db.get_filings(limit=10_000, db_path=DB_PATH) if DB_PATH.exists() else []
    all_filings = [f for f in all_filings if _filing_nav_type(f) == _cur_type] if _cur_type else all_filings
    total_issuers = len(_page_issuers)
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
        for key, issuer in _page_issuers.items():
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
        for key, issuer in _page_issuers.items():
            edgar_url = _issuer_edgar_url(_primary_cik(issuer))
            label, color = SECTOR_BADGES.get(issuer.get("type", ""), ("ABS", "#7b5ea7"))
            badge_cell = (
                f'<span class="sector-badge" style="background:{color}22;color:{color};'
                f'border:1px solid {color}55;">{label}</span>'
            )
            rows.append({
                "Sector": badge_cell,
                "Issuer": f'<b style="color:#f1f5f9;">{issuer["name"]}</b>',
                "CIK": f'<span class="cik-badge">{_primary_cik(issuer)}</span>',
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
# TAB 2 — ABS-15G FILINGS
# ===========================================================================

with tab2:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)
    st.markdown('<div class="finsight-section-title">ABS-15G Filings</div>', unsafe_allow_html=True)

    issuer_options = {v["name"]: k for k, v in _page_issuers.items()}

    if not issuer_options:
        st.info("No issuers found for this section.")
        st.stop()

    col_sel, col_spacer = st.columns([2, 5])
    with col_sel:
        st.markdown('<div class="filter-label">Select Issuer</div>', unsafe_allow_html=True)
        selected_name = st.selectbox(
            "Issuer", list(issuer_options.keys()), label_visibility="collapsed"
        )

    selected_key = issuer_options[selected_name]
    issuer_info = ISSUERS[selected_key]
    edgar_url = _issuer_edgar_url(_primary_cik(issuer_info))
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
            <span class="cik-badge">CIK {_primary_cik(issuer_info)}</span>
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
    if _cur_type:
        filings = [f for f in filings if _filing_nav_type(f) == _cur_type]

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
        asset_type_label = ASSET_TYPE_LABELS.get(issuer_info.get("type", ""), issuer_info.get("type", "—"))
        for f in filings:
            acc = f.get("accession_number", "") or f.get("accession_no", "")
            cik = f.get("cik", _primary_cik(issuer_info))
            filing_url = _accession_url(cik, acc) if acc else ""
            filing_rows.append({
                "Filing Date": _fmt_date(f.get("filed_date")),
                "Period of Report": _fmt_date(f.get("period_of_report")),
                "Form Type": f.get("form_type", "ABS-15G"),
                "Asset Type": asset_type_label,
                "Trust Series": f.get("deal_name") or "—",
                "ABS-15G Filing": (
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
    if _cur_type:
        aup_results = [r for r in aup_results if _filing_nav_type(r) == _cur_type]

    if not aup_results:
        st.markdown(
            '<div class="info-box">No AUP findings stored yet for this issuer.</div>',
            unsafe_allow_html=True,
        )
    else:
        _is_mbs = _cur_type in ("nqm", "second_lien", "rpl", "prime_jumbo", "inv_property", "npl", "sfr", "rtl", "mortgage")
        result_rows = []
        for r in aup_results:
            pool_raw = r.get("pool_size")
            pool_str = f"{int(float(pool_raw)):,}" if pool_raw and str(pool_raw).strip() not in ("", "—", "None", "nan") else "—"
            sample_str = str(int(float(s))) if (s := str(r.get("sample_size") or "")).strip() not in ("", "None", "nan", "0") else "—"

            if _is_mbs:
                result_rows.append({
                    "Trust Series": r.get("deal_name") or "—",
                    "Filing Date": _fmt_date(r.get("filed_date")),
                    "Reviewer": r.get("aup_provider") or "—",
                    "Pool Size": pool_str,
                    "Sample": sample_str,
                    "A%": f"{r['grade_a_pct']:.1f}" if r.get("grade_a_pct") is not None else "—",
                    "B%": f"{r['grade_b_pct']:.1f}" if r.get("grade_b_pct") is not None else "—",
                    "C%": f"{r['grade_c_pct']:.1f}" if r.get("grade_c_pct") is not None else "—",
                    "D%": f"{r['grade_d_pct']:.1f}" if r.get("grade_d_pct") is not None else "—",
                })
            else:
                exc_rate = r.get("exception_rate")
                exc_rate_str = f"{exc_rate * 100:.2f}%" if exc_rate is not None else "—"
                fields_raw = r.get("fields_count")
                result_rows.append({
                    "Trust Series": r.get("deal_name") or "—",
                    "Filing Date": _fmt_date(r.get("filed_date")),
                    "Auditor": r.get("aup_provider") or "—",
                    "Pool Size": pool_str,
                    "Sample": sample_str,
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
        all_results = [r for r in all_results if _filing_nav_type(r) == _cur_type] if _cur_type else all_results

        if not all_results:
            _no_data_banner()
        else:
            df = pd.DataFrame(all_results)
            _is_mbs_tab3 = _cur_type in ("nqm", "second_lien", "rpl", "prime_jumbo", "inv_property", "npl", "sfr", "rtl", "mortgage")
            df["filed_date"] = pd.to_datetime(df["filed_date"], errors="coerce")
            df["exception_rate_pct"] = df["exception_rate"].apply(
                lambda x: round(x * 100, 4) if pd.notna(x) else 0.0
            )
            name_map = {k: v["name"] for k, v in ISSUERS.items()}
            # Explicit overrides guarantee correct capitalisation regardless of import state
            name_map.update({
                "upstart": "Upstart Network",
                "lendmark": "Lendmark Financial Services",
                "onemain": "OneMain Financial",
                "achieve": "Achieve (Freedom Financial Networks)",
                "lendingclub": "LendingClub Corporation",
                "funding_circle": "Funding Circle",
                "marlette": "Marlette Funding (Best Egg)",
                "regional_management": "Regional Management",
                # Credit card discrete-deal issuers
                "mission_lane": "Mission Lane",
                "mercury_financial": "Mercury Financial",
                "continental_finance": "Continental Finance",
                "newday_funding": "NewDay Funding",
                "genesis_financial": "Genesis Financial Solutions",
                "avant_card": "Avant (Credit Card)",
                "access_financial": "Access Financial Holdings",
                "imprint_payments": "Imprint Payments",
                "fair_square": "Fair Square Financial",
                "prosper_card": "Prosper (Credit Card)",
                "cw_nexus": "CW Nexus Credit Card",
            })
            df["Issuer"] = df["issuer_key"].map(name_map).fillna(df["issuer_key"])

            # Normalize asset_type label
            _ASSET_LABELS = {
                "consumer_loan": "Consumer Loan",
                "auto": "Auto",
                "credit_card": "Credit Card",
                "student_loan": "Student Loan",
                "small_business_loan": "Small Business Loan",
            }
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

            # --- Summary statistics ---
            st.markdown(
                '<div class="finsight-section-title">Summary Statistics by Issuer</div>',
                unsafe_allow_html=True,
            )

            if _is_mbs_tab3 and "grade_a_pct" in df_filtered.columns and df_filtered["grade_a_pct"].notna().any():
                df_summary = (
                    df_filtered.dropna(subset=["grade_a_pct"])
                    .groupby("Issuer")
                    .agg(
                        **{"_avg_a": ("grade_a_pct", "mean"),
                           "_avg_b": ("grade_b_pct", "mean"),
                           "_avg_c": ("grade_c_pct", "mean"),
                           "_avg_d": ("grade_d_pct", "mean"),
                           "# of Deals": ("grade_a_pct", "count")}
                    )
                    .reset_index()
                    .sort_values("Issuer")
                )

                def _mbs_score(row):
                    avg_a = float(row["_avg_a"])
                    avg_c = float(row["_avg_c"])
                    avg_d = float(row["_avg_d"])
                    cnt = int(row["# of Deals"])
                    # A% score: 60 points max
                    rs = 60 if avg_a == 100 else 55 if avg_a >= 98 else 50 if avg_a >= 95 else 44 if avg_a >= 90 else 36 if avg_a >= 85 else 28 if avg_a >= 80 else 20 if avg_a >= 70 else 10 if avg_a >= 50 else 0
                    # C+D penalty: 20 points max
                    cd = avg_c + avg_d
                    cs = 20 if cd == 0 else 17 if cd < 1 else 14 if cd < 3 else 10 if cd < 5 else 6 if cd < 10 else 2
                    # Track record: 20 points max
                    ts = 20 if cnt >= 10 else 17 if cnt >= 7 else 14 if cnt >= 5 else 10 if cnt >= 3 else 7 if cnt >= 2 else 4
                    t = rs + cs + ts
                    if t >= 92 and avg_a == 100 and cnt >= 5:
                        rating, cls = "AAA", "r-aaa"
                    else:
                        t = min(t, 91)
                        if   t >= 90: rating, cls = "AA+",  "r-aa"
                        elif t >= 87: rating, cls = "AA",   "r-aa"
                        elif t >= 84: rating, cls = "AA-",  "r-aa"
                        elif t >= 82: rating, cls = "A+",   "r-a"
                        elif t >= 79: rating, cls = "A",    "r-a"
                        elif t >= 76: rating, cls = "A-",   "r-a"
                        elif t >= 74: rating, cls = "BBB+", "r-bbb"
                        elif t >= 71: rating, cls = "BBB",  "r-bbb"
                        elif t >= 68: rating, cls = "BBB-", "r-bbb"
                        elif t >= 65: rating, cls = "BB+",  "r-bb"
                        elif t >= 61: rating, cls = "BB",   "r-bb"
                        elif t >= 58: rating, cls = "BB-",  "r-bb"
                        elif t >= 55: rating, cls = "B+",   "r-b"
                        elif t >= 51: rating, cls = "B",    "r-b"
                        elif t >= 48: rating, cls = "B-",   "r-b"
                        elif t >= 45: rating, cls = "CCC+", "r-ccc"
                        elif t >= 41: rating, cls = "CCC",  "r-ccc"
                        elif t >= 38: rating, cls = "CCC-", "r-ccc"
                        elif t >= 28: rating, cls = "CC",   "r-cc"
                        else:         rating, cls = "C",    "r-c"
                    return f'<span class="rating-badge {cls}">{rating}</span>'

                df_summary["Collateral Rating"] = df_summary.apply(_mbs_score, axis=1)
                df_summary["Avg A%"] = df_summary["_avg_a"].apply(lambda x: f"{x:.1f}")
                df_summary["Avg B%"] = df_summary["_avg_b"].apply(lambda x: f"{x:.1f}")
                df_summary["Avg C%"] = df_summary["_avg_c"].apply(lambda x: f"{x:.1f}")
                df_summary["Avg D%"] = df_summary["_avg_d"].apply(lambda x: f"{x:.1f}")
                df_summary = df_summary[["Issuer", "Collateral Rating", "# of Deals", "Avg A%", "Avg B%", "Avg C%", "Avg D%"]]
            else:
                df_summary = (
                    df_filtered.dropna(subset=["exception_rate_pct"])
                    .groupby("Issuer")["exception_rate_pct"]
                    .agg(Avg="mean", Min="min", Max="max", Count="count")
                    .reset_index()
                    .rename(columns={
                        "Avg": "Avg Exception Rate (%)",
                        "Min": "Min (%)",
                        "Max": "Max (%)",
                        "Count": "# of Deals",
                    })
                    .sort_values("Issuer")
                )
            if not (_is_mbs_tab3 and "grade_a_pct" in df_filtered.columns and df_filtered["grade_a_pct"].notna().any()):
                # ── AUP Rating: computed before formatting so values are numeric ──
                def _aup_score(row):
                    avg = float(row["Avg Exception Rate (%)"])
                    mx  = float(row["Max (%)"])
                    cnt = int(row["# of Deals"])
                    rs = 60 if avg==0 else 55 if avg<0.5 else 48 if avg<1 else 40 if avg<2 else 30 if avg<4 else 20 if avg<7 else 10 if avg<10 else 0
                    sp = mx - avg
                    cs = 20 if sp<0.5 else 17 if sp<1 else 13 if sp<2 else 8 if sp<4 else 4
                    ts = 20 if cnt>=10 else 17 if cnt>=7 else 14 if cnt>=5 else 10 if cnt>=3 else 7 if cnt>=2 else 4
                    t  = rs + cs + ts
                    if t >= 92 and avg == 0 and cnt >= 10:
                        rating, cls = "AAA", "r-aaa"
                    else:
                        t = min(t, 91)
                        if   t >= 90: rating, cls = "AA+",  "r-aa"
                        elif t >= 87: rating, cls = "AA",   "r-aa"
                        elif t >= 84: rating, cls = "AA-",  "r-aa"
                        elif t >= 82: rating, cls = "A+",   "r-a"
                        elif t >= 79: rating, cls = "A",    "r-a"
                        elif t >= 76: rating, cls = "A-",   "r-a"
                        elif t >= 74: rating, cls = "BBB+", "r-bbb"
                        elif t >= 71: rating, cls = "BBB",  "r-bbb"
                        elif t >= 68: rating, cls = "BBB-", "r-bbb"
                        elif t >= 65: rating, cls = "BB+",  "r-bb"
                        elif t >= 61: rating, cls = "BB",   "r-bb"
                        elif t >= 58: rating, cls = "BB-",  "r-bb"
                        elif t >= 55: rating, cls = "B+",   "r-b"
                        elif t >= 51: rating, cls = "B",    "r-b"
                        elif t >= 48: rating, cls = "B-",   "r-b"
                        elif t >= 45: rating, cls = "CCC+", "r-ccc"
                        elif t >= 41: rating, cls = "CCC",  "r-ccc"
                        elif t >= 38: rating, cls = "CCC-", "r-ccc"
                        elif t >= 28: rating, cls = "CC",   "r-cc"
                        else:         rating, cls = "C",    "r-c"
                    return f'<span class="rating-badge {cls}">{rating}</span>'
                df_summary["AUP Rating"] = df_summary.apply(_aup_score, axis=1)
                for col in ["Avg Exception Rate (%)", "Min (%)", "Max (%)"]:
                    df_summary[col] = df_summary[col].map(lambda x: f"{x:.2f}")
                df_summary = df_summary[["Issuer", "AUP Rating", "# of Deals", "Avg Exception Rate (%)", "Min (%)", "Max (%)"]]

            st.html(_table_html(df_summary))

            st.markdown("<br>", unsafe_allow_html=True)

            # --- Trend chart ---
            issuers_with_data = sorted(df_filtered["Issuer"].dropna().unique())
            default_sel = issuers_with_data

            st.markdown('<div class="filter-label">Select Issuers for Trend</div>', unsafe_allow_html=True)
            selected_trend_issuers = st.multiselect(
                "TrendIssuers",
                options=issuers_with_data,
                default=list(default_sel),
                label_visibility="collapsed",
            )

            _df_trend_base = df_filtered[df_filtered["Issuer"].isin(selected_trend_issuers)].dropna(subset=["filed_date", "Issuer"])

            if _is_mbs_tab3 and "grade_c_pct" in df_filtered.columns and df_filtered["grade_c_pct"].notna().any():
                # ── MBS: C% trend and D% trend ──
                for _grade_col, _grade_label in [("grade_c_pct", "C% Material, Exceptions Noted"), ("grade_d_pct", "D% Material Documentation Missing")]:
                    df_trend = (
                        _df_trend_base.dropna(subset=[_grade_col])
                        .groupby(["Issuer", "filed_date"], as_index=False)[_grade_col]
                        .mean()
                        .sort_values("filed_date")
                    )
                    if df_trend.empty:
                        st.markdown('<div class="info-box">No trend data for the selected issuers.</div>', unsafe_allow_html=True)
                    else:
                        df_trend_plot = df_trend[["Issuer", "filed_date", _grade_col]].copy()
                        df_trend_plot["Issuer"] = df_trend_plot["Issuer"].astype(str)
                        df_trend_plot["filed_date"] = pd.to_datetime(df_trend_plot["filed_date"], errors="coerce")
                        df_trend_plot[_grade_col] = pd.to_numeric(df_trend_plot[_grade_col], errors="coerce").fillna(0.0)
                        df_trend_plot = df_trend_plot.dropna(subset=["filed_date"])
                        _trend_issuers = df_trend_plot["Issuer"].unique().tolist()
                        _palette_iter = iter([c for c in ISSUER_PALETTE if c not in ISSUER_COLOR_MAP.values()])
                        _trend_color_map = {iss: ISSUER_COLOR_MAP.get(iss, next(_palette_iter, "#cccccc")) for iss in _trend_issuers}
                        fig_trend = px.line(
                            df_trend_plot, x="filed_date", y=_grade_col, color="Issuer",
                            markers=True,
                            hover_data={"filed_date": False, "Issuer": False, _grade_col: ":.2f"},
                            labels={"filed_date": "Filing Date", _grade_col: _grade_label},
                            color_discrete_map=_trend_color_map,
                        )
                        fig_trend.update_layout(**_dark_plotly_layout())
                        fig_trend.update_layout(
                            title=dict(text=f"{_grade_label} Trend Over Time", font=dict(color="#f1f5f9", size=14)),
                            legend=dict(orientation="h", y=-0.18, bgcolor="#1e1e3f", bordercolor="#2d2d5e", borderwidth=1, font=dict(color="#94a3b8", size=10)),
                            height=380,
                        )
                        fig_trend.update_traces(line=dict(width=2), marker=dict(size=6))
                        st.plotly_chart(fig_trend, use_container_width=True)
                    st.markdown("<br>", unsafe_allow_html=True)
            else:
                # ── ABS: Exception Rate trend ──
                df_trend = (
                    _df_trend_base
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
                    _trend_issuers = df_trend_plot["Issuer"].unique().tolist()
                    _palette_iter = iter(
                        [c for c in ISSUER_PALETTE if c not in ISSUER_COLOR_MAP.values()]
                    )
                    _trend_color_map = {
                        iss: ISSUER_COLOR_MAP.get(iss, next(_palette_iter, "#cccccc"))
                        for iss in _trend_issuers
                    }
                    fig_trend = px.line(
                        df_trend_plot,
                        x="filed_date",
                        y="exception_rate_pct",
                        color="Issuer",
                        markers=True,
                        hover_data={"filed_date": False, "Issuer": False, "exception_rate_pct": ":.2f"},
                        labels={
                            "filed_date": "Filing Date",
                            "exception_rate_pct": "Avg Exception Rate (%)",
                        },
                        color_discrete_map=_trend_color_map,
                )
                fig_trend.update_layout(**_dark_plotly_layout())
                fig_trend.update_layout(
                    title=dict(text="Exception Rate Trend Over Time", font=dict(color="#f1f5f9", size=14)),
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

            # --- Full data table ---
            st.markdown(
                '<div class="finsight-section-title">All AUP Findings</div>',
                unsafe_allow_html=True,
            )


            df_filtered["deal_name"] = df_filtered["deal_name"].fillna("—")
            df_filtered["aup_provider"] = df_filtered["aup_provider"].fillna("—")

            if _is_mbs_tab3 and "grade_a_pct" in df_filtered.columns:
                display_cols = ["Issuer", "Asset Type", "deal_name", "filed_date", "aup_provider",
                                "pool_size", "sample_size",
                                "grade_a_pct", "grade_b_pct", "grade_c_pct", "grade_d_pct"]
                df_display = df_filtered[display_cols].copy()
                df_display.columns = [
                    "Company", "Asset Type", "Trust Series", "Filing Date", "Reviewer",
                    "Pool Size", "Sample",
                    "A%", "B%", "C%", "D%",
                ]
                filing_date_iso = df_display["Filing Date"].apply(
                    lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else ""
                ).tolist()
                df_display["Filing Date"] = df_display["Filing Date"].apply(
                    lambda x: x.strftime("%d %b %Y") if pd.notna(x) else "—"
                )
                df_display["Pool Size"] = df_display["Pool Size"].apply(
                    lambda x: f"{int(float(x)):,}" if x is not None and str(x).strip() not in ("", "—", "None", "nan") else "—"
                )
                df_display["Sample"] = df_display["Sample"].apply(
                    lambda x: (str(int(float(x))) if str(x).strip() not in ("", "None", "nan", "0") and pd.notna(x) else "—")
                )
                for gc in ["A%", "B%", "C%", "D%"]:
                    df_display[gc] = df_display[gc].apply(
                        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
                    )
            else:
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
                filing_date_iso = df_display["Filing Date"].apply(
                    lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else ""
                ).tolist()
                df_display["Filing Date"] = df_display["Filing Date"].apply(
                    lambda x: x.strftime("%d %b %Y") if pd.notna(x) else "—"
                )
                df_display["Pool Size"] = df_display["Pool Size"].apply(
                    lambda x: f"{int(float(x)):,}" if x is not None and str(x).strip() not in ("", "—", "None", "nan") else "—"
                )
                df_display["Sample"] = df_display["Sample"].apply(
                    lambda x: (str(int(float(x))) if str(x).strip() not in ("", "None", "nan", "0") and pd.notna(x) else "—")
                )
                df_display["Fields"] = df_display["Fields"].apply(
                    lambda x: str(int(x)) if pd.notna(x) and x not in (None, "") else "—"
                )
                df_display["Findings"] = df_display["Findings"].apply(
                    lambda x: str(int(x)) if pd.notna(x) and x not in (None, "") else "—"
                )
                confirmed_clean = df_display["Findings"] == "0"
                df_display.loc[confirmed_clean, "Finding Details"] = "—"
                misleading = (
                    (df_display["Finding Details"] == "No exceptions noted")
                    & ~df_display["Findings"].isin(["—", "0"])
                )
                df_display.loc[misleading, "Finding Details"] = "—"
                df_display["Finding %"] = df_display["Finding %"].apply(
                    lambda x: f"{x:.2f}%" if pd.notna(x) and x != 0.0 else ("0.00%" if x == 0.0 else "—")
                )
            # Use components.html so JavaScript click events work (st.html iframes block them)
            tbl_height = min(40 + len(df_display) * 38 + 20, 600)
            components.html(_table_html(df_display, sortable=True, sort_overrides={"Filing Date": filing_date_iso}), height=tbl_height, scrolling=True)

    st.markdown("</div></div>", unsafe_allow_html=True)


# ===========================================================================
# TAB 6 — RATING METHODOLOGY
# ===========================================================================

with tab6:
    st.markdown('<div class="finsight-content"><div class="page-wrapper">', unsafe_allow_html=True)
    st.markdown('<div class="finsight-section-title">AUP Rating Methodology</div>', unsafe_allow_html=True)
    st.markdown("""
<style>
.rating-table { width:100%; border-collapse:collapse; margin:1rem 0; }
.rating-table th { background:#1e1e3f; color:#a78bfa; padding:0.6rem 1rem; text-align:left; border-bottom:2px solid #2d2d5e; font-size:0.82rem; letter-spacing:0.05em; }
.rating-table td { padding:0.55rem 1rem; border-bottom:1px solid #2d2d5e; font-size:0.84rem; color:#cbd5e1; }
.rating-table tr:hover td { background:#1a1a38; }
.rating-badge { display:inline-block; padding:2px 10px; border-radius:4px; font-weight:700; font-size:0.85rem; }
.r-aaa  { background:#0d4f30; color:#34d399; }
.r-aa   { background:#134033; color:#6ee7b7; }
.r-a    { background:#1e3a5f; color:#60a5fa; }
.r-bbb  { background:#2d3a1e; color:#a3e635; }
.r-bb   { background:#3d3010; color:#fbbf24; }
.r-b    { background:#3d1e10; color:#fb923c; }
.r-ccc  { background:#4a1010; color:#f87171; }
.r-cc   { background:#3d0a0a; color:#ef4444; }
.r-c    { background:#2d0505; color:#dc2626; }
.method-section { background:#141428; border:1px solid #2d2d5e; border-radius:10px; padding:1.2rem 1.4rem; margin-bottom:1.2rem; }
.method-section h3 { color:#a78bfa; font-size:0.95rem; margin:0 0 0.6rem; letter-spacing:0.06em; text-transform:uppercase; }
.method-section p  { color:#94a3b8; font-size:0.85rem; line-height:1.6; margin:0; }
.score-bar { display:flex; align-items:center; gap:0.5rem; margin:0.3rem 0; }
.score-label { color:#cbd5e1; font-size:0.82rem; width:200px; }
.score-val { color:#a78bfa; font-weight:600; font-size:0.82rem; }
</style>

<div class="method-section">
<h3>Overview</h3>
<p>The AUP Rating is a proprietary, data-driven score assigned to each consumer ABS issuer based on
their historical performance across SEC ABS-15G agreed-upon-procedures (AUP) audit filings.
The methodology follows a three-factor model — Exception Rate, Consistency, and Track Record — 
weighted to reflect the most material drivers of credit quality in ABS collateral verification.</p>
</div>

<div class="method-section">
<h3>Factor 1: Exception Rate (60 points)</h3>
<p>The average exception rate across all AUP procedures is the primary rating driver. Issuers with 
zero or near-zero exception rates indicate robust internal controls and data integrity.</p>
</div>
""", unsafe_allow_html=True)

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        st.markdown("""
<table class="rating-table">
<thead><tr><th>Avg Exception Rate</th><th>Points (max 60)</th></tr></thead>
<tbody>
<tr><td>0.00% (clean record)</td><td>60</td></tr>
<tr><td>0.01% – 0.49%</td><td>55</td></tr>
<tr><td>0.50% – 0.99%</td><td>48</td></tr>
<tr><td>1.00% – 1.99%</td><td>40</td></tr>
<tr><td>2.00% – 3.99%</td><td>30</td></tr>
<tr><td>4.00% – 6.99%</td><td>20</td></tr>
<tr><td>7.00% – 9.99%</td><td>10</td></tr>
<tr><td>≥ 10.00%</td><td>0</td></tr>
</tbody></table>
""", unsafe_allow_html=True)

    with col_r2:
        st.markdown("""
<div class="method-section">
<h3>Factor 2: Consistency (20 points)</h3>
<p>Measures how stable exception rates are across deals. 
A small spread between average and maximum exception rate indicates 
consistent underwriting and servicing standards.</p>
</div>
<table class="rating-table">
<thead><tr><th>Max – Avg Spread</th><th>Points (max 20)</th></tr></thead>
<tbody>
<tr><td>&lt; 0.50%</td><td>20</td></tr>
<tr><td>0.50% – 0.99%</td><td>17</td></tr>
<tr><td>1.00% – 1.99%</td><td>13</td></tr>
<tr><td>2.00% – 3.99%</td><td>8</td></tr>
<tr><td>≥ 4.00%</td><td>4</td></tr>
</tbody></table>
""", unsafe_allow_html=True)

    st.markdown("""
<div class="method-section">
<h3>Factor 3: Track Record (20 points)</h3>
<p>Issuers with more completed AUP audits demonstrate a longer, verifiable history of 
compliance performance. A larger deal count provides greater statistical confidence in the rating.</p>
</div>
<table class="rating-table">
<thead><tr><th>Number of Audited Deals</th><th>Points (max 20)</th></tr></thead>
<tbody>
<tr><td>≥ 10 deals</td><td>20</td></tr>
<tr><td>7 – 9 deals</td><td>17</td></tr>
<tr><td>5 – 6 deals</td><td>14</td></tr>
<tr><td>3 – 4 deals</td><td>10</td></tr>
<tr><td>2 deals</td><td>7</td></tr>
<tr><td>1 deal</td><td>4</td></tr>
</tbody></table>

<div class="method-section" style="margin-top:1.2rem;">
<h3>Rating Scale</h3>
<p>Total score (0–100) maps to S&amp;P-equivalent rating symbols:</p>
</div>
<table class="rating-table">
<thead><tr><th>Score Range</th><th>AUP Rating</th><th>Interpretation</th></tr></thead>
<tbody>
<tr><td>92 – 100 + perfect record + ≥10 deals</td><td><span class="rating-badge r-aaa">AAA</span></td><td>Exceptional — zero exceptions, consistent, extensive track record</td></tr>
<tr><td>90 – 91</td><td><span class="rating-badge r-aa">AA+</span></td><td rowspan="3">Very Strong — minimal exceptions, highly consistent performance</td></tr>
<tr><td>87 – 89</td><td><span class="rating-badge r-aa">AA</span></td></tr>
<tr><td>84 – 86</td><td><span class="rating-badge r-aa">AA-</span></td></tr>
<tr><td>82 – 83</td><td><span class="rating-badge r-a">A+</span></td><td rowspan="3">Strong — low exception rate with solid consistency</td></tr>
<tr><td>79 – 81</td><td><span class="rating-badge r-a">A</span></td></tr>
<tr><td>76 – 78</td><td><span class="rating-badge r-a">A-</span></td></tr>
<tr><td>74 – 75</td><td><span class="rating-badge r-bbb">BBB+</span></td><td rowspan="3">Adequate — moderate exceptions, acceptable consistency</td></tr>
<tr><td>71 – 73</td><td><span class="rating-badge r-bbb">BBB</span></td></tr>
<tr><td>68 – 70</td><td><span class="rating-badge r-bbb">BBB-</span></td></tr>
<tr><td>65 – 67</td><td><span class="rating-badge r-bb">BB+</span></td><td rowspan="3">Speculative — elevated exceptions or high variability</td></tr>
<tr><td>61 – 64</td><td><span class="rating-badge r-bb">BB</span></td></tr>
<tr><td>58 – 60</td><td><span class="rating-badge r-bb">BB-</span></td></tr>
<tr><td>55 – 57</td><td><span class="rating-badge r-b">B+</span></td><td rowspan="3">Vulnerable — frequent exceptions, limited track record</td></tr>
<tr><td>51 – 54</td><td><span class="rating-badge r-b">B</span></td></tr>
<tr><td>48 – 50</td><td><span class="rating-badge r-b">B-</span></td></tr>
<tr><td>45 – 47</td><td><span class="rating-badge r-ccc">CCC+</span></td><td rowspan="3">Highly Vulnerable — persistent high exception rates</td></tr>
<tr><td>41 – 44</td><td><span class="rating-badge r-ccc">CCC</span></td></tr>
<tr><td>38 – 40</td><td><span class="rating-badge r-ccc">CCC-</span></td></tr>
<tr><td>28 – 37</td><td><span class="rating-badge r-cc">CC</span></td><td>Extremely Vulnerable — very high exceptions across all deals</td></tr>
<tr><td>0 – 27</td><td><span class="rating-badge r-c">C</span></td><td>Critical — systemic exception rates above 10%</td></tr>
</tbody></table>

<div class="method-section" style="margin-top:1.2rem;">
<h3>Important Disclosures</h3>
<p>AUP Ratings are based solely on data extracted from SEC ABS-15G filings and reflect AUP audit 
outcomes only. They are not credit ratings and should not be used as a substitute for full credit 
analysis. Ratings are updated automatically as new ABS-15G filings are processed. Issuers with fewer 
deals should be interpreted with appropriate caution given limited sample size.</p>
</div>
""", unsafe_allow_html=True)

    st.markdown("</div></div>", unsafe_allow_html=True)

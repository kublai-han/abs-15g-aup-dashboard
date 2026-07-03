"""
issuers.py

Registry of known ABS-15G filers on SEC EDGAR.
CIK numbers sourced from public SEC EDGAR records.
"""

from typing import Optional

ISSUERS = {
    "pagaya": {
        "name": "Pagaya Technologies",
        "cik": "0001897077",  # Pagaya Structured Products LLC (ABS-15G filer; was 0001883944 = Kowalewski Eric)
        "type": "consumer_loan",
        "active": True,
    },
    "affirm": {
        "name": "Affirm, Inc.",
        "cik": "0001714052",  # Affirm, Inc. (operating ABS-15G filer, 36 filings); parent Affirm Holdings = 0001820953
        "type": "consumer_loan",  # BNPL / installment loans
        "active": True,
    },
    "oportun": {
        "name": "Oportun",
        "cik": "0001478295",   # Oportun, Inc. (annual depositor entity)
        "ciks": ["0001478295", "0001857141"],  # also Oportun Funding XVI LLC (quarterly trust)
        "type": "consumer_loan",
        "active": True,
    },
    "sofi": {
        "name": "SoFi Lending Corp.",
        "cik": "0001555110",  # SoFi Lending Corp. (ABS-15G filer, 106 filings); parent SoFi Technologies = 0001818874
        "type": "consumer_loan",  # personal loans, student refi
        "active": True,
    },
    "lendingclub": {
        "name": "LendingClub Corporation",
        "cik": "0001409970",
        "type": "consumer_loan",
        "active": True,
    },
    "prosper": {
        "name": "Prosper Marketplace",
        "cik": "0001705499",  # Prosper Depositor LLC (was 0001416635 = Trendiki Inc)
        "type": "consumer_loan",
        "active": True,
    },
    "enova": {
        "name": "Enova International",
        "cik": "0001529864",
        "type": "consumer_loan",  # near-prime / online lending
        "active": True,
    },
    "avant": {
        "name": "Avant",
        "cik": "0001624523",  # Avant Credit III LLC, primary ABS-15G depositor (was 0001620459 = James River Group Holdings)
        "type": "consumer_loan",
        "active": True,
    },
    "marlette": {
        "name": "Marlette Funding (Best Egg)",
        "cik": "0001678811",  # Marlette Funding Depositor Trust (was 0001649989 = Outlook Therapeutics Inc.)
        "type": "consumer_loan",
        "active": True,
    },
    "greensky": {
        "name": "GreenSky",
        "cik": "0001472649",  # GreenSky, LLC (ABS-15G filer; was 0001712518 = Flight Of The Guineas LLC)
        "type": "consumer_loan",  # home improvement / point-of-sale
        "active": True,
    },
    "funding_circle": {
        "name": "Funding Circle",
        "cik": "0001783754",  # FC Marketplace, LLC (was 0001780530 = Bridgeway Wellness Group LLC)
        "type": "small_business_loan",
        "active": True,
    },
    "achieve": {
        "name": "Achieve (Freedom Financial Networks)",
        "cik": "0001742848",  # FREED ABS Master Depositor Trust; rebranded shelf to ACHV ABS Trust ~2022
        "ciks": ["0001742848", "0002033841"],  # also ACHV ABS Master Depositor Trust (newer shelf, 2024+)
        "type": "consumer_loan",
        "active": True,
    },
    "upstart": {
        "name": "Upstart Network",
        "cik": "0001721221",  # Upstart Funding II, LLC (ABS-15G depositor, 23 filings)
        "type": "consumer_loan",
        "active": True,
    },
    "lendmark": {
        "name": "Lendmark Financial Services",
        "cik": "0001802626",  # Lendmark Financial Funding 2020-1, LLC (master filer for group)
        "ciks": ["0001802626", "0001927039", "0001966142", "0001994514"],  # 2020-1, 2022-1, 2023-1, 2024-1
        "type": "consumer_loan",
        "active": True,
    },
    "onemain": {
        "name": "OneMain Financial",
        "cik": "0001728647",  # Springleaf Funding II, LLC (ABS-15G depositor; OneMain's securitization vehicle)
        "type": "consumer_loan",
        "active": True,
    },
    "regional_management": {
        "name": "Regional Management",
        "cik": "0001742262",  # Regional Management Receivables III, LLC (depositor entity, covers all trust series)
        # Individual trust CIKs (one per deal, but depositor CIK captures all):
        # 0001759722=2018-2, 0001790696=2019-1, 0001823105=2020-1, 0001842014=2021-1,
        # 0001870673=2021-2, 0001907053=2022-1, 0001947611=2022-2B,
        # 0002024556=2024-1, 0002043438=2024-2, 0002058355=2025-1, 0002089117=2025-2
        "type": "consumer_loan",
        "active": True,
    },
}



# ---------------------------------------------------------------------------
# Credit Card ABS issuers — discrete-deal structures that file 15Ga-2 AUP reports
#
# NOTE: Major bank "master trust" issuers (Capital One, JPMorgan Chase, American
# Express, Synchrony, Discover) operate revolving master trusts that issue new
# series without triggering individual 15Ga-2 filings — they file only item 1.02
# (annual repurchase demand reports) and have NO Exhibit 99.1 AUP letter.
# Only the smaller issuers below use discrete deal structures subject to 15Ga-2.
# ---------------------------------------------------------------------------
CREDIT_CARD_ISSUERS = {
    "mission_lane": {
        "name": "Mission Lane",
        "cik": "0001844791",   # Mission Lane Transferor LLC — 51 ABS-15G filings (most active CC AUP filer)
        "type": "credit_card",
        "active": True,
    },
    "mercury_financial": {
        "name": "Mercury Financial",
        "cik": "0001848201",   # Mercury Financial Transferor LLC — 37 ABS-15G filings
        "type": "credit_card",
        "active": True,
    },
    "continental_finance": {
        "name": "Continental Finance",
        "cik": "0001833111",   # Continental Finance Credit Card ABS, LLC — 23 filings (newer shelf)
        "ciks": ["0001833111", "0001688593"],  # also Continental Finance Company, LLC (older shelf, ~11 filings)
        "type": "credit_card",
        "active": True,
    },
    "newday_funding": {
        "name": "NewDay Funding",
        "cik": "0001743948",   # NewDay Funding Transferor Ltd — 39 ABS-15G filings (UK subprime CC issuer)
        "ciks": ["0001743948", "0001805748"],  # also NewDay Partnership Transferor PLC (~7 filings)
        "type": "credit_card",
        "active": True,
    },
    "genesis_financial": {
        "name": "Genesis Financial Solutions",
        "cik": "0001759947",   # Genesis Sales Finance Transferor LLC — 10 ABS-15G filings
        "type": "credit_card",
        "active": True,
    },
    "avant_card": {
        "name": "Avant (Credit Card)",
        "cik": "0001881673",   # Avant Credit Card Master Trust — 6 filings
        "ciks": ["0001881673", "0002029130"],  # also Avant Depositor II LLC (~7 filings)
        "type": "credit_card",
        "active": True,
    },
    "access_financial": {
        "name": "Access Financial Holdings",
        "cik": "0001831952",   # Access Financial Holdings, LLC — 6 ABS-15G filings
        "type": "credit_card",
        "active": True,
    },
    "imprint_payments": {
        "name": "Imprint Payments",
        "cik": "0001839975",   # Imprint Payments, Inc. — 4 ABS-15G filings
        "type": "credit_card",
        "active": True,
    },
    "fair_square": {
        "name": "Fair Square Financial",
        "cik": "0001800298",   # Fair Square Financial Transferor LLC — 3 ABS-15G filings
        "type": "credit_card",
        "active": True,
    },
    "prosper_card": {
        "name": "Prosper (Credit Card)",
        "cik": "0002041063",   # Prosper Credit Card 2024-1 Issuer LLC — 4 ABS-15G filings
        "type": "credit_card",
        "active": True,
    },
    "cw_nexus": {
        "name": "CW Nexus Credit Card",
        "cik": "0001827225",   # CW Nexus Credit Card Holdings I, LLC — 4 ABS-15G filings
        "type": "credit_card",
        "active": True,
    },
}

ISSUERS.update(CREDIT_CARD_ISSUERS)

# ---------------------------------------------------------------------------
# Auto ABS issuers — verified ABS-15G filers with 2024+ activity
# ---------------------------------------------------------------------------
AUTO_ISSUERS = {
    "ford_credit": {
        "name": "Ford Credit",
        "cik": "0001129987",   # Ford Credit Auto Receivables Two LLC — 33 filings, latest 2026-03-09
        "type": "auto",
        "active": True,
    },
    "ally_auto": {
        "name": "Ally Financial",
        "cik": "0001477336",   # Ally Auto Assets LLC — 15 filings, latest 2026-02-23
        "type": "auto",
        "active": True,
    },
    "santander_drive": {
        "name": "Santander Consumer USA",
        "cik": "0001383094",   # Santander Drive Auto Receivables LLC — 7 filings, latest 2026-02-10
        "type": "auto",
        "active": True,
    },
    "westlake": {
        "name": "Westlake Financial",
        "cik": "0001675921",   # WPS IV, LLC (Westlake) — 35 filings, latest 2026-04-29
        "type": "auto",
        "active": True,
    },
    "avis_budget": {
        "name": "Avis Budget Group",
        "cik": "0001664774",   # Avis Budget Rental Car Funding (AESOP) LLC — 24 filings, latest 2026-05-14
        "type": "auto",
        "active": True,
    },
    "consumer_portfolio": {
        "name": "Consumer Portfolio Services",
        "cik": "0001518859",   # CPS Receivables Five LLC — 43 filings, latest 2026-04-09
        "type": "auto",
        "active": True,
    },
    "prestige_financial": {
        "name": "Prestige Financial Services",
        "cik": "0001541629",   # Prestige Financial Services — 28 filings, latest 2026-02-11
        "type": "auto",
        "active": True,
    },
    "stellantis": {
        "name": "Stellantis Financial",
        "cik": "0001980666",   # Stellantis Financial Services, Inc. — 11 filings, latest 2026-03-20
        "type": "auto",
        "active": True,
    },
}

ISSUERS.update(AUTO_ISSUERS)

# ---------------------------------------------------------------------------
# Student loan ABS issuers — verified ABS-15G filers
# ---------------------------------------------------------------------------
STUDENT_LOAN_ISSUERS = {
    "nelnet": {
        "name": "Nelnet",
        # Two depositor entities (private loan trusts — letter suffixes 2021-A, 2025-A, etc.):
        #   0001258602  NELNET INC — sponsor, annual 15Ga-1 filer + 2021/2023 deal AUPs
        #   0002057405  Santiago Student Loan Depositor Trust — NSLT 2025+ deal AUPs
        # NOTE: Nelnet also has FFELP-backed trusts with numeric suffixes (2004-3 through 2025-1)
        # filed under Nelnet Inc. These do NOT have individual 15Ga-2 AUP filings on EDGAR;
        # only the annual 15Ga-1 "no activity" certification covers them.
        # See: https://abs.nelnetinvestors.com/debt-securities/nelnet-student-loan-trust/
        "ciks": ["0001258602", "0002057405"],
        "type": "student_loan",
        "active": True,
    },
    "ecmc": {
        "name": "ECMC Group",
        # Two depositor entities, both display under a single issuer key:
        #   0002030614  Viking Student Loan Capital, LLC — current depositor, 2024-present
        #   0001682009  Patriot Student Loan Capital, LLC — prior depositor, 2016-2021
        "ciks": ["0002030614", "0001682009"],
        "type": "student_loan",
        "active": True,
    },
    "navient": {
        "name": "Navient",
        # Two depositor entities:
        #   0001179550  Navient Credit Funding, LLC — primary depositor (NavSL / NAVRL trusts)
        #   0002071239  Navient Education Loan Funding, LLC — newer depositor (NAVEL trusts, 2025+)
        "ciks": ["0001179550", "0002071239"],
        "type": "student_loan",
        "active": True,
    },
    "goal": {
        "name": "Goal Structured Solutions",
        # Multiple depositor SPVs all display under this single issuer key:
        #   0001666411  Goal Structured Solutions, LLC — sponsor/securitizer, annual 15Ga-1 filer
        #   0001692054  GS2 Depositor 2016-A SPV, LLC — Trust 2016-A depositor
        #   0001691661  GS2 DEPOSITOR 2016-B SPV, LLC — Trust 2016-B depositor
        #   0001708959  GS2 MASTER DEPOSITOR-I SPV, LLC — Master Trust-I depositor (2017-A, 2019-A)
        "ciks": ["0001666411", "0001692054", "0001691661", "0001708959"],
        "type": "student_loan",
        "active": True,
    },
}

ISSUERS.update(STUDENT_LOAN_ISSUERS)

# ---------------------------------------------------------------------------
# Small Business Loan ABS issuers — verified ABS-15G filers
#
# NOTE: SBA program deals (SBA 504/CDC "SBAP", SBIC debentures) are issued by
# the Small Business Administration, a government agency exempt from Reg AB II
# — no ABS-15G filings exist for those on EDGAR.
# ---------------------------------------------------------------------------
SBL_ISSUERS = {
    "first_citizens": {
        "name": "First Citizens BancShares",
        "cik": "0002128731",   # First Citizens Securitization Depositor LLC — FCLT 2026-SBA1
        "type": "small_business_loan",
        "active": True,
    },
    "readycap": {
        "name": "ReadyCap Lending (Waterfall)",
        "cik": "0001795936",   # ReadyCap Lending SBL Depositor, LLC — RCLT trusts, 9 filings
        "ciks": ["0001795936", "0002137901"],  # also ReadyCap Lending, LLC (2026-4, newer filer)
        "type": "small_business_loan",
        "active": True,
    },
    "fora_financial": {
        "name": "Fora Financial",
        # Three sibling co-originators (West 0001786188, Business Loans 0001786189,
        # Advance 0001786192) each file duplicate 15Gs for the same deal on the
        # same dates — register only Advance (longest history) to avoid dup rows.
        "cik": "0001786192",   # Fora Financial Advance LLC — 11 filings
        "type": "small_business_loan",
        "active": True,
    },
    "byzfunder": {
        "name": "ByzFunder",
        "cik": "0002132051",   # ByzFunder NY LLC — BYZF 2026-1
        "type": "small_business_loan",
        "active": True,
    },
    "mulligan": {
        "name": "Mulligan Funding",
        "cik": "0001482399",   # Mulligan Funding, LLC — MLLGN trusts, 7 filings
        "type": "small_business_loan",
        "active": True,
    },
    "kapitus": {
        "name": "Kapitus (Strategic Funding Source)",
        "cik": "0001544352",   # Strategic Funding Source, Inc. dba Kapitus — 19 filings
        "type": "small_business_loan",
        "active": True,
    },
    "velocity_sba": {
        "name": "VelocitySBA (Cranemere)",
        "cik": "0002094123",   # VelocitySBA Funding, LLC — VLSBA 2026-1
        "type": "small_business_loan",
        "active": True,
    },
    "kalamata": {
        "name": "Kalamata Capital Group",
        "cik": "0002009263",   # Kalamata.com, LLC — KCG Securitization trusts, 6 filings
        "type": "small_business_loan",
        "active": True,
    },
}

ISSUERS.update(SBL_ISSUERS)

# ---------------------------------------------------------------------------
# Mortgage Backed Securities — Non-Qualified Mortgage (NQM)
# ---------------------------------------------------------------------------
NQM_ISSUERS = {
    "angel_oak": {
        "name": "Angel Oak Mortgage Trust",
        "cik": "0001697970",   # Angel Oak Mortgage Trust I, LLC — 72 filings
        "type": "nqm",
        "active": True,
    },
    "mfa_nqm": {
        "name": "MFA Financial (Verus)",
        "cik": "0001803775",   # MFRA NQM Depositor, LLC — 45 filings
        "type": "nqm",
        "active": True,
    },
    "ellington_nqm": {
        "name": "Ellington Financial Mortgage",
        "cik": "0001946154",   # EFMT Depositor LLC — 45 filings
        "type": "nqm",
        "active": True,
    },
    "acra": {
        "name": "Acra Lending (ACRA Trust)",
        "cik": "0001812383",   # Citadel Depositor, LLC — 9 filings, Consolidated Analytics
        "type": "nqm",
        "active": True,
    },
    "park_capital": {
        "name": "Park Capital Management (PRKCM)",
        "cik": "0001877944",   # Park Capital Management Depositor LLC — 24 filings, Clayton
        "type": "nqm",
        "active": True,
    },
    "jpmorgan_mortgage": {
        "name": "JP Morgan Mortgage (JPMMT)",
        "cik": "0001142786",   # J.P. Morgan Acceptance Corp II — 246 filings, AMC
        "type": "nqm",
        "active": True,
    },
    "colt": {
        "name": "COLT Mortgage (Lone Star)",
        "ciks": ["0001935056", "0001788771"],  # COLT Depositor III + II — 53 filings, AMC
        "type": "nqm",
        "active": True,
    },
    "santander_mortgage": {
        "name": "Santander Mortgage (SAN)",
        "cik": "0002058366",   # Santander Bank, N.A. — 18 filings, AMC
        "type": "nqm",
        "active": True,
    },
}

ISSUERS.update(NQM_ISSUERS)

# ---------------------------------------------------------------------------
# Mortgage Backed Securities — Second Lien
# ---------------------------------------------------------------------------
SECOND_LIEN_ISSUERS = {
    "figure_heloc": {
        "name": "Figure Lending (FIGRE Trust)",
        "cik": "0001970036",   # Figure HELOC Master Depositor Trust — 43 filings
        "type": "second_lien",
        "active": True,
    },
    "achieve_he": {
        "name": "Achieve Home Equity (ACHM Trust)",
        "ciks": ["0001889966", "0002020165"],
        "type": "second_lien",
        "active": True,
    },
    "woodward_rckt": {
        "name": "Woodward Capital (RCKT Trust)",
        "ciks": ["0001787426", "0002076571"],
        "type": "second_lien",
        "active": True,
    },
    "gs_mortgage": {
        "name": "Goldman Sachs (GSMBS)",
        "cik": "0000807641",   # GS MORTGAGE SECURITIES CORP — 220 filings, AMC reviews
        "type": "second_lien",
        "active": True,
    },
    "onslow_bay": {
        "name": "Onslow Bay / Annaly (OBX Trust)",
        "cik": "0001658638",   # Onslow Bay Funding LLC — 138 filings, AMC reviews
        "type": "second_lien",
        "active": True,
    },
    "citi_mortgage": {
        "name": "Citigroup Mortgage (CMLTI)",
        "cik": "0001257102",   # CITIGROUP MORTGAGE LOAN TRUST INC — 102 filings, AMC reviews
        "type": "second_lien",
        "active": True,
    },
    "vista_point": {
        "name": "Vista Point Mortgage (VSTA)",
        "cik": "0001802711",   # Vista Point Assets LLC — 22 filings, Clarifii reviews
        "type": "second_lien",
        "active": True,
    },
}

ISSUERS.update(SECOND_LIEN_ISSUERS)

# ---------------------------------------------------------------------------
# Mortgage Backed Securities — Non-Performing Loans (NPL)
# ---------------------------------------------------------------------------
NPL_ISSUERS = {
    "velocity_npl": {
        "name": "Velocity (VCC Mortgage)",
        "cik": "0001542220",   # VCC Mortgage Securities, LLC — 57 filings, AMC Diligence AUPs
        "type": "npl",
        "active": True,
    },
}

ISSUERS.update(NPL_ISSUERS)

# ---------------------------------------------------------------------------
# Mortgage Backed Securities — Single Family Rental (SFR)
# ---------------------------------------------------------------------------
SFR_ISSUERS = {
}

ISSUERS.update(SFR_ISSUERS)

# ---------------------------------------------------------------------------
# Mortgage Backed Securities — Residential Transition Loans (RTL)
# ---------------------------------------------------------------------------
RTL_ISSUERS = {
    "saluda_grade": {
        "name": "Saluda Grade Mortgage",
        "cik": "0001831646",   # Saluda Grade Mortgage Funding LLC — 30 filings
        "type": "rtl",
        "active": True,
    },
}

ISSUERS.update(RTL_ISSUERS)


def get_all_ciks() -> dict[str, str]:
    """
    Return a mapping of issuer_key -> CIK for all registered issuers.

    Returns
    -------
    dict[str, str]
        Keys are issuer identifiers (e.g. "pagaya"), values are zero-padded
        10-digit CIK strings as they appear on SEC EDGAR.
    """
    result = {}
    for key, issuer in ISSUERS.items():
        ciks = issuer.get("ciks") or [issuer.get("cik", "")]
        for cik in ciks:
            if cik:
                result[f"{key}|{cik}"] = cik
    return result


def get_issuer_by_cik(cik: str) -> Optional[dict]:
    """
    Look up an issuer record by its CIK number.

    The lookup normalises the supplied CIK by zero-padding it to 10 digits
    so that both "1883944" and "0001883944" resolve correctly.

    Parameters
    ----------
    cik : str
        CIK number, with or without leading zeros.

    Returns
    -------
    dict or None
        The issuer dict (including its registry key under ``"key"``) if found,
        otherwise ``None``.
    """
    normalised = cik.lstrip("0").zfill(10)
    for key, issuer in ISSUERS.items():
        ciks = issuer.get("ciks") or [issuer.get("cik", "")]
        if any(c.lstrip("0").zfill(10) == normalised for c in ciks if c):
            return {**issuer, "key": key}
    return None


def get_active_issuers() -> dict[str, dict]:
    """Return only issuers marked as active."""
    return {key: issuer for key, issuer in ISSUERS.items() if issuer.get("active")}


if __name__ == "__main__":
    print("Registered issuers:")
    for key, issuer in ISSUERS.items():
        ciks = issuer.get("ciks") or [issuer.get("cik", "?")]
        print(f"  {key:30s}  CIKs={ciks}  ({issuer['name']})")

    print("\nAll CIKs:")
    for key, cik in get_all_ciks().items():
        print(f"  {key}: {cik}")

    sample_cik = "0001883944"
    result = get_issuer_by_cik(sample_cik)
    print(f"\nLookup CIK {sample_cik}: {result}")

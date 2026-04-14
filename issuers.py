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
        "name": "Funding Circle / Lendio",
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
}


def get_all_ciks() -> dict[str, str]:
    """
    Return a mapping of issuer_key -> CIK for all registered issuers.

    Returns
    -------
    dict[str, str]
        Keys are issuer identifiers (e.g. "pagaya"), values are zero-padded
        10-digit CIK strings as they appear on SEC EDGAR.
    """
    return {key: issuer["cik"] for key, issuer in ISSUERS.items()}


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
        if issuer["cik"].lstrip("0").zfill(10) == normalised:
            return {**issuer, "key": key}
    return None


def get_active_issuers() -> dict[str, dict]:
    """Return only issuers marked as active."""
    return {key: issuer for key, issuer in ISSUERS.items() if issuer.get("active")}


if __name__ == "__main__":
    print("Registered issuers:")
    for key, issuer in ISSUERS.items():
        print(f"  {key:20s}  CIK={issuer['cik']}  ({issuer['name']})")

    print("\nAll CIKs:")
    for key, cik in get_all_ciks().items():
        print(f"  {key}: {cik}")

    sample_cik = "0001883944"
    result = get_issuer_by_cik(sample_cik)
    print(f"\nLookup CIK {sample_cik}: {result}")

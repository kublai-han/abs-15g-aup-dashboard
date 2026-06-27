"""
mbs_parser.py

Parses MBS third-party due diligence exhibits (Clayton, Visionet,
Consolidated Analytics, AMC/Clarifii) to extract A/B/C/D grade
distributions from the "Overall Grade Migration" or "Overall Loan
Results" tables.
"""
import re
from typing import Optional


def parse_grade_distribution(text: str) -> Optional[dict]:
    """
    Extract overall A/B/C/D grade percentages from an MBS TPR exhibit.

    Returns dict with keys: grade_a, grade_b, grade_c, grade_d (floats 0-100),
    sample (int), reviewer (str), deal_name (str or None).
    Returns None if no grade table found.
    """
    result = _try_visionet(text)
    if result:
        return result
    result = _try_clayton(text)
    if result:
        return result
    result = _try_amc_clarifii(text)
    if result:
        return result
    return None


def _try_visionet(text: str) -> Optional[dict]:
    """Visionet / Consolidated Analytics format:
    Event Grade A <count> $ <amount> <pct> %
    Event Grade B ...
    """
    pos = text.lower().find("overall loan results")
    if pos < 0:
        pos = text.lower().find("overall results summary")
    if pos < 0:
        return None

    section = text[pos:pos+600]

    grades = re.findall(
        r"Event Grade ([ABCD])\s+(\d+)\s+\$\s*[\d,]+\.?\d*\s+([\d.]+)\s*%",
        section,
    )
    if len(grades) < 4:
        return None

    # Take only the first A/B/C/D set (Overall, not Valuation/Compliance subtables)
    pcts = {}
    sample = 0
    for g, count, pct in grades[:4]:
        if g not in pcts:
            pcts[g] = float(pct)
            sample += int(count)

    if len(pcts) < 4:
        return None

    total_m = re.search(r"Total Sample\s+(\d+)", section)
    if total_m:
        sample = int(total_m.group(1))

    reviewer = _detect_reviewer(text)
    deal = _extract_deal(text)

    return {
        "grade_a": pcts.get("A", 0),
        "grade_b": pcts.get("B", 0),
        "grade_c": pcts.get("C", 0),
        "grade_d": pcts.get("D", 0),
        "sample": sample,
        "reviewer": reviewer,
        "deal_name": deal,
    }


def _try_clayton(text: str) -> Optional[dict]:
    """Clayton format:
    Overall Grade Migration
    Initial Final A B C D Total
    A  719  1  5  3  728
    ...
    Total  719  7  21  4  751
    """
    pos = text.lower().find("overall grade migration")
    if pos < 0:
        return None

    section = text[pos:pos+500]

    # Find the Total row: "Total <A_count> <B_count> <C_count> <D_count> <total>"
    totals = re.search(
        r"Total\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)",
        section,
    )
    if not totals:
        return None

    a, b, c, d, total = (
        int(totals.group(1)),
        int(totals.group(2)),
        int(totals.group(3)),
        int(totals.group(4)),
        int(totals.group(5)),
    )

    if total == 0:
        return None

    reviewer = _detect_reviewer(text)
    deal = _extract_deal(text)

    return {
        "grade_a": round(a / total * 100, 2),
        "grade_b": round(b / total * 100, 2),
        "grade_c": round(c / total * 100, 2),
        "grade_d": round(d / total * 100, 2),
        "sample": total,
        "reviewer": reviewer,
        "deal_name": deal,
    }


def _try_amc_clarifii(text: str) -> Optional[dict]:
    """AMC / Clarifii format:
    OVERALL RESULTS SUMMARY
    ...NRSRO Grade # Loans % of Loans A 1 50.00% B 1 50.00% ...
    Or: Credit Grade Summary ... A 54 77.14% B 16 22.86% ...
    """
    # Try NRSRO grade table first
    for marker in ["NRSRO Grade", "Overall Grade Summary", "Overall Loan Grade"]:
        pos = text.find(marker)
        if pos < 0:
            pos = text.lower().find(marker.lower())
        if pos >= 0:
            section = text[pos:pos+400]
            grades = re.findall(
                r"([ABCD])\s+(\d+)\s+([\d.]+)%",
                section,
            )
            if len(grades) >= 4:
                pcts = {}
                sample = 0
                for g, count, pct in grades[:4]:
                    pcts[g] = float(pct)
                    sample += int(count)
                if len(pcts) >= 4:
                    return {
                        "grade_a": pcts.get("A", 0),
                        "grade_b": pcts.get("B", 0),
                        "grade_c": pcts.get("C", 0),
                        "grade_d": pcts.get("D", 0),
                        "sample": sample,
                        "reviewer": _detect_reviewer(text),
                        "deal_name": _extract_deal(text),
                    }

    # Fallback: look for "Overall" grade table like Clarifii
    pos = text.lower().find("overall grade")
    if pos >= 0:
        # Look forward for the grade table
        section = text[pos:pos+2000]
        # Find first A/B/C/D percentage table
        grades = re.findall(r"([ABCD])\s+(\d+)\s+([\d.]+)%", section)
        if len(grades) >= 4:
            pcts = {}
            sample = 0
            for g, count, pct in grades[:4]:
                if g not in pcts:
                    pcts[g] = float(pct)
                    sample += int(count)
            if len(pcts) >= 4:
                return {
                    "grade_a": pcts.get("A", 0),
                    "grade_b": pcts.get("B", 0),
                    "grade_c": pcts.get("C", 0),
                    "grade_d": pcts.get("D", 0),
                    "sample": sample,
                    "reviewer": _detect_reviewer(text),
                    "deal_name": _extract_deal(text),
                }

    return None


def _detect_reviewer(text: str) -> str:
    tl = text[:2000].lower()
    if "clayton" in tl:
        return "Clayton"
    if "visionet" in tl:
        return "Visionet"
    if "consolidated analytics" in tl:
        return "Consolidated Analytics"
    if "clarifii" in tl:
        return "Clarifii"
    if "amc diligence" in tl or "situsamc" in tl or "amc" in tl:
        return "AMC Diligence"
    if "digital risk" in tl:
        return "Digital Risk"
    if "recovco" in tl:
        return "Recovco"
    if "covius" in tl:
        return "Covius"
    return "Unknown"


def _extract_deal(text: str) -> Optional[str]:
    patterns = [
        r"(FIGRE\s+Trust\s+\d{4}[\w\-]*)",
        r"(ACHM\s+Trust\s+\d{4}[\w\-]*)",
        r"(RCKT\s+(?:Mortgage\s+)?Trust\s+\d{4}[\w\-]*)",
        r"(AOMT\s+\d{4}[\w\-]*)",
        r"(EFMT\s+\d{4}[\w\-]*)",
        r"(MFRA\s+Trust\s+\d{4}[\w\-]*)",
        r"(VCC\s+\d{4}[\w\-]*)",
        r"(CAFL\s+\d{4}[\w\-]*)",
        r"(SG\s+Residential\s+\d{4}[\w\-]*)",
        r"(Saluda\s+Grade\s+[\w\s]+\d{4}[\w\-]*)",
        r"([\w\s]+Trust\s+\d{4}[\w\-]*)",
    ]
    for pat in patterns:
        m = re.search(pat, text[:5000])
        if m:
            return m.group(1).strip().rstrip(".,;")
    return None


def find_tpr_exhibit_url(filing_index_html: str, base_url: str) -> Optional[str]:
    """
    Given a filing index page HTML, find the URL of the TPR exhibit
    (Clayton/Visionet/AMC narrative), skipping KPMG/Deloitte/EY AUPs.

    Returns the exhibit URL or None.
    """
    links = re.findall(r'href="(/Archives/edgar/data/[^"]+\.htm)"', filing_index_html, re.IGNORECASE)
    non_idx = [l for l in links if "index" not in l.rsplit("/", 1)[-1].lower()]

    # Prefer exhibits by number — TPR is usually ex99-1 for Clayton/AMC, ex99-2 for Visionet
    exhibit_links = []
    for l in non_idx:
        fn = l.rsplit("/", 1)[-1].lower()
        if "abs15g" in fn:
            continue
        exhibit_links.append(l)

    return [base_url + l for l in exhibit_links] if exhibit_links else None


if __name__ == "__main__":
    import urllib.request
    import html as _html

    HEADERS = {"User-Agent": "AUP Dashboard kennethjhan@gmail.com"}

    def fetch(url):
        req = urllib.request.Request(url, headers=HEADERS)
        raw = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", errors="replace")
        text = _html.unescape(re.sub(r"<[^>]+>", " ", raw))
        return re.sub(r"\s+", " ", text)

    tests = [
        ("Clayton/Achieve", "https://www.sec.gov/Archives/edgar/data/2020165/000110465925115905/tm2531790d1_ex99-1.htm"),
        ("Visionet/Figure", "https://www.sec.gov/Archives/edgar/data/1970036/000119312526280660/d134036dex992.htm"),
    ]

    for label, url in tests:
        text = fetch(url)
        result = parse_grade_distribution(text)
        print(f"{label}: {result}")

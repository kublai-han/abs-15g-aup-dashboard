"""
exhibit_parser.py

Parses ABS-15G exhibit files (Exhibit 99.1 AUP letters) from SEC EDGAR.
These are typically PDF or HTML files containing Agreed Upon Procedures (AUP)
reports from CPA firms.
"""

import io
import logging
import re
import urllib.request
from typing import Optional
from urllib.error import URLError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional heavy dependencies – imported lazily so the module is importable
# even when only one backend is available.
# ---------------------------------------------------------------------------

def _import_bs4():
    try:
        from bs4 import BeautifulSoup
        return BeautifulSoup
    except ImportError as exc:
        raise ImportError(
            "beautifulsoup4 is required for HTML parsing: pip install beautifulsoup4"
        ) from exc


def _import_pdfplumber():
    try:
        import pdfplumber
        return pdfplumber
    except ImportError:
        return None


def _import_pypdf2():
    try:
        import PyPDF2
        return PyPDF2
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Regex patterns for AUP data extraction
# ---------------------------------------------------------------------------

# Procedure blocks: "Procedure 1", "Procedure No. 2", "1.", "Step 1", etc.
_RE_PROCEDURE_HEADER = re.compile(
    r"(?:Procedure\s*(?:No\.?\s*)?|Step\s*)(\d+)[.\s:–-]",
    re.IGNORECASE,
)

# Exception / finding lines
_RE_EXCEPTION = re.compile(
    r"(?:exception|finding|discrepanc|error|varianc)[^.\n]{0,200}",
    re.IGNORECASE,
)

# Pool / sample size
# Note: negative lookahead (?!\s*%) ensures we don't capture "5%" in "population of 5% error rate"
_RE_POOL_SIZE = re.compile(
    r"(?:pool\s+size|total\s+(?:pool|loans?|assets?|receivables?)|"
    r"(?:loan\s+data\s+file\s+)?contained|comprising|consisting\s+of|"
    r"population\s+of|underlying\s+assets[^\d]{0,5})[^\d]{0,30}([\d,]+)(?!\s*%)",
    re.IGNORECASE,
)
_RE_SAMPLE_SIZE = re.compile(
    # "sample size [of] N" — use \b to avoid matching "sample sizes," (which captures a bare comma)
    r"(?:sample\s+size\b[^\d]{0,20}"
    # "we selected/tested/examined" WITH "sample" explicitly nearby (prevents false matches
    # like "reviewed during the performance of services herein. 2")
    r"|(?:we\s+)?(?:tested|reviewed|examined)\s+[a-z\s]{0,25}sample[a-z\s,]{0,20}"
    # "selected" is more specific: "we selected N accounts" or "selected a sample of N"
    r"|(?:we\s+)?selected\s+(?:a\s+(?:random\s+|judgmental[a-z]*\s+)?)?sample\s+of\s+"
    r"|(?:we\s+)?selected\s+[^\d]{0,30}"
    # Protiviti / narrative: "a judgmentally selected sample of N"
    r"|(?:a\s+)?(?:judgmental[a-z]*\s+)?selected\s+sample\s+of\s+"
    r"|randomly\s+selected\s+[^\d]{0,30}"
    # UK/NewDay style: "A random sample of N" or "a sample of N"
    r"|a\s+random\s+sample\s+of\s+"
    r"|a\s+sample\s+of\s+(?=\d{2,})"   # "a sample of N" only if N>=10 (at least 2 digits)
    r")(\d[\d,]*)",
    re.IGNORECASE,
)

# Exception counts and rates
_RE_EXCEPTION_COUNT = re.compile(
    r"(?:(\d+)\s+exception|exception[s]?\s+(?:were\s+)?(?:noted|found|identified)[^\d]{0,10}(\d+))",
    re.IGNORECASE,
)
# Percentage exception rate: only used as fallback when count cannot be determined.
# Exclude sampling-design language ("3% errors could be observed in the sample").
_RE_EXCEPTION_RATE = re.compile(
    r"([\d.]+)\s*%\s*(?:exception|error|discrepanc)"
    r"(?!\s*(?:rate|could|presuming|observed|be\s+observed))",
    re.IGNORECASE,
)

# Accounting firm / AUP provider
_RE_FIRM_PATTERNS = [
    re.compile(r"(Deloitte[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(Ernst\s*&\s*Young[^,\n]{0,40}|\bEY\b[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(KPMG[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(PricewaterhouseCoopers[^,\n]{0,40}|PwC\b[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(Grant\s+Thornton[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(BDO\s+USA[^,\n]{0,40}|BDO\b[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(RSM\s+US[^,\n]{0,40}|RSM\b[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(Moss\s+Adams[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(Crowe[^,\n]{0,40})", re.IGNORECASE),
    re.compile(r"(Cohen\s*&\s*Company[^,\n]{0,40})", re.IGNORECASE),
    # Protiviti — consulting firm used by Mission Lane and others for CC AUP work
    re.compile(r"(Protiviti\s+Inc\.?|Protiviti\b)", re.IGNORECASE),
    # Generic CPA / LLP / LLC pattern as last-resort fallback (requires audit-context keywords)
    re.compile(
        r"(?:signed\s+by|pursuant\s+to|/s/\s*)([A-Z][A-Za-z\s&,]+(?:LLP|LLC|PC|P\.C\.|P\.A\.))",
        re.IGNORECASE,
    ),
]

# Report date patterns
_RE_DATE_PATTERNS = [
    re.compile(
        r"(?:dated?|as\s+of|report\s+date)[^\w]{0,10}"
        r"(\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        re.IGNORECASE,
    ),
    re.compile(
        r"((?:January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},\s+\d{4})",
        re.IGNORECASE,
    ),
]

# Phrases that indicate zero exceptions / clean findings
_RE_NO_EXCEPTIONS = re.compile(
    r"no\s+exception[s]?\s+(?:were\s+)?noted"
    r"|we\s+noted\s+no\s+exception"
    r"|noted\s+no\s+exceptions?"  # "noted no exceptions" (Prosper/PwC format)
    r"|found\s+(?:all\s+)?(?:\w+\s+){0,5}(?:to\s+be\s+in\s+agreement|in\s+agreement)"
    r"|were\s+found\s+to\s+be\s+in\s+agreement"
    r"|(?:all\s+)?(?:characteristics?|attributes?|fields?)\s+(?:were\s+)?(?:found\s+)?in\s+agreement"
    r"|(?:we\s+)?noted\s+no\s+(?:differences?|discrepanc|findings?)"
    # Protiviti / narrative style: "noting no differences" when all fields are clean
    r"|noting\s+no\s+differences?"
    # "No differences were noted for X" (partial match; full counting handled separately)
    r"|no\s+differences?\s+(?:were\s+)?noted"
    # UK / NewDay format: "agreed to the System, with no exception"
    r"|with\s+no\s+exception"
    # Fair Square / PwC format: "noting no exceptions" (gerund, not past-tense noted)
    r"|noting\s+no\s+exceptions?"
    # EY / Continental format: "All such compared information was in agreement"
    r"|(?:information|data|values?|fields?)\s+was\s+in\s+agreement"
    r"|all\s+such\s+compared\s+information\s+was\s+in\s+agreement",
    re.IGNORECASE,
)

# Narrative format: "noting no differences for X of (the) Y samples tested"
# Used to count exceptions as Y - X when not all samples agree.
# Also: "No differences were noted for X of the Y"
_RE_NOTING_NO_DIFFS_PARTIAL = re.compile(
    r"(?:noting\s+no\s+differences?\s+for|no\s+differences?\s+(?:were\s+)?noted\s+for)"
    r"\s+([\d,]+)\s+of\s+(?:the\s+)?([\d,]+)",
    re.IGNORECASE,
)

# Protiviti narrative exception descriptions. Two common formats:
#   Format A: "Per Management, for sample #36, <explanation>."
#             "Per Management, for two samples (#'s 33 and 111), <explanation>."
#             "Per Management, the reason these samples <explanation>."
#             "Per Management, <any explanation>."
#   Format B: "For 5 samples (#s 4, 128, ...) <description>." (leads into "Per Management" next sentence)
_RE_PROTIVITI_FINDING = re.compile(
    r"(?:"
    r"Per\s+Management\b[^.]{0,400}"       # Any "Per Management, ..." sentence
    r"|For\s+\d+\s+samples?\s*\(?#s?"      # "For N samples (#s X, Y, ...) description"
    r"[^.]{0,400}"
    r")\.",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _clean_text(text: str) -> str:
    """Normalise whitespace without losing paragraph breaks."""
    # Collapse runs of spaces/tabs but keep newlines
    text = re.sub(r"[ \t]+", " ", text)
    # Collapse 3+ blank lines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


_RE_FIRM_TRIM = re.compile(
    r"\s+(?:has|is|was|have|were|should|not|be|been|being|"
    r"in|the|a|an|of|to|for|and(?!\s+Company)|or|that|with|which|such|any|by|"
    r"performing|conducted|engaged|agreed|selected|uses?|pursuant|report|does|dated)\b.*",
    re.IGNORECASE | re.DOTALL,
)

def _extract_firm_name(text: str) -> Optional[str]:
    for pattern in _RE_FIRM_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1).strip()
            # Normalize internal whitespace (multi-line firm names like "GRANT\nTHORNTON LLP")
            raw = re.sub(r"\s+", " ", raw).strip()
            # Trim trailing boilerplate that got captured
            trimmed = _RE_FIRM_TRIM.sub("", raw).strip()
            # Keep trimmed only if it still looks like a firm name (has letters)
            result = trimmed if len(trimmed) >= 3 else raw
            # Strip any trailing non-alphanumeric chars (curly parens, special chars from HTML)
            result = re.sub(r"[^A-Za-z0-9.&,\s]+$", "", result).strip()
            return result if result else raw
    return None


def _extract_report_date(text: str) -> Optional[str]:
    for pattern in _RE_DATE_PATTERNS:
        m = pattern.search(text)
        if m:
            return m.group(1).strip()
    return None


def _extract_pool_and_sample(text: str) -> dict:
    pool_size = None
    sample_size = None

    m = _RE_POOL_SIZE.search(text)
    if m:
        pool_size = m.group(1).replace(",", "")

    m = _RE_SAMPLE_SIZE.search(text)
    if m:
        sample_size = m.group(1).replace(",", "")

    return {"pool_size": pool_size, "sample_size": sample_size}


def _extract_exception_info(text: str) -> dict:
    """Return exception_count (int|None), exception_rate (float|None), findings (list[str])."""
    exception_count = None
    exception_rate = None
    findings = []

    # ── Narrative format: "noting no differences for X of (the) Y" ─────────
    # e.g. "noting no differences for 199 of the 200" → 1 exception
    # Sum all such occurrences across the document.
    partial_matches = list(_RE_NOTING_NO_DIFFS_PARTIAL.finditer(text))
    if partial_matches:
        total_partial_exceptions = 0
        max_sample = 0
        for pm in partial_matches:
            agree = int(pm.group(1).replace(",", ""))
            total_n = int(pm.group(2).replace(",", ""))
            total_partial_exceptions += (total_n - agree)
            if total_n > max_sample:
                max_sample = total_n
        exception_count = total_partial_exceptions
        if max_sample > 0 and exception_count is not None:
            exception_rate = exception_count / max_sample

    # Check for explicit "no exceptions" first (only if narrative pattern didn't fire)
    if exception_count is None and _RE_NO_EXCEPTIONS.search(text):
        exception_count = 0

    # Try to extract a count
    m = _RE_EXCEPTION_COUNT.search(text)
    if m and exception_count is None:
        raw = m.group(1) or m.group(2)
        if raw:
            exception_count = int(raw)

    # Exception rate — only use regex as fallback when count is completely unknown;
    # if count was determined (even 0), the rate will be computed from count/sample later.
    # Divide by 100 to convert from percentage to proportion (consistent with count/sample).
    if exception_count is None:
        m = _RE_EXCEPTION_RATE.search(text)
        if m:
            try:
                exception_rate = float(m.group(1)) / 100.0
            except ValueError:
                pass

    # ── Protiviti narrative: "Per Management, for sample #N, ..." ───────────
    # Extract inline exception explanations as finding details.
    protiviti_findings = [
        m.group(0).strip()
        for m in _RE_PROTIVITI_FINDING.finditer(text)
    ]
    if protiviti_findings:
        findings.extend(protiviti_findings)

    # Collect finding sentences — exclude methodology/sampling descriptions
    # and SEC cover-form section headers that mention "finding" but are not findings
    _METHODOLOGY_SKIP = re.compile(
        r"confidence\s+level|presuming|could\s+be\s+observed|attribute\s+sampling"
        r"|error\s+rate\s+in\s+the\s+population|^\s*findings?\s*$"
        r"|findings?\s+and\s+conclusions?\s+of\s+(third[- ]party|a\s+third)"
        r"|due\s+diligence\s+report(s)?\s+obtained\s+by"
        r"|findings?\s+are\s+as\s+follows\s*:?\s*$"
        r"|exception\s+list\s*$"
        # Generic closing language ("report our findings based on the procedures")
        r"|(?:report\s+our\s+)?findings\s+based\s+on\s+the\s+procedures"
        r"|findings\s+(?:described|noted|set\s+forth)\s+(?:above|herein|below)"
        # Protiviti / narrative disclaimer boilerplate
        r"|findings\s+shall\s+in\s+any\s+way\s+constitute"
        r"|errors?,\s+fraud|irregularities"
        r"|findings?\s+for\s+any\s+other\s+factor"
        # Grant Thornton / EY: "findings are as described herein" boilerplate
        r"|findings?\s+are\s+as\s+described\s+herein"
        # Grant Thornton: "discrepancies between the Database and the Source Documents"
        r"|discrepancies\s+between\s+the\s+(?:Database|database)\s+and\s+the\s+Source"
        # EY / Continental: "findings are included in Attachment" and "findings with respect to:"
        r"|findings?\s+are\s+included\s+in\s+Attachment"
        r"|findings?\s+with\s+respect\s+to\s*:"
        # UK / NewDay confidence-interval boilerplate (not a finding — sampling methodology)
        r"|errors?,?\s+within\s+a\s+total\s+population"
        r"|errors?\s+within\s+the\s+total\s+population"
        r"|sampling\s+confidence\s+(?:is|and)\s+(?:the\s+)?precision"
        r"|precision\s+limit\s+is\s+the\s+estimated"
        # Fair Square / PwC: generic exception-threshold definition
        r"|exception\s+if\s+(?:there\s+was\s+a\s+difference|differences\s+are\s+greater)"
        # UK/NewDay methodology boilerplate: "is termed an error", "error in the relevant attribute"
        r"|is\s+termed\s+an\s+error"
        r"|^error\s+in\s+the\s+relevant\s+attribute"
        r"|failure\s+of\s+a\s+single\s+attribute\s+is\s+termed"
        r"|we\s+report\s+our\s+findings,\s+which\s+are\s+the\s+factual"
        # Generic noise: bare "error" or "error." (not a finding sentence)
        r"|^error\.?$",
        re.IGNORECASE,
    )
    for m in _RE_EXCEPTION.finditer(text):
        snippet = m.group(0).strip()
        if snippet and snippet not in findings and not _METHODOLOGY_SKIP.search(snippet):
            findings.append(snippet)

    # If no exceptions confirmed: keep only genuine positive confirmations (not noise)
    if exception_count == 0:
        # When zero exceptions are confirmed, any "error" / "exception" / "discrepancy"
        # snippet is methodology boilerplate, not a real finding.  Strip them all and
        # use the standard clean message.
        real_findings = [
            f for f in findings
            if not re.search(
                r"\berrors?\b|\bexceptions?\b|\bdiscrepanc|\bvarianc|\bfindings?\b",
                f, re.IGNORECASE
            )
        ]
        # Keep only non-noise confirmations; fall back to standard message
        findings = real_findings if real_findings else ["No exceptions noted"]

    return {
        "exception_count": exception_count,
        "exception_rate": exception_rate,
        "findings": findings[:10],  # cap to avoid noise
    }


def _extract_fields_count(text: str) -> Optional[int]:
    """
    Count the number of loan data attributes/fields being checked in an AUP.

    Handles three formats:

    1. Numbered characteristics (Santander style):
       "Characteristics\n1. Origination date\n2. VIN\n...\n16. Payment to income"
       → returns the highest numbered item (16)

    2. Attribute table (CPS style):
       "Attribute [newlines] Receivable File / Instructions\n[attr rows]"
       → counts short lines that look like attribute names (not source-doc refs)

    3. Explicit count in prose:
       "we compared the following 9 attributes" → returns 9

    Returns None if no structured field list is found.
    """
    # Method 1: Numbered characteristics (Santander/Westlake style)
    # Header word "Characteristics" or "Data Fields" followed by "N. text" items
    chars_header = re.search(
        r"\b(?:Characteristics?|Data\s+Fields?|Specified\s+Attributes?)\b",
        text, re.IGNORECASE,
    )
    if chars_header:
        section = text[chars_header.end(): chars_header.end() + 4000]
        # Find all "N. <word>" numbered items within the next ~4000 chars
        nums = re.findall(r"\b(\d{1,2})\.\s+[A-Z][A-Za-z]", section)
        if nums:
            max_n = max(int(n) for n in nums)
            if max_n >= 2:
                return max_n

    # Method 2: Attribute table with "Attribute ... Receivable File" header (CPS style)
    attr_header = re.search(
        r"\bAttribute\b.{0,300}?\bReceivable\s+File\b",
        text, re.IGNORECASE | re.DOTALL,
    )
    if attr_header:
        section = text[attr_header.end(): attr_header.end() + 3000]
        lines = section.splitlines()
        count = 0
        _SOURCE_RE = re.compile(
            r"Sale\s+Contract|Lending|Federal\s+Truth|Credit\s+Application"
            r"|Instructions?|Receivable\s+File|Exhibit\s+[A-Z]|Document",
            re.IGNORECASE,
        )
        for line in lines:
            s = line.strip()
            if not s or s in ("?",) or len(s) < 3:
                continue
            # Stop at lettered procedure sections (A. B. C.)
            if re.match(r"^[A-D]\.\s", s):
                break
            # Stop at long procedural sentences (usually start with subject verbs)
            if len(s) > 100:
                continue
            # Attribute names are short and NOT source-document references
            if len(s) < 70 and not _SOURCE_RE.search(s):
                count += 1
        if count >= 2:
            return count

    # Method 3: Explicit numeric count in prose
    m = re.search(
        r"following\s+(\d+)\s+(?:attributes?|fields?|characteristics?|data\s+elements?)",
        text, re.IGNORECASE,
    )
    if m:
        n = int(m.group(1))
        if n >= 2:
            return n

    return None


def _split_into_procedures(text: str) -> list[dict]:
    """
    Split a block of text on procedure headers and return a list of raw
    procedure dicts with keys: procedure_number, raw_text.
    """
    # Find all procedure header positions
    headers = list(_RE_PROCEDURE_HEADER.finditer(text))

    if not headers:
        # Return the whole document as a single pseudo-procedure
        return [{"procedure_number": None, "raw_text": text}]

    procedures = []
    for i, match in enumerate(headers):
        start = match.start()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        proc_num = int(match.group(1))
        raw = text[start:end].strip()
        procedures.append({"procedure_number": proc_num, "raw_text": raw})

    return procedures


def _parse_procedure_block(block: dict) -> dict:
    """
    Given a raw procedure block, extract structured fields and return a
    clean procedure result dict.
    """
    raw = block["raw_text"]
    lines = [l.strip() for l in raw.splitlines() if l.strip()]

    # First non-empty line after the header is treated as the description.
    # For numbered procedures, skip any leading stand-alone digit (paragraph markers).
    _SKIP_LINE = re.compile(
        r"^\s*(?:ex-?\d+\.\d+|exhibit\s*\d+|[\w\-]+\.(htm|html|txt|pdf)"
        r"|\d+\s*$|EX-99)\s*$",
        re.IGNORECASE,
    )
    if block["procedure_number"] is None:
        # Prose-format AUP: use first substantive line (>30 chars, not a filename/header) as description
        description = next(
            (l for l in lines if len(l) > 30 and not _SKIP_LINE.match(l)),
            lines[0] if lines else "",
        )
    else:
        candidate = lines[1] if len(lines) > 1 else (lines[0] if lines else "")
        # Skip bare numbers or very short tokens that are pagination artifacts
        if candidate and (candidate.isdigit() or len(candidate) < 5):
            candidate = lines[2] if len(lines) > 2 else candidate
        description = candidate

    sizes = _extract_pool_and_sample(raw)
    exc_info = _extract_exception_info(raw)
    fields_count = _extract_fields_count(raw)

    return {
        "procedure_number": block["procedure_number"],
        "description": description,
        "pool_size": sizes["pool_size"],
        "sample_size": sizes["sample_size"],
        "exception_count": exc_info["exception_count"],
        "exception_rate": exc_info["exception_rate"],
        "findings": exc_info["findings"],
        "fields_count": fields_count,
        "raw_text": raw,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_exhibit(url: str) -> str:
    """
    Download an exhibit file from *url* (HTTP/HTTPS) or read from a local
    path and return the raw bytes decoded as UTF-8 where possible.

    For PDF files the raw bytes are returned as a latin-1 string so that
    the caller can detect the magic bytes and dispatch to parse_aup_pdf.

    Returns
    -------
    str
        Raw content of the file. Callers should inspect the first few bytes
        to determine whether the content is HTML, plain text, or PDF.
    """
    headers = {
        "User-Agent": "AUP Dashboard contact@example.com",
        "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
    }

    try:
        if url.startswith(("http://", "https://")):
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw_bytes = resp.read()
        else:
            # Local file path
            with open(url, "rb") as fh:
                raw_bytes = fh.read()
    except (URLError, OSError) as exc:
        logger.error("Failed to fetch exhibit from %s: %s", url, exc)
        raise

    # PDF: return as latin-1 so magic bytes (%PDF) are preserved
    if raw_bytes[:4] == b"%PDF":
        return raw_bytes.decode("latin-1")

    # HTML / text: try UTF-8, fall back to latin-1
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return raw_bytes.decode("latin-1")


def _is_exception_table(headers: list[str]) -> bool:
    """Return True if this table looks like the exception description/findings table."""
    joined = " ".join(headers).lower()
    individual = {h.lower().strip() for h in headers}
    if "exception" not in joined:
        return False
    # Classic format: "Exception Description" / "Number of Exceptions" / "Finding" column
    if "description" in joined or "number" in joined or "finding" in joined:
        return True
    # Ford/PwC format: standalone "Exception" column (headers like
    # ["Sample Receivable", "Procedure", "Exception"])
    if "exception" in individual:
        return True
    return False


def _parse_html_tables(soup) -> dict:
    """
    Extract exception count, sample size, and findings from HTML tables.
    Returns dict with keys: exception_count, sample_size, findings.
    """
    exception_count = None
    findings = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        # Find the header row — scan all rows since blank spacers may come first
        header_idx = None
        for idx, row in enumerate(rows):
            header_cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
            if _is_exception_table(header_cells):
                header_idx = idx
                break
        if header_idx is None:
            continue

        # Data rows (everything after the header, skip blanks)
        data_rows = []
        for row in rows[header_idx + 1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
            cells = [c for c in cells if c]
            if cells:
                data_rows.append(cells)

        if not data_rows:
            continue

        # Word-number map for parsing descriptions like "Two differences..."
        _WORD_NUM = {
            "no": 0, "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4,
            "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        }
        _RE_WORD_COUNT = re.compile(
            r'\b(no|zero|one|two|three|four|five|six|seven|eight|nine|ten|\d+)\s+'
            r'(?:differences?|exceptions?|discrepancies?|findings?|errors?)\b',
            re.IGNORECASE,
        )

        total_exceptions = 0
        found_any_count = False
        for cells in data_rows:
            # Pull description text (second meaningful cell)
            desc = ""
            for cell in cells[1:]:
                if len(cell) > 10:
                    desc = cell
                    if desc not in findings:
                        findings.append(desc)
                    break

            # Try to parse count from description ("Two differences for APR" → 2)
            m = _RE_WORD_COUNT.search(desc)
            if m:
                token = m.group(1).lower()
                count = _WORD_NUM.get(token, None)
                if count is None:
                    try:
                        count = int(token)
                    except ValueError:
                        count = 1
                total_exceptions += count
                found_any_count = True
            else:
                # Fallback: each row represents one exception
                total_exceptions += 1
                found_any_count = True

        if found_any_count:
            exception_count = total_exceptions

    return {
        "exception_count": exception_count,
        "findings": findings,
    }


def parse_aup_html(html_content: str) -> list[dict]:
    """
    Parse an HTML AUP exhibit and return a list of procedure result dicts.

    Each dict contains:
        procedure_number  : int | None
        description       : str
        pool_size         : str | None   (numeric string, no commas)
        sample_size       : str | None
        exception_count   : int | None
        exception_rate    : float | None
        findings          : list[str]
        raw_text          : str

    Additionally, the *first* dict carries top-level metadata keys:
        aup_provider      : str | None
        report_date       : str | None

    Falls back to returning a single dict with raw_text if parsing fails.
    """
    try:
        BeautifulSoup = _import_bs4()
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract structured data from tables before stripping HTML
        table_data = _parse_html_tables(soup)

        # Remove boilerplate tags
        for tag in soup(["script", "style", "meta", "head", "nav", "footer"]):
            tag.decompose()

        text = _clean_text(soup.get_text(separator="\n"))
    except Exception as exc:
        logger.warning("HTML parsing failed, falling back to raw text: %s", exc)
        text = _clean_text(html_content)
        table_data = {"exception_count": None, "findings": []}

    procedures = _parse_text_to_procedures(text)

    # Overlay table-extracted data onto the first procedure (or only procedure)
    if procedures:
        p = procedures[0]
        # Only override if table parsing found something better
        if table_data["exception_count"] is not None:
            p["exception_count"] = table_data["exception_count"]
        if table_data["findings"]:
            p["findings"] = table_data["findings"]
        # If no exceptions found via table but "no exceptions" phrase in text, set 0
        if p["exception_count"] is None and _RE_NO_EXCEPTIONS.search(text):
            p["exception_count"] = 0
            if not p["findings"]:
                p["findings"] = ["No exceptions noted"]
        # Compute exception rate from count / sample_size
        if p["exception_count"] is not None and p.get("sample_size"):
            try:
                p["exception_rate"] = p["exception_count"] / int(p["sample_size"])
            except (ValueError, ZeroDivisionError):
                pass

    return procedures


def parse_aup_pdf(pdf_bytes: bytes) -> list[dict]:
    """
    Parse a PDF AUP exhibit and return a list of procedure result dicts
    (same schema as parse_aup_html).

    Tries pdfplumber first, falls back to PyPDF2, then returns a minimal
    dict with whatever text was extracted.
    """
    text = None

    # --- pdfplumber (preferred) ---
    pdfplumber = _import_pdfplumber()
    if pdfplumber is not None:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [page.extract_text() or "" for page in pdf.pages]
            text = _clean_text("\n".join(pages))
            logger.debug("Extracted %d chars from PDF via pdfplumber", len(text))
        except Exception as exc:
            logger.warning("pdfplumber extraction failed: %s", exc)
            text = None

    # --- PyPDF2 fallback ---
    if text is None:
        PyPDF2 = _import_pypdf2()
        if PyPDF2 is not None:
            try:
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
                pages = [
                    (reader.pages[i].extract_text() or "")
                    for i in range(len(reader.pages))
                ]
                text = _clean_text("\n".join(pages))
                logger.debug("Extracted %d chars from PDF via PyPDF2", len(text))
            except Exception as exc:
                logger.warning("PyPDF2 extraction failed: %s", exc)
                text = None

    if text is None:
        logger.error("All PDF extraction methods failed; returning empty result")
        return [
            {
                "procedure_number": None,
                "description": "",
                "pool_size": None,
                "sample_size": None,
                "exception_count": None,
                "exception_rate": None,
                "findings": [],
                "raw_text": "",
                "aup_provider": None,
                "report_date": None,
                "parse_error": "PDF text extraction failed",
            }
        ]

    return _parse_text_to_procedures(text)


def _parse_text_to_procedures(text: str) -> list[dict]:
    """
    Shared logic: given clean plain text, split on procedure headers,
    enrich each block, and attach top-level metadata to the first item.
    """
    aup_provider = _extract_firm_name(text)
    report_date = _extract_report_date(text)

    raw_blocks = _split_into_procedures(text)
    results = [_parse_procedure_block(b) for b in raw_blocks]

    # Attach metadata to first record (top-level context)
    if results:
        results[0]["aup_provider"] = aup_provider
        results[0]["report_date"] = report_date
    else:
        results = [
            {
                "procedure_number": None,
                "description": "",
                "pool_size": None,
                "sample_size": None,
                "exception_count": None,
                "exception_rate": None,
                "findings": [],
                "raw_text": text,
                "aup_provider": aup_provider,
                "report_date": report_date,
            }
        ]

    logger.info(
        "Parsed %d procedure(s); provider=%s date=%s",
        len(results),
        aup_provider,
        report_date,
    )
    return results


def extract_aup_data(url: str) -> dict:
    """
    Main entry point – fetch and parse any AUP exhibit (HTML or PDF).

    Parameters
    ----------
    url : str
        HTTP/HTTPS URL or local file path to the exhibit.

    Returns
    -------
    dict with keys:
        url          : str
        content_type : "html" | "pdf" | "unknown"
        procedures   : list[dict]   – parsed procedure results
        raw_text     : str          – full extracted text (for search / debug)
        aup_provider : str | None
        report_date  : str | None
        error        : str | None   – set if a non-fatal problem occurred
    """
    result: dict = {
        "url": url,
        "content_type": "unknown",
        "procedures": [],
        "raw_text": "",
        "aup_provider": None,
        "report_date": None,
        "error": None,
    }

    try:
        raw = fetch_exhibit(url)
    except Exception as exc:
        result["error"] = f"Fetch failed: {exc}"
        logger.error("extract_aup_data fetch error for %s: %s", url, exc)
        return result

    result["raw_text"] = raw

    # Detect content type
    is_pdf = raw[:4] == "%PDF" or url.lower().endswith(".pdf")
    is_html = (
        not is_pdf
        and (
            "<html" in raw[:2000].lower()
            or "<!doctype" in raw[:200].lower()
            or url.lower().endswith((".htm", ".html"))
        )
    )

    try:
        if is_pdf:
            result["content_type"] = "pdf"
            pdf_bytes = raw.encode("latin-1")
            procedures = parse_aup_pdf(pdf_bytes)
        elif is_html:
            result["content_type"] = "html"
            procedures = parse_aup_html(raw)
        else:
            # Plain text – treat like HTML (BeautifulSoup handles plain text)
            result["content_type"] = "text"
            procedures = parse_aup_html(raw)
    except Exception as exc:
        result["error"] = f"Parse failed: {exc}"
        logger.error("extract_aup_data parse error for %s: %s", url, exc)
        # Return what we have
        return result

    result["procedures"] = procedures

    # Hoist top-level metadata from first procedure record
    if procedures:
        result["aup_provider"] = procedures[0].get("aup_provider")
        result["report_date"] = procedures[0].get("report_date")

    return result


# ---------------------------------------------------------------------------
# CLI convenience
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python exhibit_parser.py <url_or_path>")
        sys.exit(1)

    target = sys.argv[1]
    data = extract_aup_data(target)

    # Print a readable summary
    print(f"\n=== AUP Exhibit: {data['url']} ===")
    print(f"Content type : {data['content_type']}")
    print(f"AUP provider : {data['aup_provider']}")
    print(f"Report date  : {data['report_date']}")
    print(f"Procedures   : {len(data['procedures'])}")
    if data["error"]:
        print(f"Error        : {data['error']}")
    print()

    for proc in data["procedures"]:
        num = proc.get("procedure_number", "?")
        desc = (proc.get("description") or "")[:80]
        exc = proc.get("exception_count")
        rate = proc.get("exception_rate")
        print(f"  Proc {num}: {desc}")
        print(
            f"    Pool={proc.get('pool_size')}  Sample={proc.get('sample_size')}  "
            f"Exceptions={exc}  Rate={rate}"
        )
        for f in proc.get("findings", [])[:3]:
            print(f"    Finding: {f[:100]}")
        print()

"""
aup_agent.py

Claude-powered verification agent for the ABS-15G AUP dashboard.

Functions
---------
verify_calculations(results)  -- checks exception_rate = exception_count / pool_size
review_prompt(prompt_text)    -- reviews and improves an analysis prompt
run_full_verification()       -- full verification pass; returns a report dict

Run standalone:
    python aup_agent.py

Requires: ANTHROPIC_API_KEY environment variable (or Streamlit secrets).
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import anthropic

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "aup_data.db"


def _make_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY is not set. "
            "Add it as a Streamlit secret or environment variable."
        )
    return anthropic.Anthropic(api_key=api_key)


def _get_aup_results(limit: int = 100) -> list[dict]:
    """Pull AUP results from the shared database."""
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM aup_results ORDER BY filing_date DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def _extract_json(text: str) -> dict:
    """Extract the first JSON object from a text string."""
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass
    return {"raw": text}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def verify_calculations(results: list[dict]) -> dict:
    """
    Use Claude to verify exception_rate calculations and flag anomalies.

    Parameters
    ----------
    results : list[dict]
        AUP result rows from aup_results table.

    Returns
    -------
    dict with keys:
        verified_count, issues (list), summary, overall_status
    """
    if not results:
        return {
            "verified_count": 0,
            "issues": [],
            "summary": "No AUP results to verify.",
            "overall_status": "pass",
        }

    sample = [
        {
            "id":               r.get("id"),
            "issuer_key":       r.get("issuer_key"),
            "filing_date":      r.get("filing_date"),
            "procedure_number": r.get("procedure_number"),
            "exception_count":  r.get("exception_count"),
            "pool_size":        r.get("pool_size"),
            "exception_rate":   r.get("exception_rate"),
        }
        for r in results[:50]
    ]

    prompt = f"""You are a data quality auditor for ABS securitization AUP (Agreed Upon Procedures) reports.

Review these {len(sample)} AUP result records:

{json.dumps(sample, indent=2)}

For each record with non-null exception_count, pool_size, AND exception_rate:
1. Verify exception_rate ≈ exception_count / pool_size (tolerance 0.0001)
2. Flag mismatches
3. Flag invalid values: exception_rate > 1.0, negative counts, pool_size = 0, exception_count > pool_size
4. Note records missing values that should be present

Return ONLY a JSON object (no markdown, no explanation outside the JSON):
{{
  "verified_count": <int>,
  "issues": [
    {{"id": <int>, "issuer_key": "<str>", "issue_type": "<mismatch|invalid|missing>", "description": "<str>"}}
  ],
  "summary": "<one paragraph>",
  "overall_status": "<pass|warn|fail>"
}}"""

    client = _make_client()
    with client.messages.stream(
        model="claude-opus-4-6",
        max_tokens=4096,
        thinking={"type": "adaptive"},
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        text = stream.get_final_message().content
        text = next((b.text for b in text if b.type == "text"), "{}")

    return _extract_json(text)


def review_prompt(prompt_text: str, context: str = "") -> dict:
    """
    Review and improve an analysis prompt or query.

    Parameters
    ----------
    prompt_text : str
        The prompt or query to review.
    context : str, optional
        Additional context about what the prompt is used for.

    Returns
    -------
    dict with keys:
        assessment, issues (list), improved_prompt, suggestions (list), confidence
    """
    client = _make_client()

    system = (
        "You are an expert in ABS (Asset-Backed Securities) AUP (Agreed Upon Procedures) "
        "data analysis. You review prompts and queries used to analyze AUP exception data "
        "from SEC EDGAR ABS-15G filings. You check for correctness, completeness, "
        "potential misinterpretations, and improve clarity."
    )

    message = f"""Review this prompt/query for AUP data analysis:

---
{prompt_text}
---

Context: {context or "Used to query or analyze AUP results from the aup_results database table"}

Provide a JSON object (no markdown fencing):
{{
  "assessment": "<brief assessment>",
  "issues": ["<issue1>", "<issue2>"],
  "improved_prompt": "<improved version or same if already good>",
  "suggestions": ["<suggestion1>", "<suggestion2>"],
  "confidence": "<high|medium|low>"
}}"""

    with client.messages.stream(
        model="claude-opus-4-6",
        max_tokens=2048,
        thinking={"type": "adaptive"},
        system=system,
        messages=[{"role": "user", "content": message}],
    ) as stream:
        text = stream.get_final_message().content
        text = next((b.text for b in text if b.type == "text"), "{}")

    return _extract_json(text)


def run_full_verification() -> dict:
    """
    Run a full calculation verification pass against the database.

    Returns
    -------
    dict
        Combined report: records_checked, calculation_verification, generated_at
    """
    logger.info("Starting AUP data verification...")
    results = _get_aup_results(limit=100)
    calc_report = verify_calculations(results)

    report = {
        "records_checked": len(results),
        "calculation_verification": calc_report,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    logger.info(
        "Verification complete: %d records, status=%s, issues=%d",
        len(results),
        calc_report.get("overall_status", "unknown"),
        len(calc_report.get("issues", [])),
    )
    return report


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = run_full_verification()
    print(json.dumps(report, indent=2))

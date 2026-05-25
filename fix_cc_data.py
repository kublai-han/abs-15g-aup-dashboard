#!/usr/bin/env python3
"""
fix_cc_data.py
One-pass script to:
  1. Delete CC filings with no AUP data (15Ga-1 annual reports)
  2. Strip 'for the reporting period' preamble from deal names
  3. Fix 'Deloitte &' truncated provider names
  4. Normalize provider capitalization (grant thornton -> Grant Thornton LLP)
"""
import re, sqlite3, json

conn = sqlite3.connect("aup_dashboard.db")
conn.row_factory = sqlite3.Row

# ── 1. Delete empty CC filings (no provider, no sample, no exception count) ──
# These are 15Ga-1 annual repurchase demand reports with no AUP exhibit.
result = conn.execute("""
    DELETE FROM procedures WHERE filing_id IN (
        SELECT f.id FROM filings f
        LEFT JOIN procedures p ON p.filing_id = f.id
        WHERE f.asset_type = 'credit_card'
          AND f.aup_provider IS NULL
          AND (p.sample_size IS NULL OR p.sample_size = '')
          AND p.exception_count IS NULL
    )
""")
print(f"Deleted {result.rowcount} procedure rows for empty filings")

result = conn.execute("""
    DELETE FROM filings WHERE id IN (
        SELECT f.id FROM filings f
        LEFT JOIN procedures p ON p.filing_id = f.id
        WHERE f.asset_type = 'credit_card'
          AND f.aup_provider IS NULL
          AND (p.sample_size IS NULL OR p.sample_size = '')
          AND p.exception_count IS NULL
    )
""")
print(f"Deleted {result.rowcount} empty CC filings")

# ── 2. Strip 'for the reporting period ...' preamble from deal names ────────
rows = conn.execute("""
    SELECT id, deal_name FROM filings
    WHERE deal_name LIKE '%reporting period%'
""").fetchall()
fixed_deals = 0
for r in rows:
    # Remove everything up to and including 'reporting period '
    new_name = re.sub(r'^.*?\breporting\s+period\s+', '', r["deal_name"], flags=re.IGNORECASE).strip()
    if new_name != r["deal_name"]:
        conn.execute("UPDATE filings SET deal_name=? WHERE id=?", (new_name, r["id"]))
        print(f"  Fixed deal name: '{r['deal_name'][:60]}' -> '{new_name}'")
        fixed_deals += 1
print(f"Fixed {fixed_deals} deal name preambles")

# ── 3. Fix truncated provider names ─��───────────────────────────────────────
provider_fixes = [
    # Pattern -> replacement
    (r"^Deloitte\s*&\s*$", "Deloitte & Touche LLP"),
    (r"^Deloitte\s*$", "Deloitte & Touche LLP"),
    (r"^grant\s+thornton\s+llp$", "Grant Thornton LLP"),
    (r"^grant\s+thornton$", "Grant Thornton LLP"),
    (r"^ERNST\s*&\s*YOUNG\s+LLP\.?$", "Ernst & Young LLP"),
    (r"^deloitte\.co\.uk$", "Deloitte LLP"),
    (r"^DELOITTE\s+LLP$", "Deloitte LLP"),
]
prov_rows = conn.execute("""
    SELECT id, aup_provider FROM filings
    WHERE aup_provider IS NOT NULL AND asset_type = 'credit_card'
""").fetchall()
fixed_prov = 0
for r in prov_rows:
    prov = r["aup_provider"]
    for pattern, replacement in provider_fixes:
        if re.match(pattern, prov, re.IGNORECASE):
            conn.execute("UPDATE filings SET aup_provider=? WHERE id=?", (replacement, r["id"]))
            print(f"  Fixed provider: '{prov}' -> '{replacement}'")
            fixed_prov += 1
            break
print(f"Fixed {fixed_prov} provider names")

# ��─ 4. Fix 'Deloitte & Touche LLP.' (trailing dot) ──────────────────────────
conn.execute("""
    UPDATE filings SET aup_provider = TRIM(aup_provider, '.')
    WHERE aup_provider LIKE '%.' AND asset_type='credit_card'
""")

# ── 5. Fix noisy findings for cc filings ─���──────────────────────────────────
# Remove findings that are pure methodology boilerplate
_NOISE_PATTERNS = [
    r"^findings\s+are\s+as\s+follows",
    r"^findings\s+are\s+included\s+in\s+Attachment",
    r"^findings\s+and\s+conclusions",
    r"^findings\s+based\s+on",
    r"^findings\s+of\s+\w",
    r"^findings\s+being\s+reported",
    r"^findings\s+with\s+respect\s+to",
    r"^findings\s+shall",
    r"^findings\s+therefrom",
    r"^findings\s+described",
    r"^findings?\s*:",
    r"^exceptions?\s*:",
    r"^exceptions?\s*$",
    r"^exception\s+if\s+there\s+was",
    r"^exception\s+if\s+differ",
    r"^errors?,\s+fraud",
    r"^errors?,\s+within\s+a\s+total",
    r"^errors?\s+within\s+the",
    r"exception\s+list",
]
_noise_re = re.compile("|".join(_NOISE_PATTERNS), re.IGNORECASE)

proc_rows = conn.execute("""
    SELECT p.id, p.findings_json, f.asset_type FROM procedures p
    JOIN filings f ON f.id = p.filing_id
    WHERE f.asset_type = 'credit_card'
      AND p.findings_json IS NOT NULL
      AND p.findings_json != '[]'
""").fetchall()

fixed_findings = 0
for r in proc_rows:
    try:
        findings = json.loads(r["findings_json"])
    except Exception:
        continue
    cleaned = [f for f in findings if f and not _noise_re.search(f.strip())]
    # Also clean up very short noise snippets (< 15 chars)
    cleaned = [f for f in cleaned if len(f.strip()) >= 15]
    if cleaned != findings:
        conn.execute("UPDATE procedures SET findings_json=? WHERE id=?",
                     (json.dumps(cleaned), r["id"]))
        fixed_findings += 1

print(f"Cleaned findings for {fixed_findings} procedure rows")

conn.commit()
conn.close()
print("\nDone.")

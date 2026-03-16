# ABS-15G Dashboard — Data Quality Assurance Report

**Project:** ABS-15G Dashboard
**File verified:** `/workspace/group/issuers.py`
**Report date:** 2026-03-15
**Analyst:** DQA Manager (automated SEC EDGAR live check)
**API used:** `https://data.sec.gov/submissions/CIK{padded_cik}.json`

---

## Executive Summary

11 issuers were verified against live SEC EDGAR data.
**6 issuers have incorrect CIKs** (4 of which were already flagged with TODO comments).
2 additional issuers have correct CIKs but the CIK resolves to the parent public holding company, which files no ABS-15G directly — the ABS-15G filer is a subsidiary or depositor entity.
Only 3 issuers were fully correct as-is.

**Overall Data Quality Score: 27 / 100**
_(3 full PASS out of 11; 6 CIK mismatches; 2 name/entity ambiguities)_

---

## Issuer-by-Issuer Results

### 1. pagaya — Pagaya Technologies

| Field | Value |
|---|---|
| CIK in file | `0001883944` |
| EDGAR name at that CIK | Kowalewski Eric (individual — unrelated) |
| Status | **FAIL — Wrong CIK** |
| Correct ABS-15G filer | Pagaya Structured Products LLC |
| Correct CIK | `0001897077` |
| ABS-15G filings at correct CIK | 10 |

**Action:** Replace CIK `0001883944` → `0001897077`. Note: the ABS-15G filing entity is "Pagaya Structured Products LLC", a Delaware special-purpose vehicle, not the Israeli parent "Pagaya Technologies Ltd."

---

### 2. affirm — Affirm Holdings

| Field | Value |
|---|---|
| CIK in file | `0001820201` |
| EDGAR name at that CIK | Redbox Entertainment Inc. (unrelated) |
| Status | **FAIL — Wrong CIK** |
| Correct public holding company | Affirm Holdings, Inc. (AFRM) |
| Correct holding CIK | `0001820953` |
| ABS-15G filings at holding CIK | 0 |
| Actual ABS-15G filer | Affirm, Inc. (operating subsidiary) |
| ABS-15G filer CIK | `0001714052` |
| ABS-15G filings at operating CIK | 36 |

**Action:** Replace CIK `0001820201` → `0001820953` (Affirm Holdings, Inc.) for the holding company reference. Consider whether the dashboard should track the ABS-15G filer entity `0001714052` (Affirm, Inc.) instead. CIK `0001820953` is unambiguously the correct CIK for "Affirm Holdings."

---

### 3. oportun — Oportun Financial Corporation

| Field | Value |
|---|---|
| CIK in file | `0001538716` |
| EDGAR name at that CIK | Oportun Financial Corp (OPRT) |
| Status | **WARN — Name match, no ABS-15G at this CIK** |
| ABS-15G filings at this CIK | 0 |
| Actual ABS-15G filer | Oportun, Inc. (depositor entity) |
| ABS-15G filer CIK | `0001478295` |
| ABS-15G filings at depositor CIK | 25 |

**Action:** CIK is the correct parent/holding entity; however, ABS-15G filings are filed by depositor entity `0001478295` (Oportun, Inc.). If the dashboard tracks ABS-15G filings, the operative CIK should be `0001478295`.

---

### 4. sofi — SoFi Technologies

| Field | Value |
|---|---|
| CIK in file | `0001818502` |
| EDGAR name at that CIK | OppFi Inc. (unrelated competitor) |
| Status | **FAIL — Wrong CIK** |
| Correct public holding company | SoFi Technologies, Inc. (SOFI) |
| Correct holding CIK | `0001818874` |
| ABS-15G filings at holding CIK | 0 |
| Actual ABS-15G filer | SoFi Lending Corp. (operating subsidiary) |
| ABS-15G filer CIK | `0001555110` |
| ABS-15G filings at operating CIK | 106 |

**Action:** Replace CIK `0001818502` → `0001818874` (SoFi Technologies, Inc.) for the holding company reference. The ABS-15G filer is `0001555110` (SoFi Lending Corp.).

---

### 5. lendingclub — LendingClub Corporation

| Field | Value |
|---|---|
| CIK in file | `0001409970` |
| EDGAR name at that CIK | LendingClub Corp (LC) |
| ABS-15G filings | 13 |
| Status | **PASS** |

---

### 6. prosper — Prosper Marketplace

| Field | Value |
|---|---|
| CIK in file | `0001416635` |
| EDGAR name at that CIK | Trendiki Inc (unrelated) |
| Status | **FAIL — Wrong CIK** |
| Actual ABS-15G filer | Prosper Depositor LLC |
| Correct CIK | `0001705499` |
| ABS-15G filings at correct CIK | 20 |

**Action:** Replace CIK `0001416635` → `0001705499`. Prosper Marketplace does not have its own SEC registrant CIK; ABS-15G filings are submitted by the depositor entity Prosper Depositor LLC.

---

### 7. enova — Enova International

| Field | Value |
|---|---|
| CIK in file | `0001529864` |
| EDGAR name at that CIK | Enova International, Inc. (ENVA) |
| ABS-15G filings | 11 |
| Status | **PASS** |

---

### 8. avant — Avant

| Field | Value |
|---|---|
| CIK in file | `0001620459` |
| EDGAR name at that CIK | James River Group Holdings, Inc. (unrelated insurer) |
| Status | **FAIL — Wrong CIK** |
| Actual ABS-15G filers | Avant Credit III LLC / AVANT DEPOSITOR II LLC |
| Best depositor CIK (primary, most ABS-15G) | `0001624523` (Avant Credit III LLC, 9 filings) |
| Secondary depositor CIK | `0002029130` (AVANT DEPOSITOR II LLC, 8 filings) |

**Action:** Replace CIK `0001620459` → `0001624523` (Avant Credit III LLC). Note: Avant LLC (the private company) does not have its own EDGAR CIK as a registrant; the ABS-15G filer is the depositor entity. `0001624523` is the primary depositor with the most ABS-15G history.

---

### 9. marlette — Marlette Funding (Best Egg)

| Field | Value |
|---|---|
| CIK in file | `0001649989` |
| EDGAR name at that CIK | Outlook Therapeutics, Inc. (unrelated biopharma) |
| Status | **FAIL — Wrong CIK** |
| Actual ABS-15G filer | Marlette Funding Depositor Trust |
| Correct CIK | `0001678811` |
| ABS-15G filings at correct CIK | 37 |

**Action:** Replace CIK `0001649989` → `0001678811` (Marlette Funding Depositor Trust).

---

### 10. greensky — GreenSky

| Field | Value |
|---|---|
| CIK in file | `0001712518` |
| EDGAR name at that CIK | Flight Of The Guineas, LLC (unrelated) |
| Status | **FAIL — Wrong CIK** |
| Correct ABS-15G filer | GreenSky, LLC |
| Correct CIK | `0001472649` |
| ABS-15G filings at correct CIK | 3 |

**Action:** Replace CIK `0001712518` → `0001472649` (GreenSky, LLC). Note: GreenSky, Inc. (the former public holding company, CIK `0001712923`) has 0 ABS-15G filings; the LLC entity is the correct ABS filer.

---

### 11. funding_circle — Funding Circle / Lendio

| Field | Value |
|---|---|
| CIK in file | `0001780530` |
| EDGAR name at that CIK | Bridgeway Wellness Group, LLC (unrelated) |
| Status | **WARN — Wrong CIK, limited ABS-15G history** |
| Correct ABS-15G filer | FC Marketplace, LLC |
| Correct CIK | `0001783754` |
| ABS-15G filings at correct CIK | 10 |

**Action:** Replace CIK `0001780530` → `0001783754` (FC Marketplace, LLC). Note: "Funding Circle / Lendio" is an ambiguous label — FC Marketplace LLC is the Funding Circle US ABS depositor entity. The Lendio connection should be clarified separately.

---

## Corrections Summary

| Key | Old CIK | Old EDGAR Name | New CIK | Correct EDGAR Name | ABS-15G |
|---|---|---|---|---|---|
| pagaya | `0001883944` | Kowalewski Eric | `0001897077` | Pagaya Structured Products LLC | 10 |
| affirm | `0001820201` | Redbox Entertainment Inc. | `0001820953` | Affirm Holdings, Inc. | 0* |
| sofi | `0001818502` | OppFi Inc. | `0001818874` | SoFi Technologies, Inc. | 0* |
| prosper | `0001416635` | Trendiki Inc | `0001705499` | Prosper Depositor LLC | 20 |
| avant | `0001620459` | James River Group Holdings | `0001624523` | Avant Credit III LLC | 9 |
| marlette | `0001649989` | Outlook Therapeutics Inc. | `0001678811` | Marlette Funding Depositor Trust | 37 |
| greensky | `0001712518` | Flight Of The Guineas LLC | `0001472649` | GreenSky, LLC | 3 |
| funding_circle | `0001780530` | Bridgeway Wellness Group LLC | `0001783754` | FC Marketplace, LLC | 10 |

_* Affirm Holdings and SoFi Technologies are the correct parent holding company CIKs; the ABS-15G filings are on their operating subsidiaries (Affirm, Inc. CIK `0001714052` with 36 filings; SoFi Lending Corp. CIK `0001555110` with 106 filings)._

---

## Notes on Entity Alignment

Several issuers in this registry represent public holding companies, but the ABS-15G filer of record is a depositor LLC or trust subsidiary. This is normal ABS market practice. The dashboard should decide whether to track:

- **Parent company CIK** — for issuer identification and corporate linkage
- **Depositor/SPV entity CIK** — for direct ABS-15G filing retrieval

Affected issuers: affirm, sofi, oportun.

---

## Overall Data Quality Score

| Category | Count |
|---|---|
| PASS (CIK correct, name matches, ABS-15G filings present) | 2 (lendingclub, enova) |
| PASS-WARN (CIK correct, name matches, no ABS-15G at parent CIK) | 1 (oportun) |
| FAIL (CIK resolves to completely unrelated entity) | 8 (pagaya, affirm, sofi, prosper, avant, marlette, greensky, funding_circle) |

**Score: 27 / 100**
_(2 full pass × 10pts + 1 warn × 7pts = 27pts)_

After applying corrections: estimated score **82 / 100** (accounting for entity-level alignment notes on affirm, sofi, oportun).

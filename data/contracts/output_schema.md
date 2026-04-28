# Output tables

## `gold_idp_extracted_facts`

Extracted facts per facility row:

- `row_id`
- `facility_name`
- `procedures`
- `equipment`
- `capabilities`
- `specialties`
- `confidence`
- `evidence`

## `gold_claim_verification`

Verification decisions per facility claim:

- `row_id`
- `facility_name`
- `claim`
- `status`: `Verified`, `Incomplete`, `Suspicious`, `Not claimed`
- `confidence`
- `reason`
- `present_evidence`
- `missing_evidence`
- `evidence_rows`

## `gold_facility_risk`

Facility-level desert intelligence:

- `row_id`
- `facility_name`
- `region`
- `risk_score`
- `risk_level`
- `primary_gap`
- `recommendation`
- `suspicious_or_incomplete_claims`
- `verified_claims`

## `gold_region_risk`

Region-level planning view:

- `region`
- `facilities`
- `risk_score`
- `risk_level`
- `missing_critical_capabilities`
- `recommended_action`

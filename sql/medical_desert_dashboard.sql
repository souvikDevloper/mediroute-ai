-- Databricks SQL snippets for a dashboard / Genie-style exploration.
-- Replace main.mediroute_ai with your catalog.schema.

-- 1. Highest-risk regions
SELECT
  region,
  facilities,
  risk_score,
  risk_level,
  missing_critical_capabilities,
  recommended_action
FROM main.mediroute_ai.gold_region_risk
ORDER BY risk_score DESC;

-- 2. Facilities needing manual verification
SELECT
  row_id,
  facility_name,
  claim,
  status,
  confidence,
  reason,
  missing_evidence
FROM main.mediroute_ai.gold_claim_verification
WHERE status IN ('Suspicious', 'Incomplete')
ORDER BY status DESC, confidence DESC;

-- 3. Claim status counts
SELECT status, COUNT(*) AS claims
FROM main.mediroute_ai.gold_claim_verification
GROUP BY status
ORDER BY claims DESC;

-- 4. Facility risk leaderboard
SELECT
  facility_name,
  region,
  risk_score,
  risk_level,
  primary_gap,
  recommendation
FROM main.mediroute_ai.gold_facility_risk
ORDER BY risk_score DESC;

-- 5. RAG documents sanity check
SELECT document_id, facility_name, region, LEFT(text, 500) AS preview
FROM main.mediroute_ai.gold_rag_documents
LIMIT 20;

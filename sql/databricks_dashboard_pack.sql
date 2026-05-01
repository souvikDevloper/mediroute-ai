-- MediRoute AI Databricks SQL Dashboard Pack
-- Catalog/schema used during the hackathon run: workspace.mediroute_ai

-- 1. High-risk source-derived location clusters
SELECT
  region,
  facilities,
  risk_score,
  risk_level,
  missing_critical_capabilities,
  recommended_action
FROM workspace.mediroute_ai.gold_region_risk
ORDER BY risk_score DESC
LIMIT 20;

-- 2. Claim status distribution
SELECT
  status,
  COUNT(*) AS claims
FROM workspace.mediroute_ai.gold_claim_verification
GROUP BY status
ORDER BY claims DESC;

-- 3. High-risk facilities
SELECT
  facility_name,
  region,
  risk_score,
  risk_level,
  primary_gap,
  recommendation
FROM workspace.mediroute_ai.gold_facility_risk
WHERE risk_level IN ('Critical', 'High')
ORDER BY risk_score DESC
LIMIT 50;

-- 4. Missing capability frequency
SELECT
  claim,
  status,
  COUNT(*) AS count
FROM workspace.mediroute_ai.gold_claim_verification
WHERE status IN ('Suspicious', 'Incomplete')
GROUP BY claim, status
ORDER BY count DESC;

-- 5. Quality summary
SELECT *
FROM workspace.mediroute_ai.gold_quality_summary;

-- 6. RAG document coverage
SELECT COUNT(*) AS rag_documents
FROM workspace.mediroute_ai.gold_rag_documents;

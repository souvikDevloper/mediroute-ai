# Final Build Notes — MediRoute AI

## Included official assets

- Official dataset: `data/official/virtue_foundation_ghana_v0_3.csv`
- Official schema: `docs/official_schema_documentation.txt`
- Reference prompts/Pydantic models: `prompts_reference/prompts_and_pydantic_models/`

## Final product loop

```text
Official VF Ghana CSV
→ Bronze Delta ingestion
→ Silver normalization
→ IDP extraction
→ Claim verification
→ Medical desert scoring
→ RAG document preparation
→ MLflow trace
→ Streamlit / Databricks App dashboard
```

## Judge-facing differentiators

1. Uses official VF Ghana data rather than mock data.
2. Handles JSON-list columns from the official schema.
3. Adds offline Ghana geocoding so the map works even when lat/lon are missing.
4. Gives row-level evidence and verification trace for each claim.
5. Includes Databricks notebooks, Asset Bundle config, Delta table design, MLflow trace, and Vector Search-ready RAG documents.

## Recommended demo path

1. Show Databricks notebook pipeline writing Bronze/Silver/Gold Delta tables.
2. Show MLflow metrics/artifacts.
3. Open the app and show Overview.
4. Click Medical Desert Map.
5. Ask: `Which facilities need manual verification?`
6. Show Evidence Trace.
7. Close with how this helps NGOs prioritize verification, doctors, oxygen, ICU, surgery, and maternal-care interventions.

# MediRoute AI V2 — Databricks-ready build plan

## Product loop

MediRoute AI follows one clear judge-friendly loop:

```text
Extract -> Verify -> Score -> Recommend -> Cite
```

## V2 improvements over V1

| Area | V1 | V2 |
|---|---|---|
| Data | Local CSV | Bronze/Silver/Gold Delta tables |
| Traceability | Local evidence tables | MLflow run with metrics + artifacts |
| RAG | Local planner logic | `gold_rag_documents` table ready for Vector Search |
| Deployment | Streamlit local | Databricks notebooks + Asset Bundle + App manifest |
| Judging story | Working demo | Databricks-native Lakehouse intelligence system |

## Databricks pipeline

```text
Raw CSV
  -> bronze_facility_raw
  -> silver_facility_clean
  -> gold_idp_extracted_facts
  -> gold_claim_verification
  -> gold_facility_risk
  -> gold_region_risk
  -> gold_facility_intelligence
  -> gold_rag_documents
  -> MLflow trace artifacts
```

## Why this wins

The hackathon asks for an IDP agent that extracts medical data from free-form fields, synthesizes with structured facility schemas, identifies medical deserts, flags suspicious claims, and supports accessible planning. V2 does exactly that with Databricks as the backbone.

## What to show judges

1. Databricks notebook pipeline running.
2. Delta tables created in catalog/schema.
3. MLflow run with metrics and artifacts.
4. Streamlit app showing map, facility intelligence, Ask Agent, and evidence trace.
5. GitHub repo with deployment instructions.

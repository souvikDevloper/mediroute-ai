# Databricks setup for MediRoute AI

This folder makes the project Databricks-ready for the Accenture + Databricks hackathon.

## What Databricks does in this project

MediRoute AI uses Databricks as the Lakehouse backbone:

1. **Bronze Delta table** for raw facility records.
2. **Silver Delta table** for normalized healthcare facility data.
3. **Gold Delta tables** for extracted IDP facts, claim verification, facility risk, region risk, and RAG documents.
4. **MLflow** to log extraction/verification/scoring metrics and trace artifacts.
5. **Vector Search preparation table** for retrieval-augmented planner answers.
6. **Dashboard export** for Streamlit or Databricks Apps.

## Manual notebook run order

Upload this repo to Databricks Repos or Workspace Files, then run:

```text
00_setup_config.py
01_bronze_ingest_delta.py
02_silver_normalize.py
03_gold_agent_pipeline.py
04_mlflow_agent_trace.py
05_vector_search_prep.py
06_quality_checks.py
07_export_for_app.py
```

Use these widgets:

```text
catalog = main
schema = mediroute_ai
source_path = /Workspace/Users/<your-email>/mediroute-ai/data/official/virtue_foundation_ghana_v0_3.csv
```

For the official hackathon data, replace `source_path` with the CSV path you upload to Databricks.

## Asset Bundle deployment

If the Databricks CLI is configured:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run mediroute_ai_pipeline -t dev
```

The job definition is in `resources/mediroute_job.yml`.

## Tables created

```text
bronze_facility_raw
silver_facility_clean
gold_idp_extracted_facts
gold_claim_verification
gold_facility_risk
gold_region_risk
gold_facility_intelligence
gold_rag_documents
gold_quality_summary
gold_quality_claim_status
```

## Demo talking point

Say this in the demo:

> We used Databricks to build a reproducible Lakehouse pipeline. Raw Virtue Foundation facility records are ingested into Bronze Delta, normalized into Silver, transformed into Gold healthcare intelligence tables, and logged with MLflow so every agent step can be audited with row-level evidence.

# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — MLflow Agent Trace
# MAGIC Logs pipeline metrics, extracted facts, verification decisions, and region-risk artifacts.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

import json
import tempfile
from pathlib import Path

import mlflow
import pandas as pd

try:
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "mediroute_ai")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

mlflow.set_experiment("/Shared/MediRoute-AI-Agent-Traces")

regions = spark.table("gold_region_risk").toPandas()
facilities = spark.table("gold_facility_risk").toPandas()
verification = spark.table("gold_claim_verification").toPandas()
extracted = spark.table("gold_idp_extracted_facts").toPandas()

with mlflow.start_run(run_name="mediroute-idp-verify-score") as run:
    mlflow.log_param("catalog", catalog)
    mlflow.log_param("schema", schema)
    mlflow.log_param("pipeline", "Extract -> Verify -> Score -> Recommend -> Cite")
    mlflow.log_metric("facilities", len(facilities))
    mlflow.log_metric("regions", len(regions))
    mlflow.log_metric("verified_claims", int((verification["status"] == "Verified").sum()))
    mlflow.log_metric("weak_claims", int(verification["status"].isin(["Suspicious", "Incomplete"]).sum()))
    mlflow.log_metric("critical_regions", int((regions["risk_level"] == "Critical").sum()))
    mlflow.log_metric("critical_high_facilities", int(facilities["risk_level"].isin(["Critical", "High"]).sum()))

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        regions.to_csv(root / "region_risk.csv", index=False)
        facilities.to_csv(root / "facility_risk.csv", index=False)
        verification.to_csv(root / "verification_trace.csv", index=False)
        extracted.to_csv(root / "extracted_facts.csv", index=False)
        summary = {
            "judging_alignment": {
                "technical_accuracy": "rule-based verification plus confidence and row evidence",
                "idp_innovation": "schema-guided extraction of procedure/equipment/capability/specialty",
                "social_impact": "medical desert risk and NGO action recommendations",
                "ux": "natural-language planner and dashboard",
            },
            "tables": ["gold_region_risk", "gold_facility_risk", "gold_claim_verification", "gold_idp_extracted_facts"],
        }
        (root / "agent_trace_summary.json").write_text(json.dumps(summary, indent=2))
        mlflow.log_artifacts(str(root), artifact_path="mediroute_trace")

print(f"MLflow run logged: {run.info.run_id}")

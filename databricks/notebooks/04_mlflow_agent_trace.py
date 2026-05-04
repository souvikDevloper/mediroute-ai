# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — MLflow Agent Trace
# MAGIC Logs pipeline metrics, extracted facts, verification decisions, and region-risk artifacts.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "mediroute_ai")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

import os
import json
import tempfile
from pathlib import Path

import pandas as pd

regions = spark.table("gold_region_risk").toPandas()
facilities = spark.table("gold_facility_risk").toPandas()
verification = spark.table("gold_claim_verification").toPandas()
extracted = spark.table("gold_idp_extracted_facts").toPandas()

metrics = {
    "facilities": int(len(facilities)),
    "regions": int(len(regions)),
    "extracted_evidence_facts": int(len(extracted)),
    "verification_claims": int(len(verification)),
    "verified_claims": int((verification["status"] == "Verified").sum()),
    "weak_claims": int(verification["status"].isin(["Suspicious", "Incomplete"]).sum()),
    "critical_regions": int((regions["risk_level"] == "Critical").sum()),
    "critical_high_facilities": int(facilities["risk_level"].isin(["Critical", "High"]).sum()),
}

trace_steps = [
    {
        "step": 1,
        "agent_step": "Bronze ingestion",
        "description": "Loaded official Virtue Foundation Ghana facility data into a Bronze Delta table.",
        "output_table": "bronze_facility_raw",
    },
    {
        "step": 2,
        "agent_step": "Silver normalization",
        "description": "Normalized facility names, location fields, schema fields, and combined messy medical notes.",
        "output_table": "silver_facility_clean",
    },
    {
        "step": 3,
        "agent_step": "IDP extraction",
        "description": "Extracted procedures, equipment, capabilities, and specialties with row-level evidence.",
        "output_table": "gold_idp_extracted_facts",
    },
    {
        "step": 4,
        "agent_step": "Claim verification",
        "description": "Verified or flagged suspicious facility claims using supporting and missing evidence.",
        "output_table": "gold_claim_verification",
    },
    {
        "step": 5,
        "agent_step": "Medical desert scoring",
        "description": "Scored facilities and regions by weak claims, missing capabilities, staffing, and capacity.",
        "output_table": "gold_facility_risk / gold_region_risk",
    },
    {
        "step": 6,
        "agent_step": "Planner-ready intelligence",
        "description": "Produced Databricks Gold tables for dashboard, planner queries, citations, and map visualization.",
        "output_table": "gold_facility_intelligence",
    },
]

trace_summary = {
    "project": "MediRoute AI",
    "pipeline": "Extract -> Verify -> Score -> Recommend -> Cite",
    "dataset": "Virtue Foundation Ghana v0.3",
    "judging_alignment": {
        "technical_accuracy": "Rule-based verification plus confidence scores and row-level evidence.",
        "idp_innovation": "Schema-guided extraction of procedure, equipment, capability, and specialty facts.",
        "social_impact": "Medical desert risk scoring and NGO action recommendations.",
        "user_experience": "Dashboard and planner interface for non-technical NGO users.",
    },
    "metrics": metrics,
    "trace_steps": trace_steps,
}

# 1. Always save trace into Delta table first
trace_rows = []

for s in trace_steps:
    trace_rows.append({
        "trace_type": "agent_step",
        "name": s["agent_step"],
        "value": s["description"],
        "output_table": s["output_table"],
        "step": int(s["step"]),
    })

for k, v in metrics.items():
    trace_rows.append({
        "trace_type": "metric",
        "name": k,
        "value": str(v),
        "output_table": "",
        "step": 0,
    })

trace_df = pd.DataFrame(trace_rows)

spark.createDataFrame(trace_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_agent_trace_summary")

print("Delta trace table created successfully: gold_agent_trace_summary")

# 2. Try MLflow, but don't block the project if Free Edition serverless rejects it
mlflow_ok = False
mlflow_error = ""

try:
    import mlflow

    try:
        mlflow.set_tracking_uri("databricks")
    except Exception:
        pass

    try:
        mlflow.set_registry_uri("databricks-uc")
    except Exception:
        pass

    user_email = spark.sql("SELECT current_user()").collect()[0][0]
    experiment_path = f"/Users/{user_email}/mediroute-ai-agent-trace"

    mlflow.set_experiment(experiment_path)

    with mlflow.start_run(run_name="mediroute-idp-verify-score") as run:
        mlflow.log_param("catalog", catalog)
        mlflow.log_param("schema", schema)
        mlflow.log_param("pipeline", "Extract -> Verify -> Score -> Recommend -> Cite")
        mlflow.log_param("dataset", "Virtue Foundation Ghana v0.3")

        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)

            (root / "agent_trace_summary.json").write_text(json.dumps(trace_summary, indent=2))

            regions.to_csv(root / "region_risk.csv", index=False)
            facilities.to_csv(root / "facility_risk.csv", index=False)
            verification.to_csv(root / "verification_trace.csv", index=False)
            extracted.to_csv(root / "extracted_facts.csv", index=False)

            mlflow.log_artifacts(str(root), artifact_path="mediroute_trace")

        run_id = run.info.run_id
        mlflow_ok = True

except Exception as e:
    mlflow_error = str(e)

print("Trace metrics:")
for k, v in metrics.items():
    print(f"- {k}: {v}")

if mlflow_ok:
    print("MLflow agent trace logged successfully")
    print(f"Run ID: {run_id}")
else:
    print("MLflow logging skipped/failed in this Free Edition Serverless environment.")
    print("This is okay because the Databricks Delta trace table was created successfully.")
    print(f"MLflow error: {mlflow_error[:500]}")

display(spark.table("gold_agent_trace_summary"))

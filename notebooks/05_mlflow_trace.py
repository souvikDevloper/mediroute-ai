# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — MLflow Agent Trace
# MAGIC Logs extraction, verification, scoring, and planner artifacts for transparent judging.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import json
import tempfile
from pathlib import Path
import mlflow

# COMMAND ----------

dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "default")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

region = spark.table(f"{catalog}.{schema}.gold_region_risk").toPandas()
verify = spark.table(f"{catalog}.{schema}.gold_claim_verification").toPandas()
facilities = spark.table(f"{catalog}.{schema}.gold_facility_intelligence").toPandas()

with mlflow.start_run(run_name="mediroute_agent_trace"):
    mlflow.log_metric("facilities", len(facilities))
    mlflow.log_metric("regions", region["region"].nunique())
    mlflow.log_metric("weak_claims", int(verify["status"].isin(["Suspicious", "Incomplete"]).sum()))
    mlflow.log_metric("verified_claims", int((verify["status"] == "Verified").sum()))
    mlflow.log_param("agent_loop", "Extract -> Verify -> Score -> Recommend -> Cite")

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        region.to_csv(p / "region_risk.csv", index=False)
        verify.to_csv(p / "claim_verification.csv", index=False)
        facilities.to_csv(p / "facility_intelligence.csv", index=False)
        (p / "trace_summary.json").write_text(json.dumps({
            "step_1": "IDP extraction from unstructured facility notes",
            "step_2": "Capability claim verification using evidence rules",
            "step_3": "Facility and regional medical desert scoring",
            "step_4": "Planner answer generation with row-level citations",
        }, indent=2))
        mlflow.log_artifacts(tmp, artifact_path="mediroute_trace")

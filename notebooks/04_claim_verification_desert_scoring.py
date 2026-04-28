# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Verify Claims + Score Medical Deserts
# MAGIC Creates Gold tables for facility intelligence and regional risk.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import sys, json
import pandas as pd
sys.path.append("../src")
from mediroute.verifier import verify_dataframe
from mediroute.scoring import facility_risk, region_risk

# COMMAND ----------

dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "default")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

raw = spark.table(f"{catalog}.{schema}.silver_facility_clean").toPandas()
ext = spark.table(f"{catalog}.{schema}.silver_idp_extractions").toPandas()

# Parse JSON columns back to lists.
for col in ["procedures", "equipment", "capabilities", "specialties", "evidence"]:
    ext[col] = ext[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x else [])

verification = verify_dataframe(ext)
facilities = facility_risk(ext, verification, raw)
regions = region_risk(facilities, ext)
gold = raw.merge(ext, on=["row_id", "facility_name"], how="left").merge(
    facilities[["row_id", "risk_score", "risk_level", "primary_gap", "recommendation", "suspicious_or_incomplete_claims", "verified_claims"]],
    on="row_id",
    how="left",
)

for name, df in {
    "gold_facility_intelligence": gold,
    "gold_claim_verification": verification,
    "gold_facility_risk": facilities,
    "gold_region_risk": regions,
}.items():
    out = df.copy()
    for c in out.columns:
        if out[c].map(lambda v: isinstance(v, (list, dict))).any():
            out[c] = out[c].apply(lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v)
    spark.createDataFrame(out).write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.{name}")

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.gold_region_risk"))

# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Agent Pipeline
# MAGIC Runs IDP extraction, claim verification, risk scoring, and writes all gold tables.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

import os, sys, json
sys.path.append(os.path.abspath("../../src"))

try:
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "mediroute_ai")
    dbutils.widgets.dropdown("use_llm", "false", ["false", "true"])
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
use_llm = dbutils.widgets.get("use_llm").lower() == "true"
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

from mediroute.pipeline import process_dataframe, _serializable_frame

silver = spark.table("silver_facility_clean").toPandas()
outputs = process_dataframe(silver, use_llm=use_llm)

for name, frame in outputs.items():
    out = _serializable_frame(frame)
    table_name = {
        "raw": "silver_facility_clean_export",
        "extracted": "gold_idp_extracted_facts",
        "verification": "gold_claim_verification",
        "facilities": "gold_facility_risk",
        "regions": "gold_region_risk",
        "gold": "gold_facility_intelligence",
    }[name]
    spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable(table_name)
    print(f"Wrote {table_name}: {len(out)} rows")

display(spark.table("gold_region_risk").orderBy("risk_score", ascending=False))

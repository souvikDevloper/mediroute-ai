# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Export Gold Tables for Streamlit Demo
# MAGIC Use this if your Streamlit app is running outside Databricks.

# COMMAND ----------

dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("output_dir", "dbfs:/FileStore/mediroute_exports")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
out = dbutils.widgets.get("output_dir")

tables = [
    "gold_facility_intelligence",
    "gold_claim_verification",
    "gold_facility_risk",
    "gold_region_risk",
]

for t in tables:
    df = spark.table(f"{catalog}.{schema}.{t}")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/{t}")
    print(f"Exported {t} to {out}/{t}")

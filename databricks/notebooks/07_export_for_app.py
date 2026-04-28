# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Export for Streamlit / Databricks App
# MAGIC Exports gold tables as CSV files that the Streamlit app can read.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "mediroute_ai")
    dbutils.widgets.text("export_path", "dbfs:/FileStore/mediroute-ai/processed")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
export_path = dbutils.widgets.get("export_path")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

table_map = {
    "raw": "silver_facility_clean",
    "extracted": "gold_idp_extracted_facts",
    "verification": "gold_claim_verification",
    "facilities": "gold_facility_risk",
    "regions": "gold_region_risk",
    "gold": "gold_facility_intelligence",
}

for filename, table in table_map.items():
    out = f"{export_path}/{filename}"
    spark.table(table).coalesce(1).write.mode("overwrite").option("header", True).csv(out)
    print(f"Exported {table} -> {out}")

print("For local demo, download/rename the part files to data/processed/*.csv, or point the app to Databricks SQL in a future deployment.")

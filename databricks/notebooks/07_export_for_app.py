# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Export for Streamlit / Databricks App
# MAGIC Exports gold tables as CSV files that the Streamlit app can read.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "mediroute_ai")
    dbutils.widgets.text("export_path", "/Volumes/workspace/mediroute_ai/raw/processed")
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
    "rag_documents": "gold_rag_documents",
    "quality_summary": "gold_quality_summary",
    "quality_checks": "gold_quality_checks",
}

for filename, table in table_map.items():
    out = f"{export_path}/{filename}"
    spark.table(table).coalesce(1).write.mode("overwrite").option("header", True).csv(out)
    print(f"Exported {table} -> {out}")

print("Export completed successfully.")
print(f"Files written under: {export_path}")
display(dbutils.fs.ls(export_path))
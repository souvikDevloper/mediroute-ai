# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Quality Checks
# MAGIC Produces judge-friendly validation numbers and fails if core tables are empty.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "mediroute_ai")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

required_tables = [
    "bronze_facility_raw",
    "silver_facility_clean",
    "gold_idp_extracted_facts",
    "gold_claim_verification",
    "gold_facility_risk",
    "gold_region_risk",
    "gold_facility_intelligence",
]

for table in required_tables:
    count = spark.table(table).count()
    print(f"{table}: {count} rows")
    assert count > 0, f"{table} is empty"

verification = spark.table("gold_claim_verification")
quality = verification.groupBy("status").count().orderBy("status")
quality.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_quality_claim_status")

spark.sql("""
CREATE OR REPLACE TABLE gold_quality_summary AS
SELECT
  (SELECT COUNT(*) FROM gold_facility_intelligence) AS facilities,
  (SELECT COUNT(*) FROM gold_region_risk) AS regions,
  (SELECT COUNT(*) FROM gold_claim_verification WHERE status='Verified') AS verified_claims,
  (SELECT COUNT(*) FROM gold_claim_verification WHERE status IN ('Suspicious', 'Incomplete')) AS weak_claims,
  (SELECT COUNT(*) FROM gold_facility_risk WHERE risk_level IN ('Critical', 'High')) AS critical_high_facilities
""")

display(spark.table("gold_quality_summary"))
display(spark.table("gold_quality_claim_status"))

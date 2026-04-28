# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Normalize
# MAGIC Normalizes column names and schema into `silver_facility_clean`.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../../src"))

try:
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "mediroute_ai")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

from mediroute.pipeline import normalize_columns

pdf = spark.table("bronze_facility_raw").toPandas()
clean = normalize_columns(pdf)

spark.createDataFrame(clean).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("silver_facility_clean")

spark.sql("SELECT COUNT(*) AS rows FROM silver_facility_clean").show()
display(spark.table("silver_facility_clean").limit(10))

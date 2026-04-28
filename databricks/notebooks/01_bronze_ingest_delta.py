# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingest
# MAGIC Ingests facility CSV into a Delta table: `bronze_facility_raw`.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "mediroute_ai")
    dbutils.widgets.text("source_path", "data/official/virtue_foundation_ghana_v0_3.csv")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# For Databricks Workspace/Volume paths, pass the full path in the widget.
# For bundle/local sample data, use the default repo-relative path.
import os
from pathlib import Path

candidate_paths = [
    source_path,
    f"file:{source_path}" if not source_path.startswith(("dbfs:", "/", "s3:", "abfss:")) else source_path,
]

last_error = None
for p in candidate_paths:
    try:
        df = spark.read.option("header", True).option("inferSchema", True).csv(p)
        if len(df.columns) > 0:
            break
    except Exception as e:
        last_error = e
else:
    raise RuntimeError(f"Could not read source_path={source_path}. Last error: {last_error}")

df = df.withColumnRenamed(df.columns[0], df.columns[0].strip())
df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("bronze_facility_raw")

spark.sql("SELECT COUNT(*) AS rows FROM bronze_facility_raw").show()
display(spark.table("bronze_facility_raw").limit(10))

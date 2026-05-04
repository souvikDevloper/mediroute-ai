# Databricks notebook source
# MAGIC %md
# MAGIC # MediRoute AI — Setup
# MAGIC Creates the catalog/schema used by the Lakehouse pipeline.
# MAGIC
# MAGIC Run order:
# MAGIC 1. `00_setup_config`
# MAGIC 2. `01_bronze_ingest_delta`
# MAGIC 3. `02_silver_normalize`
# MAGIC 4. `03_gold_agent_pipeline`
# MAGIC 5. `04_mlflow_agent_trace`
# MAGIC 6. `05_vector_search_prep`
# MAGIC 7. `06_quality_checks`
# MAGIC 8. `07_export_for_app`

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

import os
import sys
from pathlib import Path

try:
    dbutils.widgets.text("catalog", "main")
    dbutils.widgets.text("schema", "mediroute_ai")
    dbutils.widgets.text("source_path", "/Volumes/workspace/mediroute_ai/raw/Virtue Foundation Ghana v0.3 - Sheet1.csv")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "main"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "mediroute_ai"
source_path = dbutils.widgets.get("source_path") if "dbutils" in globals() else "/Volumes/workspace/mediroute_ai/raw/Virtue Foundation Ghana v0.3 - Sheet1.csv"

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")
print(f"Source:  {source_path}")

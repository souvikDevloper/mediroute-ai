# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Clean + Normalize Facility Records
# MAGIC Standardizes columns and saves the Silver Delta table.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import sys
sys.path.append("../src")
from mediroute.pipeline import normalize_columns

# COMMAND ----------

dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "default")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

raw = spark.table(f"{catalog}.{schema}.bronze_facility_raw").toPandas()
clean = normalize_columns(raw)
clean_sdf = spark.createDataFrame(clean)
clean_sdf.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.silver_facility_clean")

display(clean_sdf)

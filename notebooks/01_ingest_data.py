# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Facility Data
# MAGIC Creates a Bronze Delta table from the Virtue Foundation / Ghana facility dataset.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

from pathlib import Path
import pandas as pd

# Update this path after uploading the official dataset to Databricks Files/Volumes.
dbutils.widgets.text("input_csv", "data/official/virtue_foundation_ghana_v0_3.csv")
dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "default")

input_csv = dbutils.widgets.get("input_csv")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
raw_df = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)
raw_df.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.bronze_facility_raw")

display(raw_df)

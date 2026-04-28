# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — IDP Extraction Agent
# MAGIC Extracts procedures, equipment, capabilities, specialties, and row-level evidence.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt

# COMMAND ----------

import sys, json
sys.path.append("../src")
from mediroute.extractor import IDPExtractor, extraction_to_frame

# COMMAND ----------

dbutils.widgets.text("catalog", "hive_metastore")
dbutils.widgets.text("schema", "default")
dbutils.widgets.dropdown("use_databricks_llm", "false", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
use_llm = dbutils.widgets.get("use_databricks_llm") == "true"

clean = spark.table(f"{catalog}.{schema}.silver_facility_clean").toPandas()
extractor = IDPExtractor(use_llm=use_llm)
extracted = extraction_to_frame(extractor.extract_dataframe(clean))

# Convert list/dict columns before Spark write.
for col in ["procedures", "equipment", "capabilities", "specialties", "evidence"]:
    extracted[col] = extracted[col].apply(lambda x: json.dumps(x, ensure_ascii=False))

sdf = spark.createDataFrame(extracted)
sdf.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.silver_idp_extractions")
display(sdf)

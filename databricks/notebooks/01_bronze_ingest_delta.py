# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingest
# MAGIC Ingests facility CSV into a Delta table: `bronze_facility_raw`.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "mediroute_ai")
    dbutils.widgets.text(
        "source_path",
        "/Volumes/workspace/mediroute_ai/raw/Virtue Foundation Ghana v0.3 - Sheet1.csv"
    )
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")

# Use selected catalog/schema
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

import re

def clean_col_name(c):
    c = str(c).strip().lower()
    c = re.sub(r"[^a-zA-Z0-9_]", "_", c)   # replace spaces/dots/special chars
    c = re.sub(r"_+", "_", c)              # collapse repeated underscores
    c = c.strip("_")

    if not c:
        c = "col"

    if c[0].isdigit():
        c = "c_" + c

    return c

candidate_paths = [
    source_path,
    f"file:{source_path}" if not source_path.startswith(("dbfs:", "/", "s3:", "abfss:")) else source_path,
]

last_error = None

for p in candidate_paths:
    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(p)
        )
        if len(df.columns) > 0:
            break
    except Exception as e:
        last_error = e
else:
    raise RuntimeError(f"Could not read source_path={source_path}. Last error: {last_error}")

# Clean all column names for Delta compatibility
new_cols = []
seen = {}

for c in df.columns:
    base = clean_col_name(c)

    if base in seen:
        seen[base] += 1
        base = f"{base}_{seen[base]}"
    else:
        seen[base] = 0

    new_cols.append(base)

df = df.toDF(*new_cols)

# Save Bronze Delta table
df.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("bronze_facility_raw")

print("Bronze table created successfully")
print(f"Rows loaded: {df.count()}")
print(f"Columns loaded: {len(df.columns)}")

display(spark.table("bronze_facility_raw").limit(10))
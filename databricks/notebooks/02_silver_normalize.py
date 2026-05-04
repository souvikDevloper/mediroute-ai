# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Normalize
# MAGIC Normalizes column names and schema into `silver_facility_clean`.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "mediroute_ai")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

from pyspark.sql import functions as F

bronze = spark.table("bronze_facility_raw")

def col_or_lit(df, c, default=""):
    if c in df.columns:
        return F.col(c)
    return F.lit(default)

def safe_int(df, c):
    """
    Handles real null, string 'null', empty string, and malformed values.
    """
    if c in df.columns:
        return F.coalesce(F.expr(f"try_cast(`{c}` as int)"), F.lit(0))
    return F.lit(0)

silver = (
    bronze
    .withColumn("row_id", (F.monotonically_increasing_id() + F.lit(1)).cast("long"))
    .withColumn("facility_name", F.coalesce(col_or_lit(bronze, "name"), F.lit("Unknown Facility")))
    .withColumn("source_url", col_or_lit(bronze, "source_url"))
    .withColumn("city", col_or_lit(bronze, "address_city"))
    .withColumn("region", col_or_lit(bronze, "address_stateorregion"))
    .withColumn("country", col_or_lit(bronze, "address_country", "Ghana"))
    .withColumn("country_code", col_or_lit(bronze, "address_countrycode", "GH"))
    .withColumn("facility_type", col_or_lit(bronze, "facilitytypeid"))
    .withColumn("operator_type", col_or_lit(bronze, "operatortypeid"))
    .withColumn("specialties_raw", col_or_lit(bronze, "specialties"))
    .withColumn("procedure_raw", col_or_lit(bronze, "procedure"))
    .withColumn("equipment_raw", col_or_lit(bronze, "equipment"))
    .withColumn("capability_raw", col_or_lit(bronze, "capability"))
    .withColumn("description_raw", col_or_lit(bronze, "description"))
    .withColumn("number_doctors", safe_int(bronze, "numberdoctors"))
    .withColumn("capacity", safe_int(bronze, "capacity"))
)

silver = (
    silver
    .withColumn(
        "facility_name",
        F.when(F.col("facility_name").isNull() | (F.trim(F.col("facility_name")) == ""), F.lit("Unknown Facility"))
        .otherwise(F.trim(F.col("facility_name")))
    )
    .withColumn(
        "region",
        F.when(F.col("region").isNull() | (F.trim(F.col("region")) == "") | (F.lower(F.trim(F.col("region"))) == "null"), F.lit("Unknown Region"))
        .otherwise(F.trim(F.col("region")))
    )
    .withColumn(
        "city",
        F.when(F.col("city").isNull() | (F.trim(F.col("city")) == "") | (F.lower(F.trim(F.col("city"))) == "null"), F.lit("Unknown City"))
        .otherwise(F.trim(F.col("city")))
    )
    .withColumn(
        "combined_note",
        F.concat_ws(
            "\n",
            F.concat(F.lit("Description: "), F.coalesce(F.col("description_raw").cast("string"), F.lit(""))),
            F.concat(F.lit("Procedures: "), F.coalesce(F.col("procedure_raw").cast("string"), F.lit(""))),
            F.concat(F.lit("Equipment: "), F.coalesce(F.col("equipment_raw").cast("string"), F.lit(""))),
            F.concat(F.lit("Capabilities: "), F.coalesce(F.col("capability_raw").cast("string"), F.lit(""))),
            F.concat(F.lit("Specialties: "), F.coalesce(F.col("specialties_raw").cast("string"), F.lit("")))
        )
    )
)

silver_out = silver.select(
    "row_id",
    "source_url",
    "facility_name",
    "city",
    "region",
    "country",
    "country_code",
    "facility_type",
    "operator_type",
    "number_doctors",
    "capacity",
    "specialties_raw",
    "procedure_raw",
    "equipment_raw",
    "capability_raw",
    "description_raw",
    "combined_note"
)

silver_out.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("silver_facility_clean")

print("Silver table created successfully")
print(f"Rows normalized: {silver_out.count()}")
print(f"Columns: {len(silver_out.columns)}")

display(spark.table("silver_facility_clean").limit(10))
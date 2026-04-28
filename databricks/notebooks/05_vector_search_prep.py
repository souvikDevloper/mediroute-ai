# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Vector Search / RAG Preparation
# MAGIC Builds a row-level retrieval table. If Vector Search is available in your workspace, create an index over this table.

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

gold = spark.table("gold_facility_intelligence")
verification = spark.table("gold_claim_verification")

from pyspark.sql import functions as F

# One retrievable document per facility plus compact verification context.
verif_agg = (
    verification.groupBy("row_id")
    .agg(F.concat_ws(" | ", F.collect_list(F.concat_ws(": ", F.col("claim"), F.col("status"), F.col("reason")))).alias("verification_context"))
)

docs = (
    gold.join(verif_agg, on="row_id", how="left")
    .select(
        F.concat(F.lit("facility-"), F.col("row_id").cast("string")).alias("document_id"),
        F.col("row_id"),
        F.col("facility_name"),
        F.col("region"),
        F.concat_ws(
            "\n",
            F.concat(F.lit("Facility: "), F.col("facility_name")),
            F.concat(F.lit("Region: "), F.col("region")),
            F.concat(F.lit("Notes: "), F.col("notes")),
            F.concat(F.lit("Extracted procedures: "), F.col("procedures")),
            F.concat(F.lit("Extracted equipment: "), F.col("equipment")),
            F.concat(F.lit("Extracted capabilities: "), F.col("capabilities")),
            F.concat(F.lit("Specialties: "), F.col("specialties")),
            F.concat(F.lit("Risk: "), F.col("risk_level"), F.lit(" ("), F.col("risk_score").cast("string"), F.lit(")")),
            F.concat(F.lit("Verification: "), F.coalesce(F.col("verification_context"), F.lit("No verification context")))
        ).alias("text"),
        F.to_json(F.struct("row_id", "facility_name", "region", "risk_score", "risk_level", "primary_gap")).alias("metadata_json"),
    )
)

docs.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_rag_documents")

display(spark.table("gold_rag_documents").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional Vector Search setup
# MAGIC Create an endpoint/index in the Databricks UI or SDK using `gold_rag_documents` as the source table.
# MAGIC Recommended index fields:
# MAGIC - Primary key: `document_id`
# MAGIC - Text column: `text`
# MAGIC - Metadata: `row_id`, `facility_name`, `region`, `metadata_json`

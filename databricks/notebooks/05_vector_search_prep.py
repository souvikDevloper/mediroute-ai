# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Vector Search / RAG Preparation
# MAGIC Builds a row-level retrieval table. If Vector Search is available in your workspace, create an index over this table.

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

gold = spark.table("gold_facility_intelligence")
verification = spark.table("gold_verification_trace")

verif_agg = (
    verification
    .groupBy("row_id")
    .agg(
        F.concat_ws(
            " | ",
            F.collect_list(
                F.concat(
                    F.lit("Claim: "), F.col("claim"),
                    F.lit("; Status: "), F.col("status"),
                    F.lit("; Reason: "), F.col("reason"),
                    F.lit("; Missing evidence: "), F.coalesce(F.col("missing_evidence"), F.lit(""))
                )
            )
        ).alias("verification_context")
    )
)

docs = (
    gold
    .join(verif_agg, on="row_id", how="left")
    .select(
        F.concat(F.lit("facility-"), F.col("row_id").cast("string")).alias("document_id"),
        F.col("row_id"),
        F.col("facility_name"),
        F.col("region"),
        F.col("city"),
        F.col("risk_level"),
        F.col("risk_score"),
        F.col("primary_gap"),
        F.col("recommended_action"),
        F.concat_ws(
            "\n",
            F.concat(F.lit("Facility: "), F.col("facility_name")),
            F.concat(F.lit("Region: "), F.col("region")),
            F.concat(F.lit("City: "), F.col("city")),
            F.concat(F.lit("Risk: "), F.col("risk_level"), F.lit(" ("), F.col("risk_score").cast("string"), F.lit(")")),
            F.concat(F.lit("Primary gap: "), F.col("primary_gap")),
            F.concat(F.lit("Recommended action: "), F.col("recommended_action")),
            F.concat(F.lit("Extracted procedures: "), F.coalesce(F.col("procedures"), F.lit(""))),
            F.concat(F.lit("Extracted equipment: "), F.coalesce(F.col("equipment"), F.lit(""))),
            F.concat(F.lit("Extracted capabilities: "), F.coalesce(F.col("capabilities"), F.lit(""))),
            F.concat(F.lit("Specialties: "), F.coalesce(F.col("specialties"), F.lit(""))),
            F.concat(F.lit("Verification context: "), F.coalesce(F.col("verification_context"), F.lit("No verification context"))),
            F.concat(F.lit("Evidence note: "), F.coalesce(F.col("combined_note"), F.lit("")))
        ).alias("text"),
        F.to_json(
            F.struct(
                "row_id",
                "facility_name",
                "region",
                "city",
                "risk_score",
                "risk_level",
                "primary_gap"
            )
        ).alias("metadata_json")
    )
)

docs.write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_rag_documents")

print("RAG / Vector Search-ready table created successfully")
print(f"Documents created: {docs.count()}")

display(spark.table("gold_rag_documents").limit(10))
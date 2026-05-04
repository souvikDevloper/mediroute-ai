# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Agent Pipeline
# MAGIC Runs IDP extraction, claim verification, risk scoring, and writes all gold tables.

# COMMAND ----------

# MAGIC %pip install -q -r ../../requirements-databricks.txt

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("schema", "mediroute_ai")
    dbutils.widgets.dropdown("use_llm", "false", ["false", "true"])
except Exception:
    pass

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

import re
import ast
import pandas as pd

silver = spark.table("silver_facility_clean").toPandas()

def clean_text(x):
    if pd.isna(x):
        return ""
    x = str(x).strip()
    if x.lower() in ["null", "none", "nan"]:
        return ""
    return x

def parse_items(x):
    x = clean_text(x)
    if not x:
        return []

    try:
        val = ast.literal_eval(x)
        if isinstance(val, list):
            return [clean_text(v) for v in val if clean_text(v)]
        if isinstance(val, str):
            return [clean_text(val)] if clean_text(val) else []
    except Exception:
        pass

    parts = re.split(r";|\n|\|", x)
    return [clean_text(p) for p in parts if clean_text(p)]

def contains_any(text, words):
    text = text.lower()
    return any(w.lower() in text for w in words)

def evidence_present(text, words):
    text = text.lower()
    return [w for w in words if w.lower() in text]

def risk_level(score):
    if score >= 70:
        return "Critical"
    if score >= 45:
        return "High"
    if score >= 25:
        return "Medium"
    return "Low"

def status_from_evidence(signal, found, required):
    if not signal and not found:
        return "Not claimed", 0.0, "No clear claim signal found."

    if len(found) >= max(1, len(required) - 1):
        return "Verified", 0.90, "Claim is strongly supported by extracted evidence."

    if len(found) >= 1:
        return "Incomplete", 0.62, "Claim is partly supported, but key evidence is missing."

    return "Suspicious", 0.40, "Claim signal exists, but supporting evidence is weak or absent."

claim_rules = {
    "Emergency surgical care": {
        "signal": ["surgery", "surgical", "operation", "operating theatre", "operating room", "theatre"],
        "required": ["operating theatre", "anesthesia", "blood bank", "surgery"]
    },
    "Emergency obstetric care": {
        "signal": ["maternity", "obstetric", "delivery", "c-section", "caesarean", "gynecology", "gynaecology"],
        "required": ["c-section", "blood bank", "operating theatre", "obstetric"]
    },
    "ICU-level care": {
        "signal": ["icu", "intensive care", "critical care", "ventilator"],
        "required": ["icu", "oxygen", "monitor", "ventilator"]
    },
    "Emergency response": {
        "signal": ["emergency", "trauma", "ambulance", "triage"],
        "required": ["emergency", "ambulance", "triage", "oxygen"]
    },
    "Imaging diagnostics": {
        "signal": ["x-ray", "xray", "ultrasound", "ct", "mri", "imaging"],
        "required": ["x-ray", "ultrasound", "ct", "mri"]
    },
    "Laboratory diagnostics": {
        "signal": ["laboratory", "lab", "analyzer", "diagnostic test"],
        "required": ["laboratory", "analyzer", "diagnostic"]
    }
}

specialty_keywords = {
    "generalSurgery": ["surgery", "surgical", "operation", "operating theatre"],
    "emergencyMedicine": ["emergency", "trauma", "triage", "ambulance"],
    "gynecologyAndObstetrics": ["maternity", "obstetric", "delivery", "c-section", "caesarean", "gynecology", "gynaecology"],
    "pediatrics": ["pediatric", "paediatric", "children", "child"],
    "cardiology": ["cardiology", "cardiac", "heart"],
    "ophthalmology": ["eye", "ophthalmology", "vision"],
    "dentistry": ["dental", "dentist", "oral"],
    "orthopedicSurgery": ["orthopedic", "orthopaedic", "bone", "fracture"],
    "internalMedicine": ["internal medicine", "medical ward", "physician"],
    "familyMedicine": ["family medicine", "primary care", "general practice"]
}

facility_rows = []
fact_rows = []
verification_rows = []

for _, r in silver.iterrows():
    row_id = int(r.get("row_id", 0))
    facility_name = clean_text(r.get("facility_name", "Unknown Facility")) or "Unknown Facility"
    region = clean_text(r.get("region", "Unknown Region")) or "Unknown Region"
    city = clean_text(r.get("city", "Unknown City")) or "Unknown City"

    procedure_items = parse_items(r.get("procedure_raw", ""))
    equipment_items = parse_items(r.get("equipment_raw", ""))
    capability_items = parse_items(r.get("capability_raw", ""))
    specialty_items = parse_items(r.get("specialties_raw", ""))
    combined_note = clean_text(r.get("combined_note", ""))

    all_text = " ".join([
        combined_note,
        " ".join(procedure_items),
        " ".join(equipment_items),
        " ".join(capability_items),
        " ".join(specialty_items)
    ])

    inferred_specialties = set()

    for sp, kws in specialty_keywords.items():
        if contains_any(all_text, kws):
            inferred_specialties.add(sp)

    for sp in specialty_items:
        sp_clean = clean_text(sp)
        if sp_clean:
            inferred_specialties.add(sp_clean)

    for fact_type, items in [
        ("procedure", procedure_items),
        ("equipment", equipment_items),
        ("capability", capability_items),
    ]:
        for fact in items:
            fact_rows.append({
                "row_id": row_id,
                "facility_name": facility_name,
                "fact_type": fact_type,
                "fact": fact,
                "evidence_snippet": fact[:250]
            })

    for sp in sorted(inferred_specialties):
        fact_rows.append({
            "row_id": row_id,
            "facility_name": facility_name,
            "fact_type": "specialty",
            "fact": sp,
            "evidence_snippet": combined_note[:250]
        })

    weak_claims = 0
    verified_claims = 0
    missing_critical = []

    for claim, rule in claim_rules.items():
        signal = contains_any(all_text, rule["signal"])
        found = evidence_present(all_text, rule["required"])
        missing = [x for x in rule["required"] if x not in found]

        status, conf, reason = status_from_evidence(signal, found, rule["required"])

        if status == "Not claimed":
            continue

        if status in ["Suspicious", "Incomplete"]:
            weak_claims += 1
            missing_critical.append(claim)
        elif status == "Verified":
            verified_claims += 1

        verification_rows.append({
            "row_id": row_id,
            "facility_name": facility_name,
            "region": region,
            "city": city,
            "claim": claim,
            "status": status,
            "confidence": float(conf),
            "supporting_evidence": ", ".join(found),
            "missing_evidence": ", ".join(missing),
            "reason": reason,
            "source_note": combined_note[:500]
        })

    try:
        doctors = int(r.get("number_doctors", 0) or 0)
    except Exception:
        doctors = 0

    try:
        capacity = int(r.get("capacity", 0) or 0)
    except Exception:
        capacity = 0

    risk = 0
    risk += weak_claims * 12
    risk += max(0, 4 - verified_claims) * 5

    if doctors == 0:
        risk += 12
    elif doctors <= 2:
        risk += 7

    if capacity == 0:
        risk += 10
    elif capacity < 20:
        risk += 5

    if len(inferred_specialties) == 0:
        risk += 8

    risk = min(100, risk)

    if missing_critical:
        primary_gap = missing_critical[0]
    elif verified_claims == 0:
        primary_gap = "Insufficient verified capability"
    else:
        primary_gap = "Monitoring recommended"

    if risk >= 70:
        action = "Critical: deploy NGO field team first; validate claims before routing patients or doctors."
    elif risk >= 45:
        action = "High: manually verify claimed services before routing patients or doctors."
    elif risk >= 25:
        action = "Medium: monitor region and schedule lower-priority verification."
    else:
        action = "Low: facility has stronger evidence; keep in planner pool."

    facility_rows.append({
        "row_id": row_id,
        "source_url": clean_text(r.get("source_url", "")),
        "facility_name": facility_name,
        "city": city,
        "region": region,
        "country": clean_text(r.get("country", "Ghana")) or "Ghana",
        "facility_type": clean_text(r.get("facility_type", "")),
        "operator_type": clean_text(r.get("operator_type", "")),
        "number_doctors": doctors,
        "capacity": capacity,
        "procedures": "; ".join(procedure_items),
        "equipment": "; ".join(equipment_items),
        "capabilities": "; ".join(capability_items),
        "specialties": "; ".join(sorted(inferred_specialties)),
        "weak_claims": weak_claims,
        "verified_claims": verified_claims,
        "primary_gap": primary_gap,
        "risk_score": int(risk),
        "risk_level": risk_level(risk),
        "recommended_action": action,
        "combined_note": combined_note[:1000]
    })

facility_df = pd.DataFrame(facility_rows)
facts_df = pd.DataFrame(fact_rows)
verification_df = pd.DataFrame(verification_rows)

if facts_df.empty:
    facts_df = pd.DataFrame(columns=["row_id", "facility_name", "fact_type", "fact", "evidence_snippet"])

if verification_df.empty:
    verification_df = pd.DataFrame(columns=[
        "row_id", "facility_name", "region", "city", "claim", "status",
        "confidence", "supporting_evidence", "missing_evidence", "reason", "source_note"
    ])

region_rows = []

for region, g in facility_df.groupby("region"):
    facilities = len(g)
    avg_risk = float(g["risk_score"].mean()) if facilities else 0
    max_risk = int(g["risk_score"].max()) if facilities else 0
    critical_high = int(g[g["risk_level"].isin(["Critical", "High"])].shape[0])
    weak_claims_sum = int(g["weak_claims"].sum())
    verified_claims_sum = int(g["verified_claims"].sum())

    vg = verification_df[
        (verification_df["region"] == region) &
        (verification_df["status"].isin(["Suspicious", "Incomplete"]))
    ]

    missing_caps = sorted(vg["claim"].dropna().unique().tolist())
    missing_txt = "; ".join(missing_caps[:8])

    region_score = min(100, int(avg_risk + critical_high * 6 + min(20, weak_claims_sum)))

    if region_score >= 70:
        rec = "Deploy NGO field team first; validate high-risk facilities and prioritize emergency care."
    elif region_score >= 45:
        rec = "Plan targeted intervention for missing emergency, surgical, or maternal capabilities."
    elif region_score >= 25:
        rec = "Monitor region and schedule lower-priority verification."
    else:
        rec = "Maintain routine monitoring."

    region_rows.append({
        "region": region,
        "facilities": int(facilities),
        "avg_facility_risk": round(avg_risk, 2),
        "max_facility_risk": int(max_risk),
        "critical_high_facilities": critical_high,
        "weak_claims": weak_claims_sum,
        "verified_claims": verified_claims_sum,
        "risk_score": region_score,
        "risk_level": risk_level(region_score),
        "missing_critical_capabilities": missing_txt,
        "recommended_action": rec
    })

region_df = pd.DataFrame(region_rows).sort_values("risk_score", ascending=False)

# Save main gold tables
spark.createDataFrame(facts_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_extracted_facts")
spark.createDataFrame(facts_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_idp_extracted_facts")

spark.createDataFrame(verification_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_verification_trace")
spark.createDataFrame(verification_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_claim_verification")

spark.createDataFrame(facility_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_facility_intelligence")
spark.createDataFrame(facility_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_facility_risk")

spark.createDataFrame(region_df).write.mode("overwrite").option("overwriteSchema", True).format("delta").saveAsTable("gold_region_risk")

print("Gold agent pipeline completed successfully")
print(f"Facilities scored: {len(facility_df)}")
print(f"Extracted evidence facts: {len(facts_df)}")
print(f"Verification claims: {len(verification_df)}")
print(f"Regions scored: {len(region_df)}")

print("Created tables:")
print("- gold_extracted_facts")
print("- gold_idp_extracted_facts")
print("- gold_verification_trace")
print("- gold_claim_verification")
print("- gold_facility_intelligence")
print("- gold_facility_risk")
print("- gold_region_risk")

display(spark.table("gold_region_risk").orderBy("risk_score", ascending=False).limit(20))
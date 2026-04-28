# 5-minute demo script

## 0:00–0:30 — Problem
Healthcare expertise exists, but fragmented facility data makes it hard for NGOs to know where care truly exists. A facility may mention emergency or surgical services, but the evidence may be incomplete.

## 0:30–1:00 — Product
MediRoute AI is a Databricks-ready medical desert intelligence system. It uses the official Virtue Foundation Ghana dataset to extract, verify, score, recommend, and cite.

## 1:00–1:45 — Databricks pipeline
Show the notebook/job flow:

```text
Bronze raw table → Silver normalized table → Gold extraction → Gold verification → Gold risk tables → MLflow trace → RAG documents
```

Mention that the pipeline writes Delta tables and logs metrics/artifacts through MLflow.

## 1:45–2:30 — Dashboard overview
Open the app. Show:

- Facilities processed
- Regions/groups scored
- Weak claims
- Verified claims
- Highest-risk regions and facilities

## 2:30–3:15 — Map
Click Medical Desert Map. Explain that many VF records do not include exact coordinates, so the app uses offline Ghana city/region geocoding and preserves source evidence separately.

## 3:15–4:15 — Ask Agent
Ask:

```text
Which facilities need manual verification?
```

Show that the answer lists specific facilities, claims, statuses, reasons, and evidence rows.

## 4:15–4:45 — Evidence Trace
Show extracted evidence and verification trace. Emphasize that this is not just search: each recommendation is backed by row-level data.

## 4:45–5:00 — Close
Say:

> MediRoute AI is not a hospital search engine. It is a healthcare coordination layer that helps NGOs prioritize field verification, doctor deployment, and equipment support using evidence-backed intelligence.

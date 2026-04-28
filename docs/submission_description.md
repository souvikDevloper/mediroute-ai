# Submission description

**MediRoute AI** is a Databricks-ready healthcare intelligence system that converts the official Virtue Foundation Ghana facility dataset into verified medical capability maps and NGO action plans.

The system ingests structured and unstructured healthcare facility data, parses official list-like fields such as `procedure`, `equipment`, `capability`, and `specialties`, and verifies whether important claims are complete or suspicious. For example, if a facility claims surgical capability but the evidence does not show an operating theatre, anesthesia, blood bank, or surgical staffing, MediRoute AI flags that claim for manual verification before patients or doctors are routed there.

The product identifies medical deserts by scoring facility and regional gaps across emergency care, surgery, maternity care, ICU-level care, imaging, lab diagnostics, doctor count, capacity, and weak claims. The output is displayed in an NGO-friendly dashboard with map visualization, facility intelligence cards, natural-language planning, and row-level evidence citations.

## Key features

- Databricks Bronze/Silver/Gold Delta pipeline
- Official VF Ghana v0.3 dataset included as the default data source
- IDP extraction for procedure, equipment, capability, and specialty fields
- Suspicious and incomplete claim detection
- Facility-level and region-level medical desert scoring
- Offline Ghana geocoding for map visualization when lat/lon is missing
- Row-level evidence snippets and verification trace
- MLflow metrics and artifacts for agent transparency
- Vector Search-ready `gold_rag_documents` table for RAG-based planner answers
- Streamlit dashboard that can run locally or as a Databricks App-style deployment

## Current processed dataset summary

- Facilities processed: 920
- Regions/groups scored: 17
- High/Critical priority facilities: 34
- Weak claims found: 446
- Verified claims found: 6

## Impact

MediRoute AI helps organizations like the Virtue Foundation reduce manual facility-review time by turning fragmented facility records into actionable, evidence-backed healthcare planning intelligence. It helps identify where expertise exists, where records are incomplete, and where field verification, doctors, oxygen, ICU support, surgical care, or maternal-care resources should be prioritized.

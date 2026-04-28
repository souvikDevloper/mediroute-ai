# MediRoute AI

**Databricks-ready medical desert intelligence for NGO healthcare planning.**

MediRoute AI converts the official Virtue Foundation Ghana facility dataset into verified capability intelligence. It extracts procedures, equipment, specialties, and capabilities; verifies suspicious claims; scores medical desert risk; maps gaps; and gives NGO planners evidence-backed action recommendations.

> Core loop: **Extract → Verify → Score → Recommend → Cite**

---

## Why this exists

The Databricks Accenture challenge asks for an Intelligent Document Parsing agent that goes beyond search: it should extract and verify medical facility capabilities from messy data, identify infrastructure gaps, detect suspicious claims, map critical expertise, and help planners act faster.

MediRoute AI is built for that exact use case.

---

## Final-version upgrades

This final build uses the provided hackathon assets directly:

- `data/official/virtue_foundation_ghana_v0_3.csv` — official Virtue Foundation Ghana dataset.
- `docs/official_schema_documentation.txt` — provided schema definitions.
- `prompts_reference/prompts_and_pydantic_models/` — provided reference prompts and Pydantic models.
- Offline Ghana city geocoding for the map when exact lat/lon is missing.
- Robust parser for official JSON-list columns: `procedure`, `equipment`, `capability`, and `specialties`.
- Databricks notebooks, MLflow trace artifacts, and Vector Search-ready RAG table.

---

## What the product does

### 1. IDP extraction

From the official facility fields and messy free-form text, the system extracts:

- `procedure`
- `equipment`
- `capability`
- `specialties`
- evidence snippets and row IDs

### 2. Claim verification

The verification agent checks whether facility claims are supported by required evidence.

| Claim | Evidence checked |
|---|---|
| Emergency surgical care | operating theatre, anesthesia, blood bank, general surgery |
| Emergency obstetric care | C-section, operating theatre, blood bank, OB/GYN |
| ICU-level care | oxygen, monitors, ventilators |
| Dialysis care | dialysis machines and dialysis treatment |
| Imaging diagnostics | X-ray, ultrasound, CT |
| Emergency response | ambulance, oxygen, triage |

Each claim becomes:

```text
Verified / Incomplete / Suspicious / Not claimed
```

### 3. Medical desert scoring

The scoring engine calculates risk at facility and region/city level using:

- missing critical capabilities
- weak/suspicious claims
- low doctor count
- low bed capacity
- missing equipment evidence

### 4. Planner assistant

The Ask Agent answers planning questions like:

```text
Which regions lack emergency surgical and maternal care?
Which facilities need manual verification?
Where should we deploy oxygen and ICU support first?
```

Every answer links back to row-level evidence.

### 5. Map and dashboard

The dashboard includes:

- Overview
- Medical Desert Map
- Facility Intelligence
- Ask Agent
- Evidence Trace
- Databricks Trace

---

## Databricks-native architecture

```text
Official VF Ghana CSV
   ↓
Bronze Delta: bronze_facility_raw
   ↓
Silver Delta: silver_facility_clean
   ↓
IDP Extraction Agent
   ↓
Gold Delta: gold_idp_extracted_facts
   ↓
Claim Verification Agent
   ↓
Gold Delta: gold_claim_verification
   ↓
Risk Scoring Engine
   ↓
Gold Delta: gold_facility_risk + gold_region_risk
   ↓
RAG Prep: gold_rag_documents
   ↓
MLflow trace + Streamlit / Databricks App
```

Databricks assets included:

- `databricks/notebooks/*.py` — Lakehouse pipeline notebooks
- `databricks.yml` — Databricks Asset Bundle config
- `resources/mediroute_job.yml` — job workflow
- `app.yaml` — Databricks Apps-style manifest placeholder
- `requirements-databricks.txt` — Databricks runtime dependencies
- `sql/medical_desert_dashboard.sql` — Databricks SQL dashboard queries

---

## Local quickstart

```bash
cd mediroute-ai
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements-local.txt
python run_pipeline.py
python -m streamlit run app/streamlit_app.py
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
python -m pip install -r requirements-local.txt
python run_pipeline.py
python -m streamlit run app/streamlit_app.py
```

Then open:

```text
http://localhost:8501
```

The app defaults to the official VF Ghana CSV. You can upload another CSV from the sidebar.

---

## Databricks run

### Manual notebook run

Upload this repo to Databricks Repos / Workspace Files and run:

```text
databricks/notebooks/00_setup_config.py
databricks/notebooks/01_bronze_ingest_delta.py
databricks/notebooks/02_silver_normalize.py
databricks/notebooks/03_gold_agent_pipeline.py
databricks/notebooks/04_mlflow_agent_trace.py
databricks/notebooks/05_vector_search_prep.py
databricks/notebooks/06_quality_checks.py
databricks/notebooks/07_export_for_app.py
```

Widgets:

```text
catalog = main
schema = mediroute_ai
source_path = data/official/virtue_foundation_ghana_v0_3.csv
use_llm = false
```

Use `use_llm=true` only after setting a Databricks Model Serving endpoint in environment variables.

### Databricks Asset Bundle

If the Databricks CLI is configured:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run mediroute_ai_pipeline -t dev
```

---

## Repository structure

```text
mediroute-ai/
├── app/
│   └── streamlit_app.py
├── databricks/
│   ├── README.md
│   └── notebooks/
├── resources/
│   └── mediroute_job.yml
├── src/mediroute/
│   ├── extractor.py
│   ├── verifier.py
│   ├── scoring.py
│   ├── planner.py
│   ├── geo.py
│   └── pipeline.py
├── data/
│   ├── official/
│   ├── sample/
│   ├── processed/
│   └── contracts/
├── docs/
├── prompts_reference/
├── sql/
├── scripts/
├── databricks.yml
├── app.yaml
├── requirements-local.txt
├── requirements-databricks.txt
├── requirements.txt
├── run_pipeline.py
└── LICENSE
```

---

## Demo story

Say this:

> MediRoute AI is not a hospital search engine. It is a Databricks-ready healthcare coordination layer. It turns official VF facility records and messy notes into verified medical capability intelligence, detects suspicious claims, identifies medical deserts, and gives NGOs evidence-backed action plans.

---

## License

MIT License.

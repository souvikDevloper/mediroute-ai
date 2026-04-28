"""Create a small submission metadata file for the hackathon form."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "submission_summary.json"

summary = {
    "project_name": "MediRoute AI",
    "tagline": "Databricks-powered medical desert intelligence for NGO healthcare planning.",
    "core_loop": "Extract -> Verify -> Score -> Recommend -> Cite",
    "databricks_usage": [
        "Bronze/Silver/Gold Delta tables",
        "Databricks notebooks for reproducible pipeline",
        "MLflow run logging metrics and trace artifacts",
        "Vector Search-ready gold_rag_documents table",
        "Asset Bundle job definition for deployment",
    ],
    "features": [
        "Unstructured medical capability extraction",
        "Facility claim verification",
        "Medical desert risk scoring",
        "Map visualization",
        "Natural-language planning assistant",
        "Row-level evidence trace",
    ],
}

OUT.write_text(json.dumps(summary, indent=2))
print(f"Wrote {OUT}")

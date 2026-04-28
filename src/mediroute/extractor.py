"""IDP extraction engine for messy facility text and official VF fact columns."""

from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd
import requests

from .lexicons import CAPABILITY_ALIASES, EQUIPMENT_ALIASES, PROCEDURE_ALIASES, SPECIALTY_ALIASES
from .schemas import EvidenceItem, ExtractionResult
from .text_utils import clean_text, compact_list, is_medical_fact, matched_aliases, parse_list_value, snippet_for

NOTE_COLUMNS = [
    "notes",
    "description",
    "procedure",
    "procedures",
    "equipment",
    "capability",
    "capabilities",
    "specialties",
    "organization_description",
    "services",
    "free_text",
]

OFFICIAL_FACT_COLUMNS = {
    "procedure": "procedures",
    "procedures": "procedures",
    "equipment": "equipment",
    "capability": "capabilities",
    "capabilities": "capabilities",
    "specialties": "specialties",
}


class DatabricksLLMClient:
    """Tiny Databricks Model Serving client.

    The repository works without this client. Enable it by setting:
    USE_DATABRICKS_LLM=true
    DATABRICKS_HOST=https://...
    DATABRICKS_TOKEN=dapi...
    DATABRICKS_MODEL_ENDPOINT=your-serving-endpoint
    """

    def __init__(self) -> None:
        self.host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN", "")
        self.endpoint = os.getenv("DATABRICKS_MODEL_ENDPOINT", "")
        self.enabled = os.getenv("USE_DATABRICKS_LLM", "false").lower() == "true"

    def available(self) -> bool:
        return bool(self.enabled and self.host and self.token and self.endpoint)

    def extract_json(self, text: str) -> dict[str, Any] | None:
        if not self.available():
            return None
        url = f"{self.host}/serving-endpoints/{self.endpoint}/invocations"
        prompt = f"""
You are an intelligent document parsing agent for healthcare facility records.
Extract only facts clearly supported by the text. Return compact JSON with keys:
procedures, equipment, capabilities, specialties, confidence.
Allowed specialty labels: {list(SPECIALTY_ALIASES.keys())}
Text: {text}
""".strip()
        payload = {
            "messages": [
                {"role": "system", "content": "Return valid JSON only. Do not include markdown."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0,
        }
        try:
            r = requests.post(
                url,
                headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
                json=payload,
                timeout=25,
            )
            r.raise_for_status()
            data = r.json()
            content = ""
            if "choices" in data:
                content = data["choices"][0]["message"]["content"]
            elif "predictions" in data:
                content = data["predictions"][0].get("content", "")
            if not content:
                return None
            content = content.strip().strip("`")
            if content.startswith("json"):
                content = content[4:].strip()
            return json.loads(content)
        except Exception:
            return None


class IDPExtractor:
    def __init__(self, use_llm: bool = False) -> None:
        self.llm = DatabricksLLMClient() if use_llm else None

    def combine_text(self, row: pd.Series) -> str:
        parts: list[str] = []
        for col in NOTE_COLUMNS:
            if col not in row:
                continue
            value = row[col]
            if isinstance(value, list):
                txt = "; ".join(clean_text(v) for v in value if clean_text(v))
            else:
                txt = clean_text(value)
            if txt:
                parts.append(f"{col}: {txt}")
        if not parts:
            parts.append(" ".join(clean_text(v) for v in row.values if clean_text(v)))
        return " | ".join(parts)

    def _seed_from_official_columns(self, row: pd.Series, row_id: int) -> tuple[list[str], list[str], list[str], list[str], list[EvidenceItem]]:
        procedures: list[str] = []
        equipment: list[str] = []
        capabilities: list[str] = []
        specialties: list[str] = []
        evidence: list[EvidenceItem] = []

        for source_col, target in OFFICIAL_FACT_COLUMNS.items():
            if source_col not in row:
                continue
            values = parse_list_value(row[source_col])
            if target != "specialties":
                values = [v for v in values if is_medical_fact(v)]
            if not values:
                continue
            if target == "procedures":
                procedures.extend(values)
            elif target == "equipment":
                equipment.extend(values)
            elif target == "capabilities":
                capabilities.extend(values)
            elif target == "specialties":
                specialties.extend([v for v in values if v in SPECIALTY_ALIASES or clean_text(v)])
            for v in values[:4]:
                evidence.append(EvidenceItem(row_id=row_id, field=source_col, snippet=v[:280]))

        return procedures, equipment, capabilities, specialties, evidence

    def _rule_extract(self, text: str, row_id: int) -> tuple[list[str], list[str], list[str], list[str], list[EvidenceItem]]:
        evidence: list[EvidenceItem] = []
        procedures: list[str] = []
        equipment: list[str] = []
        capabilities: list[str] = []
        specialties: list[str] = []

        def scan(alias_map: dict[str, list[str]], target: list[str], field: str = "notes") -> None:
            for label, aliases in alias_map.items():
                hits = matched_aliases(text, aliases)
                if hits:
                    target.append(label)
                    evidence.append(EvidenceItem(row_id=row_id, field=field, snippet=snippet_for(text, hits[0])))

        scan(PROCEDURE_ALIASES, procedures)
        scan(EQUIPMENT_ALIASES, equipment)
        scan(CAPABILITY_ALIASES, capabilities)
        scan(SPECIALTY_ALIASES, specialties)
        return compact_list(procedures), compact_list(equipment), compact_list(capabilities), compact_list(specialties), evidence

    def extract_row(self, row: pd.Series) -> ExtractionResult:
        row_id = int(row.get("row_id", row.name if row.name is not None else 0))
        facility_name = clean_text(row.get("facility_name", row.get("name", f"Facility {row_id}")))
        text = self.combine_text(row)

        seed_p, seed_e, seed_c, seed_s, seed_ev = self._seed_from_official_columns(row, row_id)
        rule_p, rule_e, rule_c, rule_s, rule_ev = self._rule_extract(text, row_id)
        procedures = compact_list(seed_p + rule_p)
        equipment = compact_list(seed_e + rule_e)
        capabilities = compact_list(seed_c + rule_c)
        specialties = compact_list(seed_s + rule_s)
        evidence = seed_ev + rule_ev

        if self.llm and self.llm.available():
            llm_out = self.llm.extract_json(text)
            if llm_out:
                procedures = compact_list(procedures + llm_out.get("procedures", []))
                equipment = compact_list(equipment + llm_out.get("equipment", []))
                capabilities = compact_list(capabilities + llm_out.get("capabilities", []))
                specialties = compact_list(specialties + [s for s in llm_out.get("specialties", []) if s in SPECIALTY_ALIASES])

        hit_count = len(procedures) + len(equipment) + len(capabilities) + len(specialties)
        confidence = min(0.97, round(0.35 + 0.035 * hit_count, 2)) if hit_count else 0.15
        if not evidence and text:
            evidence = [EvidenceItem(row_id=row_id, field="notes", snippet=text[:240])]

        return ExtractionResult(
            row_id=row_id,
            facility_name=facility_name,
            procedures=procedures,
            equipment=equipment,
            capabilities=capabilities,
            specialties=specialties,
            confidence=confidence,
            evidence=evidence[:10],
        )

    def extract_dataframe(self, df: pd.DataFrame) -> list[ExtractionResult]:
        return [self.extract_row(row) for _, row in df.iterrows()]


def extraction_to_frame(results: list[ExtractionResult]) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append(
            {
                "row_id": r.row_id,
                "facility_name": r.facility_name,
                "procedures": r.procedures,
                "equipment": r.equipment,
                "capabilities": r.capabilities,
                "specialties": r.specialties,
                "extraction_confidence": r.confidence,
                "evidence": [e.model_dump() for e in r.evidence],
            }
        )
    return pd.DataFrame(rows)

"""End-to-end local/Databricks pipeline used by notebooks and Streamlit."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .extractor import IDPExtractor, extraction_to_frame
from .geo import canonical_region, infer_lat_lon
from .scoring import facility_risk, region_risk
from .text_utils import clean_text, parse_list_value, to_snake_case
from .verifier import verify_dataframe

LIST_COLUMNS = ["specialties", "procedure", "procedures", "equipment", "capability", "capabilities", "affiliation_type_ids", "countries", "phone_numbers", "websites"]
TEXT_CONTEXT_COLUMNS = ["description", "procedure", "equipment", "capability", "specialties", "organization_description", "mission_statement"]


def _safe_int(v: Any) -> int:
    try:
        text = clean_text(v)
        if not text:
            return 0
        return int(float(text))
    except Exception:
        return 0


def _first_present(row: pd.Series, cols: list[str]) -> str:
    for c in cols:
        if c in row and clean_text(row.get(c)):
            return clean_text(row.get(c))
    return ""


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize official VF Ghana schema and uploaded CSV variants.

    The official CSV uses columns such as ``numberDoctors``, ``facilityTypeId``,
    ``address_city`` and JSON-list text cells. This normalizer turns them into a
    consistent internal schema without losing the original evidence columns.
    """
    df = df.copy()
    df.columns = [to_snake_case(c) for c in df.columns]

    aliases = {
        "name": "facility_name",
        "facility": "facility_name",
        "facility_name_": "facility_name",
        "state": "region",
        "region_name": "region",
        "address_stateor_region": "address_state_or_region",
        "lat": "latitude",
        "lon": "longitude",
        "lng": "longitude",
        "beds": "capacity",
        "doctors": "number_doctors",
        "numberdoctors": "number_doctors",
    }
    df = df.rename(columns={k: v for k, v in aliases.items() if k in df.columns})

    if "row_id" not in df.columns:
        # Row citations should correspond to the source CSV row after the header.
        df.insert(0, "row_id", range(1, len(df) + 1))
    else:
        ids = pd.to_numeric(df["row_id"], errors="coerce")
        df["row_id"] = ids.fillna(pd.Series(range(1, len(df) + 1))).astype(int)

    if "facility_name" not in df.columns:
        df["facility_name"] = [f"Facility {i}" for i in df["row_id"]]
    df["facility_name"] = df["facility_name"].map(lambda x: clean_text(x) or "Unknown Facility")

    # Keep facilities as the primary decision-support population. NGOs remain in
    # the raw CSV and can be uploaded, but medical-desert scoring is facility-led.
    if "organization_type" in df.columns:
        mask = df["organization_type"].map(lambda x: clean_text(x).lower() in {"", "facility", "hospital", "clinic", "doctor", "dentist", "pharmacy", "farmacy"})
        if mask.any():
            # Preserve original CSV row number for row-level citations.
            df = df[mask].copy()

    for col in ["address_city", "address_state_or_region", "facility_type_id", "operator_type_id", "source_url", "description"]:
        if col not in df.columns:
            df[col] = ""

    if "city" not in df.columns:
        df["city"] = df["address_city"].map(clean_text)

    if "region" not in df.columns:
        df["region"] = ""
    df["region"] = df.apply(
        lambda r: canonical_region(_first_present(r, ["address_state_or_region", "region"]), _first_present(r, ["city", "address_city"])),
        axis=1,
    )

    for col in ["number_doctors", "capacity", "area", "year_established"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].map(_safe_int)

    # Convert JSON-list columns into real Python lists for extraction and scoring.
    for col in LIST_COLUMNS:
        if col in df.columns:
            df[col] = df[col].map(parse_list_value)

    # Build a single evidence context string for rows that do not have a notes column.
    if "notes" not in df.columns:
        df["notes"] = ""
    def build_notes(row: pd.Series) -> str:
        parts: list[str] = []
        for c in TEXT_CONTEXT_COLUMNS:
            if c in row:
                value = row[c]
                if isinstance(value, list):
                    txt = "; ".join(value)
                else:
                    txt = clean_text(value)
                if txt:
                    parts.append(f"{c}: {txt}")
        current = clean_text(row.get("notes"))
        if current:
            parts.insert(0, current)
        return " | ".join(parts)
    df["notes"] = df.apply(build_notes, axis=1)

    # Offline lat/lon inference for map. Real coordinates can override this.
    if "latitude" not in df.columns:
        df["latitude"] = None
    if "longitude" not in df.columns:
        df["longitude"] = None
    def fill_geo(row: pd.Series) -> tuple[float | None, float | None]:
        try:
            lat = pd.to_numeric(row.get("latitude"), errors="coerce")
            lon = pd.to_numeric(row.get("longitude"), errors="coerce")
            if pd.notna(lat) and pd.notna(lon):
                return float(lat), float(lon)
        except Exception:
            pass
        return infer_lat_lon(row.get("city"), row.get("region"), row.get("facility_name"))
    coords = df.apply(fill_geo, axis=1)
    df["latitude"] = [c[0] for c in coords]
    df["longitude"] = [c[1] for c in coords]

    return df.reset_index(drop=True)


def process_dataframe(df: pd.DataFrame, use_llm: bool = False) -> dict[str, pd.DataFrame]:
    raw = normalize_columns(df)
    extractor = IDPExtractor(use_llm=use_llm)
    extracted = extraction_to_frame(extractor.extract_dataframe(raw))
    verification = verify_dataframe(extracted)
    facilities = facility_risk(extracted, verification, raw)
    regions = region_risk(facilities, extracted)
    gold = raw.merge(extracted, on=["row_id", "facility_name"], how="left").merge(
        facilities[["row_id", "risk_score", "risk_level", "primary_gap", "recommendation", "suspicious_or_incomplete_claims", "verified_claims"]],
        on="row_id",
        how="left",
    )
    return {
        "raw": raw,
        "extracted": extracted,
        "verification": verification,
        "facilities": facilities,
        "regions": regions,
        "gold": gold,
    }


def _serializable_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if out[col].map(lambda x: isinstance(x, (list, dict))).any():
            out[col] = out[col].map(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
    return out


def run_pipeline(input_csv: str | Path, output_dir: str | Path = "data/processed", use_llm: bool = False) -> dict[str, pd.DataFrame]:
    input_csv = Path(input_csv)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    outputs = process_dataframe(df, use_llm=use_llm)
    for name, frame in outputs.items():
        _serializable_frame(frame).to_csv(output_dir / f"{name}.csv", index=False)
    return outputs

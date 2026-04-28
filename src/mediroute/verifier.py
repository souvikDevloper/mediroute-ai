"""Capability verification agent."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from .schemas import VerificationClaim
from .text_utils import compact_list, parse_list_value


CLAIM_RULES: dict[str, dict[str, object]] = {
    "Emergency surgical care": {
        "claim_signals": ["Provides surgical care", "Performs general surgical operations"],
        "required": ["Has operating theatre", "Has anesthesia equipment", "Has blood bank", "generalSurgery"],
        "minimum_present": 2,
    },
    "Emergency obstetric care": {
        "claim_signals": ["Provides maternity care", "Performs C-section deliveries", "gynecologyAndObstetrics"],
        "required": ["Performs C-section deliveries", "Has operating theatre", "Has blood bank", "gynecologyAndObstetrics"],
        "minimum_present": 2,
    },
    "ICU-level care": {
        "claim_signals": ["Provides ICU-level care"],
        "required": ["Has oxygen supply", "Has patient monitors", "Has ventilator support"],
        "minimum_present": 2,
    },
    "Dialysis care": {
        "claim_signals": ["Provides dialysis care", "Provides hemodialysis treatment"],
        "required": ["Has dialysis machines", "Provides hemodialysis treatment"],
        "minimum_present": 2,
    },
    "Imaging diagnostics": {
        "claim_signals": ["Provides imaging diagnostics", "Provides X-ray imaging", "Provides ultrasound screening", "Provides CT imaging"],
        "required": ["Has X-ray machine", "Has ultrasound machine", "Has CT scanner"],
        "minimum_present": 1,
    },
    "Emergency response": {
        "claim_signals": ["Provides emergency care", "Provides emergency triage"],
        "required": ["Has ambulance", "Has oxygen supply", "Provides emergency triage"],
        "minimum_present": 1,
    },
}


def _as_set(values: Iterable[str] | float | str | None) -> set[str]:
    if values is None:
        return set()
    if isinstance(values, list):
        return {str(v) for v in values if str(v).strip()}
    if isinstance(values, str):
        return set(parse_list_value(values))
    return set()


def verify_facility(row: pd.Series) -> list[VerificationClaim]:
    row_id = int(row["row_id"])
    facility = str(row["facility_name"])
    facts = set()
    for col in ["procedures", "equipment", "capabilities", "specialties"]:
        facts |= _as_set(row.get(col))

    claims: list[VerificationClaim] = []
    for claim_name, rule in CLAIM_RULES.items():
        signals = set(rule["claim_signals"])  # type: ignore[arg-type]
        required = set(rule["required"])  # type: ignore[arg-type]
        min_present = int(rule["minimum_present"])
        has_claim = bool(facts & signals)
        present = compact_list(required & facts)
        missing = compact_list(required - facts)

        if not has_claim:
            claims.append(
                VerificationClaim(
                    row_id=row_id,
                    facility_name=facility,
                    claim=claim_name,
                    status="Not claimed",
                    confidence=0.0,
                    reason="No clear claim signal found in extracted facts.",
                    present_evidence=present,
                    missing_evidence=missing,
                    evidence_rows=[row_id],
                )
            )
            continue

        if len(present) >= len(required):
            status = "Verified"
            confidence = 0.92
            reason = f"All expected evidence for {claim_name.lower()} is present."
        elif len(present) >= min_present:
            status = "Incomplete"
            confidence = round(0.55 + 0.1 * len(present), 2)
            reason = f"Claim is partly supported, but key evidence is missing: {', '.join(missing)}."
        else:
            status = "Suspicious"
            confidence = round(0.35 + 0.08 * len(present), 2)
            reason = f"Claim signal exists, but supporting evidence is weak or absent. Missing: {', '.join(missing)}."

        claims.append(
            VerificationClaim(
                row_id=row_id,
                facility_name=facility,
                claim=claim_name,
                status=status,
                confidence=min(confidence, 0.95),
                reason=reason,
                present_evidence=present,
                missing_evidence=missing,
                evidence_rows=[row_id],
            )
        )
    return claims


def verify_dataframe(extracted_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in extracted_df.iterrows():
        for claim in verify_facility(row):
            rows.append(claim.model_dump())
    return pd.DataFrame(rows)

"""Pydantic schemas used across MediRoute AI."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ClaimStatus = Literal["Verified", "Incomplete", "Suspicious", "Not claimed"]
RiskLevel = Literal["Low", "Medium", "High", "Critical"]


class EvidenceItem(BaseModel):
    row_id: int
    field: str = "notes"
    snippet: str


class ExtractionResult(BaseModel):
    row_id: int
    facility_name: str
    procedures: list[str] = Field(default_factory=list)
    equipment: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    specialties: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    evidence: list[EvidenceItem] = Field(default_factory=list)


class VerificationClaim(BaseModel):
    row_id: int
    facility_name: str
    claim: str
    status: ClaimStatus
    confidence: float
    reason: str
    present_evidence: list[str] = Field(default_factory=list)
    missing_evidence: list[str] = Field(default_factory=list)
    evidence_rows: list[int] = Field(default_factory=list)


class FacilityRisk(BaseModel):
    row_id: int
    facility_name: str
    region: str
    risk_score: int
    risk_level: RiskLevel
    primary_gap: str
    recommendation: str


class RegionRisk(BaseModel):
    region: str
    facilities: int
    verified_claims: int
    suspicious_or_incomplete_claims: int
    avg_doctors: float
    total_capacity: int
    missing_critical_capabilities: list[str]
    risk_score: int
    risk_level: RiskLevel
    recommended_action: str

"""Medical desert risk scoring."""

from __future__ import annotations

import pandas as pd

from .lexicons import CRITICAL_CAPABILITIES
from .text_utils import compact_list


def risk_level(score: int) -> str:
    if score >= 75:
        return "Critical"
    if score >= 45:
        return "High"
    if score >= 25:
        return "Medium"
    return "Low"


def facility_risk(extracted_df: pd.DataFrame, verification_df: pd.DataFrame, raw_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    raw = raw_df.set_index("row_id") if "row_id" in raw_df.columns else raw_df.copy()
    for _, r in extracted_df.iterrows():
        row_id = int(r["row_id"])
        caps = set(r.get("capabilities", []) or [])
        missing = [c for c in CRITICAL_CAPABILITIES if c not in caps]
        v = verification_df[(verification_df["row_id"] == row_id) & (verification_df["status"].isin(["Suspicious", "Incomplete"]))]
        verified = verification_df[(verification_df["row_id"] == row_id) & (verification_df["status"] == "Verified")]

        doctor_count = 0
        capacity = 0
        region = "Unknown"
        if row_id in raw.index:
            rr = raw.loc[row_id]
            doctor_count = int(pd.to_numeric(rr.get("number_doctors", 0), errors="coerce") or 0)
            capacity = int(pd.to_numeric(rr.get("capacity", 0), errors="coerce") or 0)
            region = str(rr.get("region", "Unknown"))

        score = 0
        score += min(18, len(missing) * 3)
        score += min(32, len(v) * 8)
        score += 6 if doctor_count <= 1 else 3 if doctor_count <= 3 else 0
        score += 6 if capacity <= 10 else 3 if capacity <= 30 else 0
        score -= min(18, len(verified) * 4)
        score = max(0, min(100, int(score)))

        primary_gap = missing[0].replace("Provides ", "") if missing else "No major extracted gap"
        recommendation = recommendation_for_facility(score, primary_gap, len(v))
        rows.append(
            {
                "row_id": row_id,
                "facility_name": r["facility_name"],
                "region": region,
                "risk_score": score,
                "risk_level": risk_level(score),
                "primary_gap": primary_gap,
                "suspicious_or_incomplete_claims": len(v),
                "verified_claims": len(verified),
                "doctor_count": doctor_count,
                "capacity": capacity,
                "recommendation": recommendation,
            }
        )
    return pd.DataFrame(rows)


def recommendation_for_facility(score: int, gap: str, weak_claims: int) -> str:
    if score >= 75:
        return f"Critical: prioritize field verification and deploy support for {gap}."
    if weak_claims >= 3:
        return "High: manually verify claimed services before routing patients or doctors."
    if score >= 55:
        return f"High: consider targeted equipment or specialist support for {gap}."
    if score >= 30:
        return "Medium: monitor capability gaps and validate records during next outreach cycle."
    return "Low: facility appears comparatively better supported from available evidence."


def region_risk(facility_df: pd.DataFrame, extracted_df: pd.DataFrame) -> pd.DataFrame:
    merged = facility_df.merge(extracted_df[["row_id", "capabilities", "specialties"]], on="row_id", how="left")
    rows = []
    for region, g in merged.groupby("region", dropna=False):
        all_caps = set()
        for caps in g["capabilities"]:
            if isinstance(caps, list):
                all_caps |= set(caps)
        missing = [c for c in CRITICAL_CAPABILITIES if c not in all_caps]
        avg_score = int(g["risk_score"].mean()) if len(g) else 0
        weak_claims = int(g["suspicious_or_incomplete_claims"].sum())
        low_capacity_penalty = 6 if int(g["capacity"].sum()) < 40 else 0
        score = min(100, max(0, avg_score + min(24, weak_claims * 2) + len(missing) * 3 + low_capacity_penalty))
        action = recommendation_for_region(score, missing)
        rows.append(
            {
                "region": str(region),
                "facilities": int(len(g)),
                "avg_facility_risk": avg_score,
                "risk_score": int(score),
                "risk_level": risk_level(int(score)),
                "suspicious_or_incomplete_claims": weak_claims,
                "verified_claims": int(g["verified_claims"].sum()),
                "total_capacity": int(g["capacity"].sum()),
                "avg_doctors": round(float(g["doctor_count"].mean()), 2),
                "missing_critical_capabilities": compact_list(missing),
                "recommended_action": action,
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["risk_score", "suspicious_or_incomplete_claims"], ascending=False)
    return out


def recommendation_for_region(score: int, missing: list[str]) -> str:
    gap = missing[0].replace("Provides ", "") if missing else "remaining minor gaps"
    if score >= 75:
        return f"Deploy NGO field team first; validate records and prioritize {gap}."
    if score >= 55:
        return f"Plan targeted intervention for {gap}; verify incomplete facility claims."
    if score >= 30:
        return "Monitor region and schedule lower-priority validation visits."
    return "Maintain records; region is lower priority under current evidence."

"""Evidence-first natural-language planner for NGO users.

The planner is deterministic for hackathon reliability. It returns an answer and
an evidence DataFrame that can be displayed as row-level citations.
"""

from __future__ import annotations

import pandas as pd


def _list_text(v) -> str:
    if isinstance(v, list):
        return ", ".join(str(x) for x in v[:6]) if v else "—"
    if pd.isna(v) if not isinstance(v, (list, dict, str)) else False:
        return "—"
    return str(v) if str(v).strip() else "—"


def _header(title: str) -> list[str]:
    return [f"### {title}", ""]


def _priority_lines(rows: pd.DataFrame, max_rows: int = 6) -> list[str]:
    lines: list[str] = []
    for i, (_, r) in enumerate(rows.head(max_rows).iterrows(), start=1):
        region = r.get("region", "Unknown region")
        risk = r.get("risk_level", "—")
        score = r.get("risk_score", "—")
        action = r.get("recommended_action", r.get("recommendation", "Verify facility record and plan targeted support."))
        name = r.get("facility_name", r.get("region", "Unknown"))
        lines.append(f"**Priority {i}: {name}**")
        lines.append(f"- Risk: **{risk} ({score})** in **{region}**")
        if "claim" in r:
            lines.append(f"- Claim/gap: **{r.get('claim')}** is **{r.get('status', 'flagged')}**")
        elif "primary_gap" in r:
            lines.append(f"- Primary gap: **{r.get('primary_gap')}**")
        if "reason" in r:
            lines.append(f"- Reason: {r.get('reason')}")
        lines.append(f"- Recommended action: {action}")
        if "row_id" in r:
            lines.append(f"- Evidence row: `{r.get('row_id')}`")
        lines.append("")
    return lines


def answer_query(query: str, outputs: dict[str, pd.DataFrame]) -> tuple[str, pd.DataFrame]:
    q = query.lower().strip()
    regions = outputs["regions"].sort_values("risk_score", ascending=False).copy()
    facilities = outputs["facilities"].sort_values("risk_score", ascending=False).copy()
    verification = outputs["verification"].copy()
    gold = outputs["gold"].copy()

    # Attach risk fields to verification evidence when possible.
    risk_cols = ["row_id", "region", "risk_score", "risk_level", "primary_gap", "recommendation"]
    risk_cols = [c for c in risk_cols if c in facilities.columns]
    verification_risk = verification.merge(facilities[risk_cols], on="row_id", how="left") if risk_cols else verification

    if any(k in q for k in ["manual", "verify", "verification", "suspicious", "incomplete"]):
        weak = verification_risk[verification_risk["status"].isin(["Suspicious", "Incomplete"])].copy()
        weak = weak.sort_values(["risk_score", "confidence"], ascending=[False, True]) if "risk_score" in weak.columns else weak
        lines = _header("Facilities needing manual verification")
        lines.append("These facilities should be checked before routing patients, doctors, or equipment because their claims are suspicious or incomplete.")
        lines.append("")
        lines += _priority_lines(weak, 8)
        evidence = weak.head(12)[[c for c in ["row_id", "facility_name", "region", "risk_score", "risk_level", "claim", "status", "confidence", "reason", "present_evidence", "missing_evidence"] if c in weak.columns]]
        return "\n".join(lines), evidence

    if any(k in q for k in ["surgeon", "surgery", "surgical", "obstetric", "maternal", "maternity", "c-section", "caesarean"]):
        weak = verification_risk[
            (verification_risk["claim"].isin(["Emergency surgical care", "Emergency obstetric care"]))
            & (verification_risk["status"].isin(["Suspicious", "Incomplete"]))
        ].copy()
        weak = weak.sort_values(["risk_score", "confidence"], ascending=[False, True]) if "risk_score" in weak.columns else weak
        lines = _header("Surgical and maternal-care deployment priority")
        lines.append("Prioritize field verification first, then deploy surgical or OB/GYN teams where supporting infrastructure is weakest.")
        lines.append("")
        lines += _priority_lines(weak, 6)
        if not regions.empty:
            lines.append("**Top location clusters to watch:**")
            for _, r in regions.head(3).iterrows():
                lines.append(f"- **{r['region']}**: {r['risk_level']} risk, score {r['risk_score']}. {r['recommended_action']}")
        evidence = weak.head(12)[[c for c in ["row_id", "facility_name", "region", "risk_score", "risk_level", "claim", "status", "reason", "missing_evidence"] if c in weak.columns]]
        return "\n".join(lines), evidence

    if any(k in q for k in ["icu", "oxygen", "critical", "ventilator", "emergency support"]):
        weak = verification_risk[
            (verification_risk["claim"].isin(["ICU-level care", "Emergency response"]))
            & (verification_risk["status"].isin(["Suspicious", "Incomplete", "Not claimed"]))
        ].copy()
        weak = weak.sort_values(["risk_score", "confidence"], ascending=[False, True]) if "risk_score" in weak.columns else weak
        lines = _header("Oxygen and ICU-support deployment priority")
        lines.append("Send oxygen/emergency support where ICU or emergency-response evidence is incomplete, then validate before sustained patient routing.")
        lines.append("")
        lines += _priority_lines(weak, 7)
        evidence = weak.head(12)[[c for c in ["row_id", "facility_name", "region", "risk_score", "risk_level", "claim", "status", "reason", "missing_evidence"] if c in weak.columns]]
        return "\n".join(lines), evidence

    if any(k in q for k in ["imaging", "x-ray", "xray", "ultrasound", "lab", "laboratory", "diagnostic"]):
        weak = verification_risk[
            (verification_risk["claim"].isin(["Imaging diagnostics", "Laboratory diagnostics"]))
            & (verification_risk["status"].isin(["Suspicious", "Incomplete"]))
        ].copy()
        weak = weak.sort_values(["risk_score", "confidence"], ascending=[False, True]) if "risk_score" in weak.columns else weak
        lines = _header("Diagnostics deployment priority")
        lines.append("Use mobile imaging/lab resources where diagnostic claims are present but evidence is incomplete.")
        lines.append("")
        lines += _priority_lines(weak, 7)
        evidence = weak.head(12)[[c for c in ["row_id", "facility_name", "region", "risk_score", "risk_level", "claim", "status", "reason", "missing_evidence"] if c in weak.columns]]
        return "\n".join(lines), evidence

    if any(k in q for k in ["region", "desert", "gap", "where", "deploy", "send", "prioritize", "month"]):
        lines = _header("Medical desert priority summary")
        lines.append("Start with the highest-risk source-derived location clusters, then verify the top facilities inside each cluster.")
        lines.append("")
        for i, (_, r) in enumerate(regions.head(6).iterrows(), start=1):
            missing = _list_text(r.get("missing_critical_capabilities", []))
            lines.append(f"**Priority {i}: {r['region']}**")
            lines.append(f"- Risk: **{r['risk_level']} ({r['risk_score']})**")
            lines.append(f"- Missing signals: {missing}")
            lines.append(f"- Recommended action: {r['recommended_action']}")
            lines.append("")
        evidence = regions.head(10)
        return "\n".join(lines), evidence

    lines = _header("Recommended NGO action plan")
    lines.extend(
        [
            "1. Start with the highest-risk location clusters and facilities.",
            "2. Manually verify suspicious or incomplete claims before routing patients or volunteers.",
            "3. Prioritize surgical/OB-GYN, oxygen/emergency, imaging, and lab resources using the intervention simulator.",
            "4. Use row-level evidence and verification trace before making operational decisions.",
        ]
    )
    evidence = facilities.head(10)
    return "\n".join(lines), evidence

"""Natural-language planner for NGO users.

This is intentionally deterministic and evidence-first for demo reliability.
It can be upgraded to call a Databricks model endpoint after retrieving evidence.
"""

from __future__ import annotations

import pandas as pd


def answer_query(query: str, outputs: dict[str, pd.DataFrame]) -> tuple[str, pd.DataFrame]:
    q = query.lower().strip()
    regions = outputs["regions"]
    facilities = outputs["facilities"]
    verification = outputs["verification"]
    gold = outputs["gold"]

    if any(k in q for k in ["surgeon", "surgery", "surgical", "obstetric", "maternal", "maternity", "c-section"]):
        weak = verification[
            (verification["claim"].isin(["Emergency surgical care", "Emergency obstetric care"]))
            & (verification["status"].isin(["Suspicious", "Incomplete"]))
        ]
        evidence_rows = weak["row_id"].unique().tolist()[:8]
        evidence = gold[gold["row_id"].isin(evidence_rows)][["row_id", "facility_name", "region", "notes", "risk_level", "recommendation"]]
        top_regions = regions.head(3)
        lines = ["### Surgical / maternal-care deployment priority", ""]
        for _, r in top_regions.iterrows():
            missing = r.get("missing_critical_capabilities", [])
            missing_txt = ", ".join(missing) if isinstance(missing, list) else str(missing)
            lines.append(f"- **{r['region']}**: {r['risk_level']} risk, score {r['risk_score']}. Missing signals: {missing_txt}. Action: {r['recommended_action']}")
        if not weak.empty:
            lines += ["", "**Facilities needing manual verification:**"]
            for _, w in weak.head(6).iterrows():
                lines.append(f"- Row {w['row_id']} — **{w['facility_name']}**: {w['claim']} is **{w['status']}**. {w['reason']}")
        return "\n".join(lines), evidence

    if any(k in q for k in ["icu", "oxygen", "critical", "ventilator"]):
        weak = verification[(verification["claim"] == "ICU-level care") & (verification["status"].isin(["Suspicious", "Incomplete", "Not claimed"]))]
        high = facilities.sort_values("risk_score", ascending=False).head(8)
        evidence = gold[gold["row_id"].isin(high["row_id"])][["row_id", "facility_name", "region", "notes", "risk_level", "recommendation"]]
        lines = ["### ICU / oxygen support priority", ""]
        for _, r in regions.head(3).iterrows():
            lines.append(f"- **{r['region']}**: {r['risk_level']} risk. {r['recommended_action']}")
        lines += ["", f"Found **{len(weak)}** facilities without verified ICU-level evidence. Prioritize oxygen, monitors, and manual verification before patient routing."]
        return "\n".join(lines), evidence

    if any(k in q for k in ["suspicious", "incomplete", "verify", "manual"]):
        weak = verification[verification["status"].isin(["Suspicious", "Incomplete"])].sort_values("confidence")
        evidence = weak.head(10)[["row_id", "facility_name", "claim", "status", "reason", "missing_evidence"]]
        lines = ["### Facilities needing verification", ""]
        for _, w in weak.head(8).iterrows():
            lines.append(f"- Row {w['row_id']} — **{w['facility_name']}**: {w['claim']} is **{w['status']}**. {w['reason']}")
        return "\n".join(lines), evidence

    if any(k in q for k in ["region", "desert", "gap", "where", "deploy", "send"]):
        evidence = regions.head(10)
        lines = ["### Medical desert summary", ""]
        for _, r in regions.head(5).iterrows():
            lines.append(f"- **{r['region']}**: {r['risk_level']} risk, score {r['risk_score']}. {r['recommended_action']}")
        return "\n".join(lines), evidence

    evidence = facilities.sort_values("risk_score", ascending=False).head(8)
    lines = [
        "### Recommended NGO action plan",
        "",
        "1. Start with the highest-risk regions in the table below.",
        "2. Manually verify facilities with suspicious or incomplete claims.",
        "3. Prioritize specialist deployment where maternity, surgery, ICU, imaging, or lab evidence is missing.",
        "4. Use row-level evidence before routing patients or volunteers.",
    ]
    return "\n".join(lines), evidence

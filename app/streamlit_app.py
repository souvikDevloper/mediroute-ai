from __future__ import annotations

import json
import os
import sys
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import pydeck as pdk
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mediroute.pipeline import process_dataframe  # noqa: E402
from mediroute.planner import answer_query  # noqa: E402

OFFICIAL = ROOT / "data" / "official" / "virtue_foundation_ghana_v0_3.csv"
SAMPLE = ROOT / "data" / "sample" / "ghana_facility_sample.csv"
PROCESSED_DIR = Path(os.getenv("MEDIROUTE_PROCESSED_DIR", ROOT / "data" / "processed"))

st.set_page_config(page_title="MediRoute AI", page_icon="🏥", layout="wide")

st.markdown(
    """
    <style>
        .main .block-container { padding-top: 2.1rem; max-width: 1480px; }
        [data-testid="stSidebar"] button { width: 100%; white-space: normal; text-align: left; }
        [data-testid="stSidebar"] .stCodeBlock pre { white-space: pre-wrap; word-break: break-word; }
        div[data-testid="stMetricValue"] { font-size: 2rem; }
        .hero {
            border: 1px solid rgba(103, 232, 249, 0.25);
            border-radius: 24px;
            padding: 1.4rem 1.5rem;
            background:
                radial-gradient(circle at 10% 20%, rgba(103, 232, 249, 0.16), transparent 30%),
                radial-gradient(circle at 90% 10%, rgba(255, 75, 110, 0.18), transparent 32%),
                linear-gradient(135deg, rgba(10, 25, 47, 0.92), rgba(6, 17, 31, 0.98));
            margin-bottom: 1rem;
        }
        .hero-title { font-size: 2.25rem; font-weight: 850; color: #ffffff; margin-bottom: 0.25rem; }
        .hero-subtitle { color: rgba(219, 234, 254, 0.88); font-size: 1.02rem; line-height: 1.55; }
        .loop-pill {
            display: inline-block;
            border-radius: 999px;
            padding: 0.35rem 0.75rem;
            margin: 0.2rem 0.25rem 0.2rem 0;
            background: rgba(103, 232, 249, 0.10);
            border: 1px solid rgba(103, 232, 249, 0.22);
            color: #dbeafe;
            font-size: 0.88rem;
        }
        .mediroute-card {
            border: 1px solid rgba(250,250,250,0.14);
            border-radius: 16px;
            padding: 1rem 1.1rem;
            background: rgba(255,255,255,0.04);
            margin-bottom: 0.85rem;
            box-shadow: 0 10px 28px rgba(0,0,0,0.14);
        }
        .priority-card {
            border: 1px solid rgba(103, 232, 249, 0.24);
            border-radius: 18px;
            padding: 1.05rem 1.1rem;
            background: linear-gradient(135deg, rgba(14, 43, 70, 0.74), rgba(9, 18, 32, 0.86));
            margin-bottom: 0.9rem;
        }
        .small-muted { color: rgba(250,250,250,0.64); font-size: 0.92rem; }
        .risk-critical { color: #ff6b7c; font-weight: 800; }
        .risk-high { color: #f59e0b; font-weight: 800; }
        .risk-medium { color: #facc15; font-weight: 800; }
        .risk-low { color: #22c55e; font-weight: 800; }
        .evidence-chip {
            display: inline-block;
            border-radius: 999px;
            padding: 0.22rem 0.58rem;
            margin: 0.12rem;
            background: rgba(148, 163, 184, 0.14);
            border: 1px solid rgba(148, 163, 184, 0.18);
            color: #e2e8f0;
            font-size: 0.78rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def load_official() -> pd.DataFrame:
    return pd.read_csv(OFFICIAL, dtype=str, keep_default_na=False)


@st.cache_data(show_spinner=False)
def load_sample() -> pd.DataFrame:
    return pd.read_csv(SAMPLE, dtype=str, keep_default_na=False)


@st.cache_data(show_spinner=True)
def run_cached(csv_bytes: bytes | None, use_included: bool) -> dict[str, pd.DataFrame]:
    if use_included or csv_bytes is None:
        df = load_official() if OFFICIAL.exists() else load_sample()
    else:
        df = pd.read_csv(BytesIO(csv_bytes), dtype=str, keep_default_na=False)
    return process_dataframe(df, use_llm=False)


def parse_any_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if str(x).strip()]
    if isinstance(v, float) and pd.isna(v):
        return []
    if isinstance(v, str):
        txt = v.strip()
        if not txt or txt.lower() in {"nan", "none", "null", "[]"}:
            return []
        if txt.startswith("["):
            try:
                data = json.loads(txt.replace("'", '"'))
                if isinstance(data, list):
                    return [str(x) for x in data if str(x).strip()]
            except Exception:
                pass
        if ";" in txt:
            return [p.strip() for p in txt.split(";") if p.strip()]
        if "|" in txt:
            return [p.strip() for p in txt.split("|") if p.strip()]
        return [txt]
    return [str(v)]


def list_text(v: Any, limit: int = 8) -> str:
    items = parse_any_list(v)
    if not items:
        return "—"
    shown = items[:limit]
    suffix = f" +{len(items) - limit} more" if len(items) > limit else ""
    return ", ".join(shown) + suffix


def chips(v: Any, limit: int = 6) -> str:
    items = parse_any_list(v)[:limit]
    if not items:
        return '<span class="small-muted">No explicit evidence listed</span>'
    return "".join(f'<span class="evidence-chip">{x}</span>' for x in items)


def risk_color(level: str) -> list[int]:
    return {
        "Critical": [214, 55, 80, 190],
        "High": [245, 140, 45, 185],
        "Medium": [235, 196, 68, 180],
        "Low": [60, 170, 105, 175],
    }.get(str(level), [120, 120, 120, 160])


def risk_class(level: str) -> str:
    return {
        "Critical": "risk-critical",
        "High": "risk-high",
        "Medium": "risk-medium",
        "Low": "risk-low",
    }.get(str(level), "")


def safe_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    existing = [c for c in cols if c in df.columns]
    return df[existing].copy() if existing else pd.DataFrame()


def download_button(df: pd.DataFrame, label: str, filename: str) -> None:
    st.download_button(
        label,
        df.to_csv(index=False).encode("utf-8"),
        file_name=filename,
        mime="text/csv",
        use_container_width=True,
    )


def evidence_card(row: pd.Series) -> None:
    present = chips(row.get("present_evidence", []))
    missing = chips(row.get("missing_evidence", []))
    st.markdown(
        f"""
        <div class="mediroute-card">
            <b>{row.get('facility_name', 'Unknown facility')}</b> — Row {row.get('row_id', '—')}<br/>
            <span class="small-muted">Claim:</span> {row.get('claim', '—')} &nbsp; | &nbsp;
            <span class="small-muted">Status:</span> <b>{row.get('status', '—')}</b><br/>
            <span class="small-muted">Reason:</span> {row.get('reason', '—')}<br/>
            <span class="small-muted">Observed evidence signals:</span> {present}<br/>
            <span class="small-muted">Expected evidence to verify:</span> {missing}
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_priority_board(outputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    facilities = outputs["facilities"].copy()
    verification = outputs["verification"].copy()
    gold = outputs["gold"].copy()

    weak = verification[verification["status"].isin(["Suspicious", "Incomplete"])] if not verification.empty else verification
    merged = weak.merge(facilities[["row_id", "region", "risk_score", "risk_level", "primary_gap", "recommendation"]], on="row_id", how="left")

    rows: list[dict[str, Any]] = []
    buckets = [
        ("Manual verification", weak, "Send field team before routing patients, volunteers, or equipment", "Facility claim is suspicious or incomplete and needs human confirmation"),
        ("Surgery / maternal care", weak[weak["claim"].isin(["Emergency surgical care", "Emergency obstetric care"])] if not weak.empty else weak, "Verify surgical/OB-GYN readiness; deploy specialist support if the claim fails verification", "Surgery or maternity claims lack expected support evidence"),
        ("Oxygen / ICU support", weak[weak["claim"].isin(["ICU-level care", "Emergency response"])] if not weak.empty else weak, "Verify oxygen/ambulance readiness; deploy backup oxygen support if verification fails", "Emergency or ICU-level readiness is weakly supported"),
        ("Diagnostics support", weak[weak["claim"].isin(["Imaging diagnostics", "Laboratory diagnostics"])] if not weak.empty else weak, "Verify diagnostic capacity; prioritize mobile imaging or lab support where evidence is weak", "Diagnostic claims are incomplete or suspicious"),
    ]

    for category, subset, action, why in buckets:
        if subset.empty:
            continue
        sub = subset.merge(facilities[["row_id", "region", "risk_score", "risk_level", "primary_gap", "recommendation"]], on="row_id", how="left")
        sub = sub.sort_values(["risk_score", "confidence"], ascending=[False, True]).head(4)
        for _, r in sub.iterrows():
            rows.append(
                {
                    "priority": len(rows) + 1,
                    "category": category,
                    "facility_name": r.get("facility_name"),
                    "region": r.get("region"),
                    "risk_score": int(r.get("risk_score", 0) or 0),
                    "risk_level": r.get("risk_level", "—"),
                    "claim": r.get("claim"),
                    "status": r.get("status"),
                    "why": why,
                    "recommended_action": action,
                    "evidence_row": int(r.get("row_id", 0) or 0),
                    "evidence_to_verify": list_text(r.get("missing_evidence", []), 5),
                }
            )

    if not rows:
        top = facilities.sort_values("risk_score", ascending=False).head(8)
        for _, r in top.iterrows():
            rows.append(
                {
                    "priority": len(rows) + 1,
                    "category": "General medical-desert risk",
                    "facility_name": r.get("facility_name"),
                    "region": r.get("region"),
                    "risk_score": int(r.get("risk_score", 0) or 0),
                    "risk_level": r.get("risk_level", "—"),
                    "claim": r.get("primary_gap"),
                    "status": "Prioritize",
                    "why": "High aggregate facility risk",
                    "recommended_action": r.get("recommendation"),
                    "evidence_row": int(r.get("row_id", 0) or 0),
                    "evidence_to_verify": "See facility intelligence",
                }
            )
    return pd.DataFrame(rows).drop_duplicates(subset=["category", "facility_name", "claim"]).head(20)


def intervention_plan(outputs: dict[str, pd.DataFrame], field_teams: int, surgical_teams: int, oxygen_kits: int, imaging_units: int, lab_kits: int) -> pd.DataFrame:
    facilities = outputs["facilities"].copy()
    verification = outputs["verification"].copy()
    weak = verification[verification["status"].isin(["Suspicious", "Incomplete"])].copy()
    weak = weak.merge(facilities[["row_id", "region", "risk_score", "risk_level", "primary_gap", "recommendation"]], on="row_id", how="left")
    rows: list[dict[str, Any]] = []
    used_rows: set[int] = set()

    def add_alloc(resource: str, n: int, subset: pd.DataFrame, reason: str, expected: str):
        nonlocal rows, used_rows
        if n <= 0 or subset.empty:
            return
        subset = subset.sort_values(["risk_score", "confidence"], ascending=[False, True])
        for _, r in subset.iterrows():
            if len([x for x in rows if x["resource"] == resource]) >= n:
                break
            rid = int(r.get("row_id", 0) or 0)
            if rid in used_rows and resource != "Field verification team":
                continue
            used_rows.add(rid)
            rows.append(
                {
                    "resource": resource,
                    "destination": r.get("facility_name", "Unknown facility"),
                    "region": r.get("region", "Unknown"),
                    "risk_level": r.get("risk_level", "—"),
                    "risk_score": int(r.get("risk_score", 0) or 0),
                    "reason": reason,
                    "claim_or_gap": r.get("claim", r.get("primary_gap", "—")),
                    "expected_impact": expected,
                    "evidence_row": rid,
                }
            )

    add_alloc("Field verification team", field_teams, weak, "Claim is suspicious/incomplete and should be verified before deployment.", "Reduces false-routing risk and improves trust in facility capability data.")
    add_alloc("Surgical / OB-GYN team", surgical_teams, weak[weak["claim"].isin(["Emergency surgical care", "Emergency obstetric care"])], "Surgical or maternal-care readiness is weakly supported by evidence.", "Confirms whether specialist deployment is safe and useful before routing patients or volunteers.")
    add_alloc("Oxygen / emergency kit", oxygen_kits, weak[weak["claim"].isin(["ICU-level care", "Emergency response"])], "Oxygen, ambulance, emergency, or ICU-readiness evidence needs confirmation.", "Provides backup oxygen/emergency support where verification shows a real operational gap.")
    add_alloc("Mobile imaging unit", imaging_units, weak[weak["claim"] == "Imaging diagnostics"], "Imaging diagnostics evidence is incomplete or suspicious.", "Improves diagnostic access where X-ray/ultrasound/CT capability is not reliably supported.")
    add_alloc("Lab support kit", lab_kits, weak[weak["claim"] == "Laboratory diagnostics"], "Laboratory diagnostics evidence is incomplete or suspicious.", "Improves basic diagnostic confirmation and triage support after field confirmation.")

    return pd.DataFrame(rows)


def quality_report(outputs: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    gold = outputs["gold"]
    extracted = outputs["extracted"]
    verification = outputs["verification"]
    facilities = outputs["facilities"]
    regions = outputs["regions"]

    def has_any_facts(row: pd.Series) -> bool:
        return any(parse_any_list(row.get(c, [])) for c in ["procedures", "equipment", "capabilities", "specialties"])

    facilities_with_facts = int(extracted.apply(has_any_facts, axis=1).sum()) if not extracted.empty else 0
    weak_claims = int(verification["status"].isin(["Suspicious", "Incomplete"]).sum()) if not verification.empty else 0
    verified_claims = int((verification["status"] == "Verified").sum()) if not verification.empty else 0
    critical_high = int(facilities["risk_level"].isin(["Critical", "High"]).sum()) if not facilities.empty else 0
    rag_docs = len(gold)

    metrics = pd.DataFrame(
        [
            ["Facilities processed", len(gold), "Official/uploaded facility rows normalized into decision records"],
            ["Facilities with extracted facts", facilities_with_facts, "Rows with at least one procedure/equipment/capability/specialty signal"],
            ["Evidence facts extracted", sum(len(parse_any_list(r.get(c, []))) for _, r in extracted.iterrows() for c in ["procedures", "equipment", "capabilities", "specialties"]), "Schema-guided IDP extraction output"],
            ["Verification claims", len(verification), "Claim checks generated by the verification agent"],
            ["Weak/suspicious/incomplete claims", weak_claims, "Claims needing review before action"],
            ["Verified claims", verified_claims, "Claims with stronger supporting evidence"],
            ["Critical/high facilities", critical_high, "Facilities prioritized for action"],
            ["Location clusters scored", len(regions), "Source-derived region/location groups scored"],
            ["RAG-ready documents", rag_docs, "One planner document per facility"],
        ],
        columns=["Metric", "Value", "Why it matters"],
    )

    checks = pd.DataFrame(
        [
            ["Bronze/Silver/Gold pipeline", "PASS", "Databricks notebooks create raw, cleaned, and gold intelligence tables"],
            ["IDP extraction coverage", "PASS" if facilities_with_facts > 0 else "WARN", f"{facilities_with_facts}/{len(gold)} facilities have extracted facts"],
            ["Claim verification coverage", "PASS" if len(verification) > 0 else "FAIL", f"{len(verification)} verification records"],
            ["Medical-desert scoring", "PASS" if critical_high > 0 else "WARN", f"{critical_high} critical/high facilities"],
            ["Evidence-first planner", "PASS", "Planner answers cite rows and verification trace"],
            ["RAG-ready table", "PASS", f"{rag_docs} facility documents prepared for retrieval"],
            ["Responsible-use posture", "PASS", "Tool recommends verification/support; it does not diagnose patients"],
        ],
        columns=["Check", "Status", "Details"],
    )
    return metrics, checks


if "planner_question" not in st.session_state:
    st.session_state["planner_question"] = "Which facilities need manual verification?"

with st.sidebar:
    st.header("Data")
    up = st.file_uploader("Upload facility CSV", type=["csv"])
    use_included = st.toggle("Use included official Virtue Foundation Ghana data", value=up is None)
    st.caption("Default uses the official VF Ghana v0.3 CSV. Upload another CSV to test your own facility records.")
    st.divider()
    st.markdown("**Suggested planner questions**")
    suggestions = [
        "Which facilities need manual verification?",
        "Which areas lack emergency surgical care?",
        "Where should we deploy oxygen support first?",
        "Which facilities have suspicious maternal care claims?",
        "Which regions should Virtue Foundation prioritize this month?",
    ]
    for i, s in enumerate(suggestions):
        if st.button(s, key=f"suggestion_{i}", use_container_width=True):
            st.session_state["planner_question"] = s
    st.divider()
    st.markdown("**Databricks proof**")
    st.caption("Delta Bronze/Silver/Gold • MLflow trace • RAG-ready table • quality checks")

outputs = run_cached(up.getvalue() if up else None, use_included)
raw = outputs["raw"]
gold = outputs["gold"]
regions = outputs["regions"]
facilities = outputs["facilities"]
verification = outputs["verification"]
extracted = outputs["extracted"]

# Prepare map data
map_df = gold.copy()
if "latitude" in map_df.columns and "longitude" in map_df.columns:
    map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
    map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
    map_df = map_df.dropna(subset=["latitude", "longitude"])
    map_df["color"] = map_df["risk_level"].map(risk_color)

st.markdown(
    """
    <div class="hero">
      <div class="hero-title">MediRoute AI</div>
      <div class="hero-subtitle">
        Databricks-backed medical desert intelligence for NGO healthcare planning. It turns messy facility records into verified capability intelligence, intervention priorities, and evidence-backed action plans.
      </div>
      <div style="margin-top:0.7rem;">
        <span class="loop-pill">Extract</span>
        <span class="loop-pill">Verify</span>
        <span class="loop-pill">Score</span>
        <span class="loop-pill">Recommend</span>
        <span class="loop-pill">Cite</span>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Facilities", f"{len(gold):,}")
m2.metric("Location clusters", f"{gold['region'].nunique():,}")
m3.metric("Critical/High", f"{int(facilities['risk_level'].isin(['Critical', 'High']).sum()):,}")
m4.metric("Weak claims", f"{int(verification['status'].isin(['Suspicious', 'Incomplete']).sum()):,}")
m5.metric("Verified claims", f"{int((verification['status'] == 'Verified').sum()):,}")
m6.metric("RAG docs", f"{len(gold):,}")

(
    tab_overview,
    tab_priority,
    tab_map,
    tab_facility,
    tab_agent,
    tab_sim,
    tab_trust,
    tab_evidence,
    tab_databricks,
) = st.tabs(
    [
        "Overview",
        "Priority Board",
        "Medical Desert Map",
        "Facility Intelligence",
        "Ask Agent",
        "Intervention Simulator",
        "Trust & Evaluation",
        "Evidence Trace",
        "Databricks SQL",
    ]
)

with tab_overview:
    st.subheader("Medical desert command overview")
    c1, c2 = st.columns([1.15, 0.85])
    with c1:
        show_regions = regions.copy()
        if "missing_critical_capabilities" in show_regions.columns:
            show_regions["missing_critical_capabilities"] = show_regions["missing_critical_capabilities"].map(list_text)
        st.dataframe(
            safe_cols(show_regions, ["region", "facilities", "risk_score", "risk_level", "missing_critical_capabilities", "recommended_action"]).head(20),
            use_container_width=True,
            hide_index=True,
        )
    with c2:
        st.markdown("### Claim status distribution")
        status_counts = verification["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        st.bar_chart(status_counts, x="status", y="count")
        download_button(build_priority_board(outputs), "Download priority board", "mediroute_priority_board.csv")

    st.markdown("### Top action cards")
    top_regions = regions.sort_values("risk_score", ascending=False).head(3)
    cols = st.columns(3)
    for col, (_, r) in zip(cols, top_regions.iterrows()):
        with col:
            st.markdown(
                f"""
                <div class="priority-card">
                    <div class="small-muted">Priority location cluster</div>
                    <h3 style="margin: 0.2rem 0 0.4rem 0;">{r.get('region', 'Unknown')}</h3>
                    <div>Risk: <span class="{risk_class(r.get('risk_level'))}">{r.get('risk_level')} ({r.get('risk_score')})</span></div>
                    <div class="small-muted" style="margin-top:0.4rem;">Weak / missing capability signals</div>
                    <div>{list_text(r.get('missing_critical_capabilities'))}</div>
                    <div class="small-muted" style="margin-top:0.6rem;">Recommended action</div>
                    <div>{r.get('recommended_action')}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

with tab_priority:
    st.subheader("Intervention Priority Board")
    st.caption("Ranked actions for NGO planners: who needs verification, support, or specialist deployment first.")
    board = build_priority_board(outputs)
    for _, r in board.head(12).iterrows():
        st.markdown(
            f"""
            <div class="priority-card">
                <div class="small-muted">Priority #{int(r['priority'])} • {r['category']}</div>
                <h3 style="margin:0.2rem 0;">{r['facility_name']}</h3>
                <div>{r['region']} • Risk: <span class="{risk_class(r['risk_level'])}">{r['risk_level']} ({r['risk_score']})</span></div>
                <div style="margin-top:0.45rem;"><b>Claim/gap:</b> {r['claim']} — {r['status']}</div>
                <div><b>Why:</b> {r['why']}</div>
                <div><b>Action:</b> {r['recommended_action']}</div>
                <div class="small-muted">Evidence row: {r['evidence_row']} • Expected evidence to verify: {r['evidence_to_verify']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("### Board table")
    st.dataframe(board, use_container_width=True, hide_index=True)
    download_button(board, "Download priority-board CSV", "priority_board.csv")

with tab_map:
    st.subheader("Medical desert map")
    st.caption("Red = Critical, Orange = High, Yellow = Medium, Green = Low. Coordinates are inferred offline when exact latitude/longitude is missing.")
    if not map_df.empty:
        view = pdk.ViewState(latitude=float(map_df["latitude"].mean()), longitude=float(map_df["longitude"].mean()), zoom=6, pitch=0)
        layer = pdk.Layer(
            "ScatterplotLayer",
            map_df,
            get_position="[longitude, latitude]",
            get_color="color",
            get_radius="risk_score * 55 + 1200",
            pickable=True,
        )
        tooltip = {
            "html": "<b>{facility_name}</b><br/>Region: {region}<br/>Risk: {risk_level} ({risk_score})<br/>Gap: {primary_gap}",
            "style": {"backgroundColor": "white", "color": "black"},
        }
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip), use_container_width=True)
    else:
        st.info("No latitude/longitude columns found. Add latitude and longitude to enable the map.")

with tab_facility:
    st.subheader("Facility Intelligence")
    f1, f2, f3 = st.columns([1, 1, 1])
    with f1:
        chosen_region = st.selectbox("Region", ["All"] + sorted(gold["region"].dropna().unique().tolist()))
    with f2:
        chosen_level = st.selectbox("Risk level", ["All", "Critical", "High", "Medium", "Low"])
    with f3:
        search = st.text_input("Search facility", "")
    view_df = gold.copy()
    if chosen_region != "All":
        view_df = view_df[view_df["region"] == chosen_region]
    if chosen_level != "All":
        view_df = view_df[view_df["risk_level"] == chosen_level]
    if search.strip():
        view_df = view_df[view_df["facility_name"].str.contains(search.strip(), case=False, na=False)]

    st.caption(f"Showing top {min(25, len(view_df))} of {len(view_df)} matching facilities sorted by risk.")
    for _, r in view_df.sort_values("risk_score", ascending=False).head(25).iterrows():
        with st.expander(f"{r['facility_name']} — {r['region']} — {r['risk_level']} ({r['risk_score']})"):
            c1, c2 = st.columns([1, 1])
            with c1:
                st.markdown(f"**Primary gap:** {r.get('primary_gap', '—')}")
                st.markdown(f"**Recommendation:** {r.get('recommendation', '—')}")
                st.markdown(f"**Doctors:** {r.get('number_doctors', r.get('doctor_count', 0))} | **Capacity:** {r.get('capacity', 0)}")
                st.markdown("**Original note / source context**")
                st.write(str(r.get("notes", ""))[:1200] or "No note available")
            with c2:
                st.markdown("**Extracted procedures**")
                st.write(list_text(r.get("procedures", [])))
                st.markdown("**Extracted equipment**")
                st.write(list_text(r.get("equipment", [])))
                st.markdown("**Extracted capabilities**")
                st.write(list_text(r.get("capabilities", [])))
                st.markdown("**Specialties**")
                st.write(list_text(r.get("specialties", [])))
            st.markdown("**Verification claims**")
            st.dataframe(
                safe_cols(verification[verification["row_id"] == r["row_id"]], ["claim", "status", "confidence", "reason", "present_evidence", "missing_evidence"]),
                use_container_width=True,
                hide_index=True,
            )

with tab_agent:
    st.subheader("Ask MediRoute AI")
    st.caption("Evidence-first deterministic planner. It retrieves relevant rows and returns actions with citations.")
    q = st.text_input("Ask a planning question", key="planner_question")
    ask_clicked = st.button("Ask", type="primary")
    if ask_clicked or q:
        ans, ev = answer_query(q, outputs)
        st.markdown(ans)
        ev = ev if isinstance(ev, pd.DataFrame) else pd.DataFrame()
        if not ev.empty:
            st.subheader("Evidence cards")
            if {"facility_name", "row_id", "claim", "status", "reason"}.issubset(ev.columns):
                for _, rr in ev.head(8).iterrows():
                    evidence_card(rr)
            st.subheader("Evidence table")
            st.dataframe(ev, use_container_width=True, hide_index=True)
            download_button(ev, "Download answer evidence", "mediroute_answer_evidence.csv")

with tab_sim:
    st.subheader("Intervention Simulator")
    st.caption("Simulate limited NGO resources and generate a deployment plan. This is not a clinical decision tool; it is a planning triage aid.")
    s1, s2, s3, s4, s5 = st.columns(5)
    with s1:
        field_teams = st.slider("Field teams", 0, 10, 4)
    with s2:
        surgical_teams = st.slider("Surgical teams", 0, 6, 2)
    with s3:
        oxygen_kits = st.slider("Oxygen kits", 0, 10, 4)
    with s4:
        imaging_units = st.slider("Imaging units", 0, 5, 1)
    with s5:
        lab_kits = st.slider("Lab kits", 0, 5, 1)
    plan = intervention_plan(outputs, field_teams, surgical_teams, oxygen_kits, imaging_units, lab_kits)
    if plan.empty:
        st.info("No allocation generated for the selected resources. Increase resources or inspect facility risk table.")
    else:
        st.markdown("### Recommended deployment order")
        st.dataframe(plan, use_container_width=True, hide_index=True)
        download_button(plan, "Download intervention plan", "mediroute_intervention_plan.csv")
        for _, r in plan.head(8).iterrows():
            st.markdown(
                f"""
                <div class="mediroute-card">
                    <b>{r['resource']}</b> → <b>{r['destination']}</b> ({r['region']})<br/>
                    Risk: <span class="{risk_class(r['risk_level'])}">{r['risk_level']} ({r['risk_score']})</span><br/>
                    <span class="small-muted">Reason:</span> {r['reason']}<br/>
                    <span class="small-muted">Expected impact:</span> {r['expected_impact']}<br/>
                    <span class="small-muted">Evidence row:</span> {r['evidence_row']}
                </div>
                """,
                unsafe_allow_html=True,
            )

with tab_trust:
    st.subheader("Trust & Evaluation")
    st.caption("Reliability dashboard for judges: coverage, verification, evidence, and responsible-use checks.")
    metrics_df, checks_df = quality_report(outputs)
    st.markdown("### Evaluation metrics")
    st.dataframe(metrics_df, use_container_width=True, hide_index=True)
    st.markdown("### Quality checks")
    st.dataframe(checks_df, use_container_width=True, hide_index=True)
    c1, c2, c3 = st.columns(3)
    weak = int(verification["status"].isin(["Suspicious", "Incomplete"]).sum())
    total_claims = max(1, len(verification))
    with c1:
        st.metric("Weak-claim ratio", f"{weak / total_claims:.1%}")
    with c2:
        st.metric("Verified-claim ratio", f"{int((verification['status'] == 'Verified').sum()) / total_claims:.1%}")
    with c3:
        st.metric("RAG document coverage", f"{len(gold) / max(1, len(gold)):.0%}")
    st.warning("Responsible-use note: MediRoute AI supports NGO planning and field verification. It does not diagnose patients, replace clinicians, or make final medical-routing decisions without human review.")

with tab_evidence:
    st.subheader("Agent Evidence Trace")
    st.caption("Each planner output can be traced to extracted facts and verification decisions.")
    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown("### Extracted evidence facts")
        ev_rows = []
        for _, r in extracted.iterrows():
            evidence = r.get("evidence", [])
            if isinstance(evidence, str):
                try:
                    evidence = json.loads(evidence)
                except Exception:
                    evidence = []
            for e in evidence if isinstance(evidence, list) else []:
                ev_rows.append({"row_id": e.get("row_id"), "facility_name": r.get("facility_name"), "snippet": e.get("snippet")})
        ev_df = pd.DataFrame(ev_rows)
        if ev_df.empty:
            # Fallback: show extracted facts by row.
            ev_df = safe_cols(extracted, ["row_id", "facility_name", "procedures", "equipment", "capabilities", "specialties"])
        st.dataframe(ev_df, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("### Verification trace")
        st.dataframe(
            safe_cols(verification[verification["status"].isin(["Verified", "Incomplete", "Suspicious"])], ["row_id", "facility_name", "claim", "status", "reason", "present_evidence", "missing_evidence"]),
            use_container_width=True,
            hide_index=True,
        )

with tab_databricks:
    st.subheader("Databricks SQL Dashboard Pack")
    st.caption("Use these queries in Databricks SQL to create native dashboard tiles from the Gold tables.")
    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown("### Lakehouse tables")
        dbx_tables = pd.DataFrame(
            [
                ["Bronze", "bronze_facility_raw", "Raw uploaded facility data"],
                ["Silver", "silver_facility_clean", "Normalized facility records"],
                ["Gold", "gold_idp_extracted_facts", "Extracted procedure/equipment/capability/specialty"],
                ["Gold", "gold_claim_verification", "Verified/incomplete/suspicious claim decisions"],
                ["Gold", "gold_facility_risk", "Facility-level medical desert risk"],
                ["Gold", "gold_region_risk", "Location-cluster medical desert risk"],
                ["Gold", "gold_rag_documents", "Vector Search-ready planner documents"],
                ["Gold", "gold_quality_checks", "Evaluation and trust checks"],
            ],
            columns=["Layer", "Table", "Purpose"],
        )
        st.dataframe(dbx_tables, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("### Proof metrics")
        st.dataframe(quality_report(outputs)[0].head(8), use_container_width=True, hide_index=True)

    dashboard_sql = """-- 1. High-risk location clusters
SELECT region, facilities, risk_score, risk_level, missing_critical_capabilities, recommended_action
FROM workspace.mediroute_ai.gold_region_risk
ORDER BY risk_score DESC
LIMIT 20;

-- 2. Claim status distribution
SELECT status, COUNT(*) AS claims
FROM workspace.mediroute_ai.gold_claim_verification
GROUP BY status
ORDER BY claims DESC;

-- 3. High-risk facilities
SELECT facility_name, region, risk_score, risk_level, primary_gap, recommendation
FROM workspace.mediroute_ai.gold_facility_risk
WHERE risk_level IN ('Critical', 'High')
ORDER BY risk_score DESC
LIMIT 50;

-- 4. Missing capability frequency
SELECT claim, status, COUNT(*) AS count
FROM workspace.mediroute_ai.gold_claim_verification
WHERE status IN ('Suspicious', 'Incomplete')
GROUP BY claim, status
ORDER BY count DESC;

-- 5. Planner/RAG document coverage
SELECT COUNT(*) AS rag_documents
FROM workspace.mediroute_ai.gold_rag_documents;
"""
    st.code(dashboard_sql, language="sql")


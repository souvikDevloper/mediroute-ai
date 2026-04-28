from __future__ import annotations

import json
import os
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mediroute.pipeline import process_dataframe
from mediroute.planner import answer_query

OFFICIAL = ROOT / "data" / "official" / "virtue_foundation_ghana_v0_3.csv"
SAMPLE = ROOT / "data" / "sample" / "ghana_facility_sample.csv"
PROCESSED_DIR = Path(os.getenv("MEDIROUTE_PROCESSED_DIR", ROOT / "data" / "processed"))

st.set_page_config(page_title="MediRoute AI", page_icon="🏥", layout="wide")

st.markdown(
    """
    <style>
        .main .block-container { padding-top: 3.0rem; max-width: 1400px; }
        [data-testid="stSidebar"] button { width: 100%; white-space: normal; text-align: left; }
        [data-testid="stSidebar"] .stCodeBlock pre { white-space: pre-wrap; word-break: break-word; }
        div[data-testid="stMetricValue"] { font-size: 2rem; }
        .mediroute-card {
            border: 1px solid rgba(250,250,250,0.14);
            border-radius: 14px;
            padding: 1rem 1.1rem;
            background: rgba(255,255,255,0.035);
            margin-bottom: 0.75rem;
        }
        .small-muted { color: rgba(250,250,250,0.62); font-size: 0.92rem; }
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


def list_text(v) -> str:
    if isinstance(v, list):
        return ", ".join(str(x) for x in v) if v else "—"
    if isinstance(v, float) and pd.isna(v):
        return "—"
    if isinstance(v, str) and v.startswith("["):
        try:
            data = json.loads(v)
            return ", ".join(str(x) for x in data) if data else "—"
        except Exception:
            return v
    return "—" if v is None else str(v)


def risk_color(level: str) -> list[int]:
    return {
        "Critical": [210, 55, 55, 190],
        "High": [245, 135, 48, 185],
        "Medium": [235, 196, 68, 180],
        "Low": [65, 155, 95, 175],
    }.get(level, [120, 120, 120, 160])


def evidence_card(row: pd.Series) -> None:
    st.markdown(
        f"""
        <div class="mediroute-card">
            <b>{row['facility_name']}</b> — Row {row['row_id']}<br/>
            <span class="small-muted">Claim:</span> {row['claim']} &nbsp; | &nbsp;
            <span class="small-muted">Status:</span> <b>{row['status']}</b><br/>
            <span class="small-muted">Why:</span> {row['reason']}<br/>
            <span class="small-muted">Missing evidence:</span> {list_text(row.get('missing_evidence', []))}
        </div>
        """,
        unsafe_allow_html=True,
    )


if "planner_question" not in st.session_state:
    st.session_state["planner_question"] = "Which regions lack emergency surgical and maternal care?"

with st.sidebar:
    st.header("Data")
    up = st.file_uploader("Upload facility CSV", type=["csv"])
    use_sample = st.toggle("Use included official Virtue Foundation Ghana data", value=up is None)
    st.caption("Default uses the official VF Ghana v0.3 CSV. Upload another CSV to test your own facility records.")
    st.divider()
    st.markdown("**Suggested questions**")
    suggestions = [
        "Which regions lack emergency surgical and maternal care?",
        "Which facilities need manual verification?",
        "Where should we deploy oxygen and ICU support first?",
        "Which claims are suspicious and why?",
    ]
    for i, s in enumerate(suggestions):
        if st.button(s, key=f"suggestion_{i}", use_container_width=True):
            st.session_state["planner_question"] = s
    st.divider()
    st.markdown("**Databricks V2**")
    st.caption("Bronze/Silver/Gold Delta tables • MLflow trace • Vector Search-ready RAG table")

outputs = run_cached(up.getvalue() if up else None, use_sample)
raw = outputs["raw"]
gold = outputs["gold"]
regions = outputs["regions"]
facilities = outputs["facilities"]
verification = outputs["verification"]
extracted = outputs["extracted"]

# Parse risk colors for map
map_df = gold.copy()
if "latitude" in map_df.columns and "longitude" in map_df.columns:
    map_df["latitude"] = pd.to_numeric(map_df["latitude"], errors="coerce")
    map_df["longitude"] = pd.to_numeric(map_df["longitude"], errors="coerce")
    map_df = map_df.dropna(subset=["latitude", "longitude"])
    map_df["color"] = map_df["risk_level"].map(risk_color)

st.title("🏥 MediRoute AI")
st.caption("Databricks-ready medical desert intelligence: Extract → Verify → Score → Recommend → Cite")

st.markdown(
    """
    <div class="small-muted">
    A healthcare coordination layer for NGOs: parse messy facility notes and official VF fact columns, verify whether capabilities are trustworthy,
    identify medical deserts, and recommend where doctors/equipment should be deployed first with row-level evidence.
    </div>
    """,
    unsafe_allow_html=True,
)

metrics = st.columns(5)
metrics[0].metric("Facilities", len(gold))
metrics[1].metric("Regions", gold["region"].nunique())
metrics[2].metric("Critical/High facilities", int(gold["risk_level"].isin(["Critical", "High"]).sum()))
metrics[3].metric("Weak claims", int(verification["status"].isin(["Suspicious", "Incomplete"]).sum()))
metrics[4].metric("Verified claims", int((verification["status"] == "Verified").sum()))

t1, t2, t3, t4, t5, t6 = st.tabs(
    ["Overview", "Medical Desert Map", "Facility Intelligence", "Ask Agent", "Evidence Trace", "Databricks Trace"]
)

with t1:
    st.subheader("Regional medical desert risk")
    show_regions = regions.copy()
    show_regions["missing_critical_capabilities"] = show_regions["missing_critical_capabilities"].map(list_text)
    st.dataframe(
        show_regions[["region", "facilities", "risk_score", "risk_level", "missing_critical_capabilities", "recommended_action"]],
        use_container_width=True,
        hide_index=True,
    )

    c1, c2 = st.columns([1.05, 1])
    with c1:
        st.subheader("Highest-risk facilities")
        st.dataframe(
            facilities.sort_values("risk_score", ascending=False)[
                ["facility_name", "region", "risk_score", "risk_level", "primary_gap", "recommendation"]
            ].head(10),
            use_container_width=True,
            hide_index=True,
        )
    with c2:
        st.subheader("Claim status counts")
        status_counts = verification["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        st.bar_chart(status_counts, x="status", y="count")

    st.subheader("Priority actions")
    top_regions = regions.sort_values("risk_score", ascending=False).head(3)
    cols = st.columns(3)
    for col, (_, r) in zip(cols, top_regions.iterrows()):
        with col:
            st.markdown(
                f"""
                <div class="mediroute-card">
                <b>{r['region']}</b><br/>
                Risk: <b>{r['risk_level']} ({r['risk_score']})</b><br/>
                Gap: {list_text(r['missing_critical_capabilities'])}<br/><br/>
                <span class="small-muted">Action:</span> {r['recommended_action']}
                </div>
                """,
                unsafe_allow_html=True,
            )

with t2:
    st.subheader("Medical desert map")
    st.caption("Red = Critical, Orange = High, Yellow = Medium, Green = Low")
    if not map_df.empty:
        view = pdk.ViewState(
            latitude=float(map_df["latitude"].mean()),
            longitude=float(map_df["longitude"].mean()),
            zoom=6,
            pitch=0,
        )
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

with t3:
    st.subheader("Facility intelligence")
    regions_filter = sorted(gold["region"].dropna().unique().tolist())
    chosen_region = st.selectbox("Region", ["All"] + regions_filter)
    chosen_level = st.selectbox("Risk level", ["All", "Critical", "High", "Medium", "Low"])
    view_df = gold.copy()
    if chosen_region != "All":
        view_df = view_df[view_df["region"] == chosen_region]
    if chosen_level != "All":
        view_df = view_df[view_df["risk_level"] == chosen_level]

    for _, r in view_df.sort_values("risk_score", ascending=False).head(25).iterrows():
        with st.expander(f"{r['facility_name']} — {r['region']} — {r['risk_level']} ({r['risk_score']})"):
            c1, c2 = st.columns([1, 1])
            with c1:
                st.markdown(f"**Primary gap:** {r['primary_gap']}")
                st.markdown(f"**Recommendation:** {r['recommendation']}")
                st.markdown(f"**Doctors:** {r.get('number_doctors', 0)} | **Capacity:** {r.get('capacity', 0)}")
                st.markdown("**Original note**")
                st.write(r.get("notes", ""))
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
                verification[verification["row_id"] == r["row_id"]][["claim", "status", "confidence", "reason", "missing_evidence"]],
                use_container_width=True,
                hide_index=True,
            )

with t4:
    st.subheader("Ask MediRoute AI")
    q = st.text_input("Ask a planning question", key="planner_question")
    ask_clicked = st.button("Ask", type="primary")
    if ask_clicked or q:
        ans, ev = answer_query(q, outputs)
        st.markdown(ans)
        weak_ev = ev.head(8) if isinstance(ev, pd.DataFrame) else pd.DataFrame()
        if not weak_ev.empty:
            st.subheader("Evidence cards")
            for _, rr in weak_ev.iterrows():
                if {"facility_name", "row_id", "claim", "status", "reason"}.issubset(weak_ev.columns):
                    evidence_card(rr)
        st.subheader("Evidence table")
        st.dataframe(ev, use_container_width=True, hide_index=True)

with t5:
    st.subheader("Agent evidence trace")
    st.caption("Each answer is backed by extracted row-level evidence and verification decisions.")
    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown("### Extracted evidence")
        ev_rows = []
        for _, r in extracted.iterrows():
            evidence = r.get("evidence", [])
            if isinstance(evidence, str):
                try:
                    evidence = json.loads(evidence)
                except Exception:
                    evidence = []
            for e in evidence:
                ev_rows.append(
                    {
                        "row_id": e.get("row_id"),
                        "facility_name": r["facility_name"],
                        "snippet": e.get("snippet"),
                    }
                )
        st.dataframe(pd.DataFrame(ev_rows), use_container_width=True, hide_index=True)
    with c2:
        st.markdown("### Verification trace")
        st.dataframe(
            verification[verification["status"].isin(["Verified", "Incomplete", "Suspicious"])][
                ["row_id", "facility_name", "claim", "status", "reason", "present_evidence", "missing_evidence"]
            ],
            use_container_width=True,
            hide_index=True,
        )

with t6:
    st.subheader("Databricks Trace")
    st.caption("This page explains how the local demo maps to the Databricks Lakehouse version.")

    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown("### Lakehouse tables")
        dbx_tables = pd.DataFrame(
            [
                ["Bronze", "bronze_facility_raw", "Raw uploaded facility data"],
                ["Silver", "silver_facility_clean", "Normalized schema for facility records"],
                ["Gold", "gold_idp_extracted_facts", "Extracted procedure/equipment/capability/specialty"],
                ["Gold", "gold_claim_verification", "Verified/incomplete/suspicious claim decisions"],
                ["Gold", "gold_facility_risk", "Facility-level desert risk"],
                ["Gold", "gold_region_risk", "Region-level medical desert risk"],
                ["Gold", "gold_rag_documents", "Vector Search-ready documents for planner RAG"],
            ],
            columns=["Layer", "Table", "Purpose"],
        )
        st.dataframe(dbx_tables, use_container_width=True, hide_index=True)

    with c2:
        st.markdown("### MLflow metrics to log")
        mlflow_metrics = pd.DataFrame(
            [
                ["facilities", len(facilities)],
                ["regions", gold["region"].nunique()],
                ["weak_claims", int(verification["status"].isin(["Suspicious", "Incomplete"]).sum())],
                ["verified_claims", int((verification["status"] == "Verified").sum())],
                ["critical_high_facilities", int(gold["risk_level"].isin(["Critical", "High"]).sum())],
            ],
            columns=["Metric", "Value"],
        )
        st.dataframe(mlflow_metrics, use_container_width=True, hide_index=True)

    st.markdown("### Databricks run order")
    st.code(
        """databricks/notebooks/00_setup_config.py
 databricks/notebooks/01_bronze_ingest_delta.py
 databricks/notebooks/02_silver_normalize.py
 databricks/notebooks/03_gold_agent_pipeline.py
 databricks/notebooks/04_mlflow_agent_trace.py
 databricks/notebooks/05_vector_search_prep.py
 databricks/notebooks/06_quality_checks.py
 databricks/notebooks/07_export_for_app.py""",
        language="text",
    )

    st.markdown("### Asset Bundle commands")
    st.code(
        """databricks bundle validate -t dev
 databricks bundle deploy -t dev
 databricks bundle run mediroute_ai_pipeline -t dev""",
        language="bash",
    )

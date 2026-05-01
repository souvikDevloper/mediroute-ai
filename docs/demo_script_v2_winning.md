# MediRoute AI — 5 Minute Winning Demo Script

## 0:00–0:25 — Problem

MediRoute AI helps NGOs answer a hard operational question: where does medical capability actually exist, where are claims incomplete, and where should scarce field teams, doctors, oxygen kits, and equipment go first?

## 0:25–0:55 — Product thesis

The product follows a five-step loop: Extract, Verify, Score, Recommend, and Cite. It extracts procedures, equipment, capabilities, and specialties; verifies claims; scores medical desert risk; recommends intervention priorities; and links decisions to row-level evidence.

## 0:55–1:35 — Databricks proof

Show the Databricks notebook list and Gold pipeline output. Mention: 987 facilities processed, 8302 evidence facts extracted, 1117 verification claims generated, 1046 weak claims flagged, 626 critical/high facilities prioritized, and 987 RAG-ready documents created.

## 1:35–2:10 — Not just RAG

Show the architecture. Explain that the system creates structured IDP facts and verification traces before retrieval. Planner answers are based on extracted and verified evidence, not free-form hallucinated summaries.

## 2:10–2:55 — Priority Board

Open the Priority Board tab. Show ranked action cards. Explain how each card gives the facility, region, risk, claim, expected evidence to verify, action, and evidence row.

## 2:55–3:35 — Intervention Simulator

Open the Intervention Simulator. Adjust resource sliders. Show the generated deployment plan for field teams, surgical teams, oxygen kits, imaging units, and lab kits.

## 3:35–4:15 — Trust & Evaluation

Open Trust & Evaluation. Explain extraction coverage, verification coverage, weak-claim ratio, verified-claim ratio, RAG document coverage, and the responsible-use posture.

## 4:15–4:45 — Ask Agent + Evidence

Ask: "Which facilities need manual verification?" Show that answers include priorities, reasons, actions, and evidence table.

## 4:45–5:00 — Impact close

MediRoute AI reduces manual review time and helps organizations like Virtue Foundation route scarce expertise and equipment to communities where missing capability creates the highest risk.

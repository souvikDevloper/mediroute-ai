# Winning Upgrade Plan — MediRoute AI

This version upgrades MediRoute AI from a working Databricks-backed IDP prototype into a judge-facing healthcare decision center.

## Added product layers

1. **Intervention Priority Board**
   - Ranks facilities by weak claims, expected evidence to verify, and risk score.
   - Groups actions into manual verification, surgical/maternal care, oxygen/ICU support, and diagnostics support.
   - Shows the exact row ID and expected evidence to verify that triggered each action.

2. **Intervention Simulator**
   - Lets planners enter limited resources: field teams, surgical teams, oxygen kits, imaging units, and lab kits.
   - Produces a deployment table with destination facility, region, risk score, reason, expected impact, and evidence row.

3. **Trust & Evaluation**
   - Shows extraction coverage, verification coverage, weak-claim ratio, verified-claim ratio, critical/high facilities, and RAG document coverage.
   - Includes responsible-use note: this is for NGO planning and verification, not clinical diagnosis.

4. **Evidence-first Ask Agent**
   - Stronger answer templates for manual verification, surgical/maternal care, ICU/oxygen, diagnostics, and monthly prioritization.
   - Always returns evidence tables for traceability.

5. **Databricks SQL Dashboard Pack**
   - Ready SQL queries to create native Databricks dashboard tiles from Gold tables.

## Demo angle

The upgraded demo should focus on this line:

> MediRoute AI is not just RAG over a spreadsheet. It is a Databricks-backed decision layer that extracts, verifies, scores, recommends, and cites evidence before planner answers are shown.

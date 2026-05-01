# Streamlit Cloud Deployment Notes

## Goal

Deploy the user-facing MediRoute AI dashboard publicly while keeping Databricks as the backend proof pipeline.

## Steps

1. Push the repository to GitHub.
2. Go to Streamlit Community Cloud.
3. Create a new app from the GitHub repository.
4. Use this entrypoint:

```text
app/streamlit_app.py
```

5. Use Python 3.10+.
6. Streamlit Cloud will install from:

```text
requirements-local.txt
```

If Streamlit Cloud expects `requirements.txt`, either keep `requirements.txt` aligned with `requirements-local.txt`, or set the dependency file manually if supported.

## Data

The app includes the official VF Ghana CSV inside:

```text
data/official/virtue_foundation_ghana_v0_3.csv
```

The dashboard can also process an uploaded CSV from the sidebar.

## What to submit

For the hackathon form, submit:

- GitHub repo URL
- Streamlit app URL if deployment succeeds
- Demo video URL
- Databricks screenshots showing notebooks, Delta tables, MLflow trace, RAG table, and quality checks

## Fallback

If deployment fails, submit the GitHub repo and demo video. The video should show the local Streamlit dashboard and Databricks notebook proof.

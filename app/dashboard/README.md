# Clinical Dashboard — Module 4

> **Module 4 of 4** — Streamlit UI over the real-time healthcare pipeline.
> Live risk board, patient detail view, pipeline health monitor.

## Run

```bash
pip install -r app/requirements.txt

# Make sure Redpanda + the ML scorer are running (see top-level quickstart)
streamlit run app/dashboard/clinical_dashboard.py --server.port 8501
```

Then open <http://localhost:8501>.

## Views

1. **Live Risk Board** — rolling buffer of the last 200 readmission predictions from
   `healthcare.predictions`. Auto-refreshes every 5s. Shows risk distribution and
   per-patient SHAP top-5 features.
2. **Patient Detail** — pick a patient, see their demographics (from OMOP),
   conditions, recent visits, and recent streaming vitals — alongside their
   latest readmission risk score.
3. **Pipeline Health** — row counts in the OMOP warehouse, row counts in the
   streaming silver tables, MLflow Production model version, Kafka consumer
   status.

## Architecture

The dashboard is intentionally a **read-only** view. It subscribes to Kafka
with a `latest` offset reset, so it only shows new predictions. It connects
to OMOP and the streaming silver DuckDBs for historical context. It does
**not** write to anything.

The background consumer is started once via `st.cache_resource` and runs in
a daemon thread. Predictions are pushed into a `collections.deque(maxlen=200)`
that's also cached.

## What it does NOT do

- No authentication — production should put this behind an authenticating
  reverse proxy (Cloudflare Access, OAuth proxy, etc.)
- No write operations — patient updates, scoring, model retraining happen
  elsewhere
- No PHI decryption — the dashboard reads `top_feature_contributions` and
  the high-level OMOP fields, not encrypted PHI envelopes. Sensitive fields
  (mrn, birth_datetime, name) are encrypted and not surfaced here.

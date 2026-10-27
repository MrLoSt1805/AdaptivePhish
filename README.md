# AdaptivePhish

**An MLOps-Based Phishing URL Detection System with Temporal Drift Detection and Automated Retraining**

AdaptivePhish is an academic, URL-only phishing detection project. It combines safe dataset ingestion, deterministic lexical features, model comparison, experiment tracking, time-aware evaluation, drift monitoring, governance, and an API deployment path. Dataset URLs are treated as inert text and are never visited.

## Verified status

- PhreshPhish source pinned to revision `eca603e0ecd4f698f8d4d226c088659e72681239`.
- Four local train shards currently verified: 20,413 rows total. The remaining ingestion campaign is separately budgeted and is not claimed complete.
- Phase 3 compared logistic regression, random forest, XGBoost, and LightGBM; its trusted `phase3-v1` artifact remains the champion.
- A later-period evaluation over the verified local scope found critical PSI drift, while the temporal candidate underperformed the champion and was correctly rejected by governance.
- MLflow uses a local SQLite store. FastAPI exposes health, prediction, and Prometheus metrics endpoints. Docker, offline CI, and an AWS deployment boundary are documented.

## Quick start

```powershell
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/unit -v
.\.venv\Scripts\python.exe -B scripts\demo.py
.\.venv\Scripts\python.exe -B scripts\run_api.py
```

Useful documentation:

- [Phase 2 data record](docs/phase2_completion.md)
- [Phase 3 ML pipeline](docs/phase3_ml_pipeline.md)
- [Phases 4–11 implementation record](docs/phase4_to_11_completion.md)
- [Architecture](docs/architecture.md)
- [Deployment guide](docs/deployment.md)

## Reproducibility boundaries

Git contains source code, tests, configuration, documentation, and DVC pointer files—not raw phishing data. No DVC remote is configured, so data cannot yet be retrieved on a clean machine from Git alone. The model artifact is also local and ignored. Live AWS deployment and further dataset ingestion require explicit authorization because they change external state and may incur cost.

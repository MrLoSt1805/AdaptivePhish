# AdaptivePhish architecture

```mermaid
flowchart LR
  HF[Revision-pinned PhreshPhish Parquet] -->|validated HTTP ranges| RAW[Private URL metadata Parquet]
  RAW -->|DVC pointers| PREP[Validation and time-aware preparation]
  PREP --> FEAT[Deterministic URL lexical features]
  FEAT --> TRAIN[Candidate training]
  TRAIN --> MLFLOW[Local MLflow SQLite tracking]
  TRAIN --> GOV[Governance gate]
  GOV -->|approved only| REG[Checksum-verified model registry]
  REG --> API[FastAPI service]
  API --> METRICS[Prometheus aggregate metrics]
  METRICS --> DRIFT[PSI drift evaluation]
  DRIFT --> RETRAIN[Conditional retraining decision]
  RETRAIN --> TRAIN
```

The dataset URLs are inert strings: the project never resolves or visits them.
Raw URL records and model artifacts are excluded from Git. Git holds source,
configuration, documentation, and DVC pointers. A DVC remote remains required
for cross-machine data retrieval.

# AdaptivePhish phases 4–11 implementation record

## Experiment tracking and temporal research

MLflow uses a local SQLite tracking store under ignored `mlruns/`. The temporal
pipeline evaluates a fresh LightGBM candidate and the unchanged Phase 3 champion
on the later side of a day-level chronological split. It reads only verified
local train-shard outputs. The experiment is reproducible with:

```powershell
$env:MLFLOW_DISABLE_AGENT_HINT = '1'
.\.venv\Scripts\python.exe -B scripts\run_temporal_evaluation.py --run
```

The completed local run used 20,413 rows from verified train-000 through
train-003, a 2025-06-25 cutoff, 14,941 earlier rows, and 5,472 later rows. The
Phase 3 champion achieved later-period F1 0.7782 and ROC AUC 0.9420. The temporal
candidate achieved F1 0.7139 and ROC AUC 0.8528, so it is not eligible to replace
the champion. PSI detected critical drift (maximum 0.3074, hostname ratio).
These results do not represent the complete 77-shard dataset.

## Drift, retraining, and governance

The monitoring package implements per-feature Population Stability Index with
stable/warning/critical thresholds. Retraining is triggered only when critical
drift and sufficient labeled examples coexist. Promotion is fail-closed: the
candidate must achieve the configured F1 gain without worse ROC AUC or an
unapproved false-positive-rate increase. Promotion and rollback artifacts use
checksums and an atomic champion pointer. No automatic promotion occurred.

Run the governance comparison after temporal evaluation:

```powershell
.\.venv\Scripts\python.exe -B scripts\evaluate_model_governance.py
```

## Serving, observability, and packaging

FastAPI provides `/health`, `/predict`, and Prometheus `/metrics`. Prediction
responses never echo input URLs. The container uses Python 3.12, a non-root user,
and a health check. The API can be exercised locally with:

```powershell
.\.venv\Scripts\python.exe -B scripts\run_api.py
```

CI installs the declared package, runs the entire offline suite, and performs an
API import smoke test. Deployment guidance exists for AWS EC2, but no AWS
resource or remote registry was created because those actions require account,
security, and billing approval.

## Demonstration

```powershell
.\.venv\Scripts\python.exe -B scripts\demo.py
```

The demo processes two reserved `.invalid` examples locally and prints only
aggregate prediction counts. It needs the trusted local Phase 3 artifact.

## Explicit limitations

- The approved train-004 live ingestion remains blocked by the external action
  authorization boundary; the current verified local scope is four train shards.
- Full 666,315-row temporal conclusions require the separately approved and
  budgeted remaining-shard campaign.
- Collection dates are day-level and are not proof of label-availability time.
- PSI is a covariate-shift signal, not proof that model quality decreased.
- Conditional retraining needs reliable later labels and human governance.
- MLflow is local only; there is no shared tracking server.
- There is no DVC remote, so data and the ignored model artifact cannot yet be
  reproduced on another machine from Git alone.
- Docker and AWS deployment definitions were not executed in this environment.

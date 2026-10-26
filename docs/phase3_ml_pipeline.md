# AdaptivePhish Phase 3: Core URL ML Pipeline

Phase 3 implements a deterministic URL-only training and inference pipeline on the verified 5,780-row PhreshPhish shard. It does not claim temporal generalization, and this document contains no fabricated model results. At the time of implementation, the ML packages were declared but were not installed in the active environment, so real comparison metrics remain pending.

## Architecture

| Component | Responsibility |
|---|---|
| `features/url_features.py` | One deterministic lexical extractor shared by training and inference |
| `pipelines/data_preparation.py` | Verify the Phase 2 artifact, map labels, and create grouped stratified splits |
| `models/training.py` | Construct all four required deterministic candidates |
| `evaluation/metrics.py` | Calculate aggregate metrics and latency |
| `pipelines/training_pipeline.py` | Select on validation F1, evaluate the winner once on test, and publish outputs |
| `models/artifact.py` | Save and load the fixed trusted model bundle and run single-URL inference |

The pipeline first calls the Phase 2 `verify_completed_output()` function. This checks the completion manifest, local Parquet schema, row count, and SHA-256 checksum. It also checks the pinned dataset identity and revision before reading `url`, `label`, and `date`.

## Feature schema

Schema version `url-lexical-v1` contains 26 ordered numeric features:

`url_length`, `hostname_length`, `path_length`, `query_length`, `fragment_length`, `dot_count`, `digit_count`, `hyphen_count`, `slash_count`, `subdomain_count`, `https_flag`, `ip_literal_flag`, `at_sign_flag`, `percent_encoding_count`, `underscore_count`, `equals_count`, `ampersand_count`, `question_mark_count`, `special_character_count`, `digit_ratio`, `hostname_ratio`, `path_ratio`, `query_parameter_count`, `suspicious_keyword_count`, `punycode_flag`, and `explicit_port_flag`.

URLs without schemes are parsed using a temporary local `http://` prefix; their original text is not changed. Empty, malformed, Unicode, and unusually long strings produce bounded numeric features without network activity. The extractor never performs DNS, HTTP, browsing, or URL execution.

HTML, target, label, date, and SHA-256 are never predictive inputs. Date remains attached to each split as nonpredictive metadata for the later temporal phase.

## Random split methodology

The fixed seed is `42`, with requested ratios of 70% training, 15% validation, and 15% test. Exact URL strings are grouped before assignment, so duplicates cannot cross partitions. Groups are shuffled separately within `benign` and `phish`, then assigned deterministically toward the three label-specific targets. Ratios may differ slightly when a duplicate group contains multiple rows.

Labels map exactly as `benign=0` and `phish=1`. Conflicting labels for the same exact URL fail closed. The split is random and approximately stratified; it is not evidence of temporal performance. Phase 5 must perform date-aware experiments.

## Candidate models and selection

All four candidates are required:

1. Logistic Regression inside an sklearn `Pipeline` with `StandardScaler`;
2. Random Forest with 160 trees and bounded depth;
3. XGBoost with 140 modest-depth estimators;
4. LightGBM with 140 modest-depth estimators.

Configurations use one worker where supported and seed 42. No hyperparameter search is performed. If any required dependency is missing, training stops with `ml_dependencies_unavailable`; no candidate is silently omitted.

Each candidate fits only the training partition and is compared on validation data. Primary selection is validation F1, followed deterministically by validation ROC-AUC, validation accuracy, and declared candidate order. After selection, a fresh winner is fitted on training plus validation data and evaluated once on the held-out test partition.

For every validation candidate and the selected test model, reports include accuracy, precision, recall, F1, ROC-AUC when defined, false-positive rate, `tn/fp/fn/tp`, total prediction latency, latency per row, and prediction count. Latency uses single-threaded wall time around `predict_proba` plus `predict` and is machine-dependent.

## Outputs and safety

Training writes aggregate-only reports:

- `reports/results/phase3_metrics.json`;
- `reports/results/phase3_model_comparison.csv`.

These reports contain source identity, seed, split counts, feature schema, aggregate metrics, selection details, and limitations. They never contain raw URLs or sample identifiers.

The winning trusted local bundle is `artifacts/adaptivephish_model.joblib`. It includes the estimator, ordered feature names, schema version, model version, source identity, selected model, and seed. Binary artifacts remain Git-ignored. Joblib/pickle-family files can execute code during loading; `load_trusted_model()` therefore accepts only the fixed local artifact path. Never replace it with an artifact from an untrusted source.

Publication refuses to overwrite existing reports or artifacts unless `--overwrite` is explicitly supplied. Individual files are atomically replaced, but model and report publication is not one multi-file filesystem transaction. Review all outputs after an interrupted run.

## Commands

Install the declared Python 3.12-compatible stack without upgrading unrelated packages deliberately:

```powershell
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\python.exe -m pip install "dvc==3.67.1"
```

Run the offline suite:

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/unit -v
```

Train and compare all candidates:

```powershell
.\.venv\Scripts\python.exe -B scripts/train_models.py
```

Use `--overwrite` only after reviewing existing Phase 3 outputs:

```powershell
.\.venv\Scripts\python.exe -B scripts/train_models.py --overwrite
```

Classify one URL locally. The CLI does not echo the supplied value:

```powershell
.\.venv\Scripts\python.exe -B scripts/predict_url.py "https://example.invalid/path"
```

Inspect the aggregate report:

```powershell
.\.venv\Scripts\python.exe -B scripts/inspect_metrics.py
```

## Future DVC stage design

No `dvc.yaml` has been added. A later approved stage can use the two DVC-tracked Phase 2 files plus feature and pipeline modules as dependencies, run `scripts/train_models.py`, and declare the binary artifact and two aggregate reports as outputs. Parameters should include seed, split ratios, schema version, and candidate configurations. The stage must not invoke live ingestion or overwrite the verified raw data. A DVC remote is still required for cross-machine retrieval.

## Current limitations

- The ML dependencies were unavailable during implementation, so no real training metrics or winner are reported here.
- The 5,780 rows come from one shard and may not represent the complete dataset.
- Random splitting does not measure future-time performance.
- Exact URL grouping prevents exact overlap but not related domains, campaigns, or near-duplicate URLs across splits.
- Threshold selection remains fixed at 0.5; probability calibration is not evaluated.
- The artifact format must be loaded only from a trusted local source.
- Phase 4 MLflow, Phase 5 temporal evaluation, Phase 6 drift/retraining, and later governance, API, Docker, CI/CD, and deployment phases are not implemented.

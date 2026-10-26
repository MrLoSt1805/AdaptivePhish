# AdaptivePhish Phase 2 Completion Report

Phase 2 status as of **2026-10-26**: the first approved PhreshPhish URL-metadata shard has been ingested, validated, audited, and tracked locally with DVC. This phase does not include feature engineering, temporal splitting, model training, or a DVC pipeline.

## Dataset source and fixed scope

The official source is [phreshphish/phreshphish](https://huggingface.co/datasets/phreshphish/phreshphish), pinned to repository revision `eca603e0ecd4f698f8d4d226c088659e72681239`. The immutable source contract is stored in `configs/phreshphish_source.toml`:

| Property | Approved value |
|---|---|
| Split and shard | `train`, `data/train-001.parquet` |
| Remote shard size | 322,018,201 bytes |
| Row groups and rows | 1 row group, 5,780 rows |
| Selected columns | `sha256`, `url`, `label`, `date` |
| Application response-body budget | 2,097,152 bytes |
| Maximum HTTP requests | 32 |

Only one shard was ingested because the full published dataset is approximately 36.6 GB in stored Parquet files and about 193.1 GB as reported expanded data. Starting with one immutable shard made it possible to validate remote range behavior, schema handling, publication safety, and data quality before increasing cost or exposure. This shard is useful for pipeline development, but it does not represent the complete dataset and cannot establish full-dataset date boundaries, duplicate rates, language mix, source mix, or class balance.

## Column-only HTTP access

The reusable reader in `src/adaptivephish/data/remote_parquet.py` uses HTTPS byte-range requests against the revision-pinned shard. It requires HTTP 206 responses, exact `Content-Range` and `Content-Length` headers, identity encoding, approved redirects, approved byte intervals, and bounded request counts. It parses the Parquet trailer and footer, validates column-chunk boundaries, and authorizes only the four selected chunks. The HTML, target, language, and language-score chunks are not authorized.

The controlled preflight measured 390,940 compressed bytes for the four selected column chunks and estimated 393,949 application response-body bytes including Parquet metadata. The successful ingestion stayed within the 2 MiB configured budget.

This counter is an application-level safety guard, not a hard total-network limit. It excludes DNS, TLS, TCP and HTTP headers, redirect bodies that are not consumed, socket buffering, and bytes a transport might consume before raising an exception without exposing a partial body. A five-row preview also does not imply five rows' worth of compressed I/O: Parquet readers may need complete column chunks.

## URL-only research scope

The local artifact contains exactly:

- `sha256`: publisher-provided sample identifier;
- `url`: original URL text;
- `label`: `benign` or `phish`;
- `date`: collection date stored as Arrow `date32`.

HTML is deliberately excluded. Dataset URLs are treated as inert strings: the code never visits, resolves, repairs, or renders them. The current artifact supports later URL-based feature engineering. The identifier and date are provenance and split fields, not automatically approved model inputs.

## Ingestion, validation, and publication

The production flow is implemented across:

- `source_config.py`: validates the immutable source contract;
- `remote_parquet.py`: performs bounded, authorized range reads;
- `ingestion.py`: connects reading, normalization, validation, and output writing;
- `validation.py`: validates schema, values, dates, labels, missing fields, duplicates, and identifier conflicts;
- `output_writer.py`: writes temporary artifacts, verifies schema and checksum, invalidates an old completion marker, atomically replaces Parquet, and publishes the new manifest last.

Text source columns may decode as Arrow `string` or `large_string`. Normalization explicitly checks and converts supported text types into the canonical local `string, string, string, date32` schema without changing values or row order. Unsupported types and unsafe conversions fail with sanitized categories.

The final manifest records source identity, selected columns, row count, label counts, date range, duplicate counts, and the output SHA-256 checksum. `verify_completed_output()` accepts an artifact only when the Parquet schema, row count, manifest completion state, and checksum agree. Publication of the Parquet file and JSON manifest cannot be one filesystem transaction; publishing the completion manifest last makes interruptions fail closed.

## Verified quality results

The read-only audit independently reread and validated the completed local Parquet artifact, then compared its aggregate results with the verified manifest.

| Metric | Result |
|---|---:|
| Total rows | 5,780 |
| Benign | 3,184 |
| Phish | 2,596 |
| Earliest date | 2024-07-02 |
| Latest date | 2025-09-08 |
| Missing required values | 0 |
| Invalid required values | 0 |
| URLs without a scheme | 50 |
| Duplicate identifiers | 0 |
| Duplicate URLs | 0 |
| Exact duplicate rows | 0 |
| Conflicting identifier reuse | 0 |

A missing URL scheme is recorded as a formatting issue rather than repaired or rejected. These results describe only `train-001.parquet`.

## DVC and Git integration

DVC 3.67.1 is initialized. The Parquet artifact and its manifest have separate committed pointer files:

- `data/raw/phreshphish/train-001-url-metadata.parquet.dvc`;
- `data/raw/phreshphish/train-001-url-metadata.manifest.json.dvc`.

The local DVC cache stores content-addressed copies used to restore tracked outputs. Git tracks the small pointer files and ignore rules, while the nested `data/raw/phreshphish/.gitignore` excludes the actual Parquet and manifest. GitHub therefore contains provenance and DVC object hashes rather than raw URL records.

An initial Git ignore design used `/data/raw/*` and a nested `*`. On DVC 3.67.1 for Windows, those wildcard rules caused DVC's discovery walk to prune ancestor directories even though Git allowed the individual `.dvc` filenames. The behavior was reproduced in disposable repositories. The verified correction removed the directory-wide wildcards and uses exact nested rules:

```gitignore
/train-001-url-metadata.parquet
/train-001-url-metadata.manifest.json
```

After the correction, `dvc status` reports that data and pipelines are up to date, and `dvc ls . data/raw/phreshphish --dvc-only` lists the Parquet artifact. Future raw files must receive explicit ignore or DVC tracking rules; directory-wide wildcard rules above DVC pointers must not be reintroduced.

`.dvc/config` is currently empty, so no DVC remote exists. The local cache and output files are available only on this machine. A fresh clone from GitHub receives pointer files but cannot download their corresponding objects until a DVC remote is configured and populated. Remote selection, credentials, access policy, and the first `dvc push` require separate approval.

No `dvc.yaml` or `dvc.lock` exists. DVC currently versions the two artifacts, but it does not yet define ingestion or auditing as reproducible `dvc repro` stages.

## Reproducible local commands

Run commands from the workspace root with the existing virtual environment activated, or use its interpreter explicitly.

Install the declared Phase 2 environment on a new machine:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Run all offline unit tests without contacting the dataset:

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests/unit -v
```

Verify the existing local artifact and print only aggregate quality metrics:

```powershell
.\.venv\Scripts\python.exe -B scripts/audit_ingested_dataset.py
```

Check local DVC state and list tracked outputs:

```powershell
.\.venv\Scripts\dvc.exe status
.\.venv\Scripts\dvc.exe ls . data/raw/phreshphish --dvc-only
```

The ingestion command is deliberately gated and must not be used while the verified output exists:

```powershell
.\.venv\Scripts\python.exe -B scripts/ingest_phreshphish.py --live
```

It refuses to overwrite an already verified completed output. The `--live` command performs network requests and belongs only in a separately approved recovery or re-ingestion procedure. Running the script without exactly `--live` refuses execution before constructing the network reader.

## Completion checklist

Completed and verified:

- [x] Official dataset and immutable source revision documented.
- [x] One approved shard inspected through bounded, column-only HTTP ranges.
- [x] HTML and all unselected column chunks excluded by byte-range authorization.
- [x] Immutable source configuration and fail-closed preflight implemented.
- [x] Arrow schema compatibility and canonical local schema verified.
- [x] Required values, labels, dates, URL formatting, duplicates, and conflicts validated.
- [x] Temporary writing, checksum verification, completion manifest, and fail-closed publication implemented.
- [x] Read-only aggregate quality audit implemented.
- [x] Dataset and manifest tracked by separate DVC pointers.
- [x] Git ignore rules protect actual data while allowing DVC discovery.
- [x] Offline tests block real sockets and use synthetic records.

Items requiring a decision before Phase 3 claims cross-machine reproducibility:

- [ ] Select and configure a private DVC remote, then upload the two DVC objects under an approved credential policy.
- [ ] Decide whether to create `dvc.yaml` ingestion and audit stages now or in the later pipeline phase. No pipeline should be claimed until this exists and reproduces safely.
- [ ] Decide whether one shard is sufficient for Phase 3 feature prototyping or whether additional revision-pinned metadata shards require separate bounded ingestion approval.

Limitations that remain documented:

- The shard is not representative of the complete PhreshPhish dataset.
- Its observed date range does not prove a valid full-dataset chronological boundary.
- Collection dates are day-level and do not prove label-availability time.
- URL-only modeling excludes potentially useful HTML and language information by design.
- Application-body accounting is not a hard network-transfer cap.
- DVC objects currently exist only in the local cache; GitHub pointers alone cannot restore them.
- No DVC pipeline, feature engineering, temporal split, model, MLflow experiment, drift detector, API, container, CI/CD workflow, or deployment has been implemented.

Phase 3 must begin only after the open data-scope and reproducibility decisions are recorded. This report does not implement Phase 3.

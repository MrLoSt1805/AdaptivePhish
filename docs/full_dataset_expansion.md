# AdaptivePhish Controlled Dataset Expansion

This workflow supports progressive, revision-pinned ingestion. The verified local outputs currently cover `train-000` through `train-003`, containing 20,413 rows. `train-004` is footer-verified and approved but is not yet ingested. The claimed 666,315-row complete dataset remains unverified locally.

## Current inventory status

`configs/phreshphish_inventory.json` is marked `metadata_complete`: all 77 pinned paths and advertised file sizes are known. `train-000` through `train-004` are `footer_verified`; 72 shards still have repository metadata only. The document records 666,315 as an expected publisher total, not a locally completed result.

Unknown shard names, sizes, row counts, budgets, and row-group counts are never guessed. An inventory can be marked `complete_verified` only when it contains exactly 77 unique revision-pinned paths, every row count is known, and their sum is 666,315.

## Safe expansion flow

1. Discover the official pinned repository's Parquet filenames and advertised sizes using the explicit live discovery command.
2. Review that output and add one shard at a time to the tracked inventory. Row and row-group counts remain unknown until its footer preflight succeeds.
3. Run metadata-only preflight for the explicit shard. This fetches the four-byte header, eight-byte trailer, and footer only.
4. Record the reported row count, row groups, compressed selected-column bytes, request count, and exact estimated application-body bytes in the approved inventory.
5. Add exact Git-ignore entries for the proposed local Parquet and manifest. Do not add wildcard rules above DVC pointer files.
6. Run ingestion with the same exact preflight estimate as an approval token. A changed estimate fails closed.
7. Run aggregate verification before training or DVC tracking.
8. Run `dvc add` manually only after reviewing the verified local files.

Every source URL is constructed from dataset `phreshphish/phreshphish`, revision `eca603e0ecd4f698f8d4d226c088659e72681239`, and an approved inventory path. The command does not accept arbitrary URLs.

## Reader protections

The workflow reuses `RangeFile`, including HTTPS destination allowlisting, exact HTTP 206, exact `Content-Range`, exact `Content-Length`, identity encoding, redirect limits, no retries, cache accounting, approved-range enforcement, and fail-closed transport errors.

Preflight validates all row groups and all column chunks before authorizing data. It rejects external column files, invalid boundaries, overlapping chunks, missing selected columns, selected/forbidden overlap, unexpected dimensions, excessive requests, and excessive transfer estimates. Ingestion authorizes only `sha256`, `url`, `label`, and `date`. HTML and all other chunks remain forbidden.

Each shard has its own response-body budget and request limit. The command also receives the remaining session budget. For multiple invocations, subtract each successful command's reported `application_body_bytes` from the operator-approved session total and pass only the remainder to the next command.

These counters measure bytes returned to the application. They are not hard network-transfer limits and exclude DNS, TLS, TCP and HTTP headers, some redirect traffic, socket buffers, and bytes hidden by transport failures.

## Publication and resumption

Decoded records use batches of 1,024 rows. Selected compressed chunks are cached so PyArrow cannot request unapproved ranges. Validation enforces the canonical four-column schema, required values, exact labels, dates, identifiers, and sanitized duplicate counts.

Publication reuses the Phase 2 temporary-file, checksum, schema, completion-manifest, and manifest-last protocol. If an output or sidecar already exists, ingestion never overwrites it. A matching completed shard returns `verified_existing` without any network request. An incomplete or mismatched output requires manual investigation.

The current implementation materializes normalized batches for one shard before publication because the Phase 2 validator performs complete-dataset conflict checks. It does not hold HTML or full remote shards. A later optimization may add a disk-backed validation index if individual metadata shards become too large for memory.

## Duplicate policy

`scripts/verify_expanded_dataset.py` recomputes cross-shard and cross-split exact duplicate counts without printing values:

- duplicate SHA-256 identifiers;
- duplicate exact URLs;
- duplicate exact rows;
- identifiers reused with conflicting URL, label, or date.

Duplicates are reported and preserved during raw ingestion. Conflicting identifier reuse is a hard stop before preparation. Publisher train and test identities stay separate. The publisher test split must never enter feature selection, candidate selection, preprocessing fitting, or retraining decisions.

Phase 3 continues to use the original single-shard loader. `load_verified_expanded_rows()` is an inactive future entry point that refuses a partial inventory and preserves publisher train/test partitions. Switching training to it requires a separately reviewed evaluation design and must not overwrite the existing model or metrics.

## Commands

All live commands refuse execution without `--live`. The following are examples for the already approved `train-001` entry; do not run them for unapproved entries.

Discover pinned repository filenames and sizes:

```powershell
.\.venv\Scripts\python.exe -B scripts/discover_phreshphish_inventory.py --live
```

Preflight one inventory shard with a 2 MiB remaining session budget:

```powershell
.\.venv\Scripts\python.exe -B scripts/preflight_dataset_shard.py --live --shard train-001 --session-budget 2097152
```

After reviewing the preflight, pass its exact estimate to ingestion:

```powershell
.\.venv\Scripts\python.exe -B scripts/ingest_dataset_shard.py --live --shard train-001 --approved-estimate <EXACT_PREFLIGHT_ESTIMATE> --session-budget 2097152
```

The existing verified `train-001` output is detected and skipped without network access.

Verify all locally available approved shards:

```powershell
.\.venv\Scripts\python.exe -B scripts/verify_expanded_dataset.py
```

After a new shard is fully verified, track its two artifacts explicitly:

```powershell
.\.venv\Scripts\dvc.exe add data/raw/phreshphish/<SHARD>-url-metadata.parquet
.\.venv\Scripts\dvc.exe add data/raw/phreshphish/<SHARD>-url-metadata.manifest.json
git add data/raw/phreshphish/.gitignore
git add data/raw/phreshphish/<SHARD>-url-metadata.parquet.dvc
git add data/raw/phreshphish/<SHARD>-url-metadata.manifest.json.dvc
```

Review `git check-ignore` and `dvc status` before committing. No DVC remote exists, so pointers alone do not make objects retrievable on another machine.

## Capacity and limitations

The official repository stores approximately 36.6 GB of Parquet, largely because of HTML. Column-only transfer should be much smaller, but the total is unknown until every footer is inspected. Do not multiply the first shard's 390,940 selected compressed bytes by 77 and present it as verified capacity: chunk sizes and row-group layouts vary.

Local URL-metadata Parquet size, DVC cache duplication or linking behavior, temporary publication space, cumulative transfer, validation memory, and runtime must be measured progressively. Maintain free space for the temporary output and final output simultaneously during publication.

No live discovery, metadata preflight, new ingestion, `dvc add`, DVC remote configuration, model retraining, commit, push, or upload was performed while implementing this foundation.

## Resumable bounded batch workflow

`scripts/batch_dataset_shards.py` wraps the existing single-shard reader,
preflight, ingestion writer, and aggregate verifier. It does not implement a
second download path. A batch has four separate steps:

1. **Dry-run selection** chooses exact keys, a publisher split, and a maximum
   shard count without network access.
2. **Metadata preflight** reads only headers, trailers, and footers. It writes
   `configs/phreshphish_batch_plan.json` with status `pending_review`.
3. **Offline approval** validates the plan against the pinned inventory,
   records exact row counts, row groups, estimates and budgets, adds exact
   output ignore rules, and seals the plan with a SHA-256 approval ID.
4. **Bounded ingestion** requires that approval ID. Each footer is checked
   again and must produce the same dimensions and byte estimate. Existing
   verified outputs are skipped. Aggregate verification runs after every new
   atomic publication.

Only one active plan file is allowed. After a batch is complete and reviewed,
archive or commit its approved plan before removing it to prepare the next
batch. Re-running ingestion with the same approved plan is the supported
resume operation: completed outputs are checksum-verified and skipped.

The ignore file, inventory, and approval plan are separate files and cannot be
replaced in one filesystem transaction. Approval publishes the plan last, so
partial approval cannot authorize ingestion. Extra exact ignore rules may
remain after an interrupted approval; they do not expose data or authorize a
source.

The wall-clock limit is checked between shards and after each shard. An active
HTTPS request uses the reader's existing 30-second timeout, so the batch timer
cannot interrupt a system call at an exact instant. Application-body counters
still are not hard total-network caps.

`planned_requests` in a plan counts logical byte ranges: three metadata ranges
plus one range for each selected column chunk. Actual HTTP request counts also
include redirect hops. There are no automatic retries. Each approved shard
therefore reserves its inventory `maximum_http_requests` value (currently 32),
and the batch session request limit must cover the sum of those reservations.
Unused reservations from checksum-verified skipped shards do not consume the
resumed session. Future plans also record `approved_http_request_budget` per
shard explicitly.

### Safe commands

Preview the next three unverified train shards without network access:

```powershell
.\.venv\Scripts\python.exe -B scripts\batch_dataset_shards.py dry-run --split train --max-shards 3
```

Preflight those three shards with explicit independent limits:

```powershell
.\.venv\Scripts\python.exe -B scripts\batch_dataset_shards.py preflight --live --split train --max-shards 3 --per-shard-byte-limit 50000000 --session-byte-limit 150000000 --session-request-limit 96 --runtime-seconds 300
```

Review the pending JSON before approval. Approval performs no network access:

```powershell
Get-Content configs\phreshphish_batch_plan.json
.\.venv\Scripts\python.exe -B scripts\batch_dataset_shards.py approve
```

Copy the printed approval ID into the bounded ingestion command. Choose session
limits no larger than the reviewed plan totals:

```powershell
.\.venv\Scripts\python.exe -B scripts\batch_dataset_shards.py ingest --live --approval-id <APPROVAL_ID> --session-byte-limit <REVIEWED_BYTES> --session-request-limit <REVIEWED_REQUESTS> --runtime-seconds <REVIEWED_RUNTIME_SECONDS>
```

Verify current local outputs without network access:

```powershell
.\.venv\Scripts\python.exe -B scripts\batch_dataset_shards.py verify
```

Resume an interrupted batch by repeating the same ingestion command with the
same approved plan and approval ID. Never edit an approved plan: its digest
will fail.

After reviewing each new Parquet and manifest, DVC tracking remains explicit:

```powershell
.\.venv\Scripts\dvc.exe add data/raw/phreshphish/<SHARD>-url-metadata.parquet
.\.venv\Scripts\dvc.exe add data/raw/phreshphish/<SHARD>-url-metadata.manifest.json
git check-ignore -v --no-index data/raw/phreshphish/<SHARD>-url-metadata.parquet
git status --short
.\.venv\Scripts\dvc.exe status
```

Review the generated `.dvc` pointers and nested `.gitignore` changes before
staging them. The workflow never runs `dvc add`, Git commands, training, or
model publication automatically.

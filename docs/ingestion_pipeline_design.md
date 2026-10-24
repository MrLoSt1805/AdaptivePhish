# AdaptivePhish Dataset Ingestion Pipeline Design

Phase 2, Step 2.10. Design date: **2026-10-24**.

## Purpose

This document describes how to turn the controlled PhreshPhish Parquet proof of concept into a reusable ingestion pipeline. Ingestion means reading an approved part of the source dataset, validating it, and writing a reproducible local copy for later preparation. It does not include feature engineering, model training, chronological splitting, or DVC setup.

The first pipeline version should remain deliberately small. It should process **one already tested Parquet shard**, not the full 36.6 GB repository.

## Source contract

The pipeline must treat its source as an immutable input contract:

| Item | Initial value |
|---|---|
| Dataset | [phreshphish/phreshphish](https://huggingface.co/datasets/phreshphish/phreshphish) |
| Repository revision | `eca603e0ecd4f698f8d4d226c088659e72681239` |
| Split | `train` |
| Shard | `data/train-001.parquet` |
| Expected remote file size | 322,018,201 bytes |
| Expected row groups | 1 |
| Expected rows | 5,780 |
| Selected columns | `sha256`, `url`, `label`, `date` |
| Excluded source columns | `target`, `lang`, `lang_score`, `html` |

The revision, path, size, schema, and expected metadata must be configuration values checked before data is accepted. The pipeline must not silently switch to `main`, another shard, or an auto-converted file.

## What is already implemented and verified

The following capabilities exist as controlled proof-of-concept scripts and tests:

- `RangeFile` provides a read-only, seekable interface backed by validated HTTP byte-range requests.
- It requires HTTP 206, exact `Content-Range`, exact `Content-Length`, and identity content encoding.
- It rejects HTTP 200 full-file responses, unsafe redirects, unapproved ranges, malformed responses, and budget overruns.
- Parquet trailer and footer metadata can be fetched and parsed locally.
- Column-chunk offsets are checked against file boundaries before authorization.
- Only the four selected chunks can be authorized; HTML and other column chunks remain blocked.
- PyArrow runs with pre-buffering and threads disabled for the controlled read.
- Offline tests cover projection, cache behavior, budgets, malformed metadata, overlap, and accidental HTML reads.
- A controlled live read returned five records from the pinned shard without printing or saving raw URLs. It measured 393,945 application response-body bytes.

These scripts prove that the access method works for the pinned shard. They are not yet a reusable ingestion command, configuration system, validator, atomic writer, or DVC pipeline.

## Reuse instead of duplication

The ingestion implementation should move stable reader behavior into package code, for example under `src/adaptivephish/data/`, and make both diagnostic scripts and the future ingestion command call it. There should be one implementation of:

- approved-host and redirect validation;
- HTTP range response validation;
- application-body and request accounting;
- footer parsing;
- column-chunk boundary and overlap validation;
- exact range authorization;
- sanitized failure categories.

`RangeFile` and its validated planning functions should be reused or carefully extracted without changing their behavior. Copying the same safety logic into a new downloader would allow the copies to drift apart. The existing scripts can remain small entry points around the shared implementation.

The reusable API should accept an explicit immutable source specification and return a table plus a sanitized run report. It must not accept arbitrary URLs by default. Any later support for more shards should use an approved manifest containing exact revision-pinned paths, expected sizes, splits, and budgets.

## Initial ingestion scope

The first approved ingestion run should read the single pinned `train-001.parquet` shard and materialize all 5,780 rows for only the four selected columns. The known combined compressed size of those columns is 391,940 bytes. Footer and trailer bytes are additional.

This scope is useful because it is large enough to test validation and local storage, but close to the live proof already completed. It does not establish the quality, date boundaries, duplicate rate, or class balance of the complete dataset.

Expansion should happen one explicit shard at a time. Before adding a shard, inspect its footer, record its expected metadata, estimate selected-column bytes, run offline tests, and approve a suitable budget. A request such as “read every train shard” must not be the default behavior.

## Transfer and request controls

For the first ingestion run, retain these limits:

- **Application response-body budget:** 2 MiB, or 2,097,152 bytes, for trailer, footer, and selected column chunks combined.
- **HTTP request limit:** 32 requests, including redirects counted by `RangeFile`.
- **Retries:** none during the initial implementation.
- **Allowed ranges:** final trailer, calculated footer, and the exact four selected column chunks only.
- **PyArrow settings:** `pre_buffer=False`, `buffer_size=0`, `use_threads=False`, first row group only.

Preflight must calculate the expected selected compressed bytes and compare them with the remaining budget before any column data request. A five-row batch limit controls how many records enter output logic; it does not limit compressed bytes fetched. For ingestion of all 5,780 approved rows, the same selected chunks are sufficient because the shard contains one row group.

The body counter is an application-level guard, not a hard limit on total network traffic. It does not count request and response headers, DNS, TLS, TCP overhead, redirect traffic not read as bodies, socket buffers, or bytes a transport consumed before raising an exception without exposing a partial payload. If byte accounting becomes uncertain, the run must stop and report `transport_unmetered`. A true network cap would require a separately validated network control or proxy.

## Ingestion flow

1. Load the immutable source specification and refuse unknown hosts, paths, revisions, columns, or file sizes.
2. Fetch and validate the final eight-byte Parquet trailer.
3. Calculate, authorize, fetch, and locally parse the footer.
4. Verify schema, one row group, expected row count, and every column-chunk boundary.
5. Prove that selected ranges do not overlap `target`, `lang`, `lang_score`, or `html`.
6. Preflight selected bytes against the remaining application budget.
7. Authorize and fetch exactly the four selected chunks. Cache them so PyArrow does not issue duplicate HTTP reads.
8. Decode the row group in bounded record batches. Batch size limits memory and validation work, although complete selected chunks may already be cached.
9. Validate each batch and accumulate sanitized counts. Never log row values.
10. Write to a temporary file inside the intended ignored data directory, validate the completed file, then atomically rename it to its final name. A failed run must not leave a file that appears complete.
11. Write a separate sanitized manifest containing provenance, counts, software versions, output checksum, validation summary, and measured application-body bytes.

Steps 8–11 are planned and have not been implemented.

## Data validation rules

The four fields are required for every accepted row:

| Field | Validation |
|---|---|
| `sha256` | Non-null string matching exactly 64 hexadecimal characters. Treat it as the publisher's sample identifier; do not claim it was independently recomputed. |
| `url` | Non-null, non-empty string. Preserve the original text exactly. Parsing checks must be local and must not resolve or open it. A missing scheme is a recorded formatting issue, not silently repaired in raw output. |
| `label` | Exact string `benign` or `phish`. Any other value fails validation. Numeric conversion belongs in a later preparation stage. |
| `date` | Non-null valid calendar date represented as Arrow `date32` in storage. Reject invalid values; do not invent or impute dates during ingestion. |

The validator should calculate, without printing values:

- total rows and missing count per field;
- label counts;
- earliest and latest dates;
- invalid identifier, label, date, and URL-format counts;
- exact duplicate `sha256` count;
- exact duplicate URL count;
- count of identifiers associated with conflicting URL, label, or date values.

The raw selected-column output should preserve valid source rows. Exact duplicate rows should be reported rather than silently removed. Conflicting reuse of the same identifier must fail the run. A later preparation stage may deduplicate using a documented rule and produce a separate interim dataset. Cross-shard and cross-split duplicate checks become mandatory when the scope grows beyond one shard.

## Safe URL handling

Dataset URLs are data, not navigation instructions. The ingestion pipeline must:

- never issue requests to URLs stored in the dataset;
- never render them as clickable links;
- never print them in logs, exceptions, test output, notebooks, or CI output;
- never include raw values in Git-tracked reports or manifests;
- parse strings locally only when computing validation flags;
- preserve original values in the ignored data artifact;
- report only aggregate counts and sanitized failure categories.

Tests should use reserved `.invalid` domains and invented identifiers. If a validation library raises an exception containing a raw value, the public error handler must replace it with a fixed category.

## Proposed local output

Write the selected raw data as Parquet because it preserves the date type, supports column projection, compresses repeated values, and is already supported by PyArrow.

Proposed location:

```text
data/raw/phreshphish/train-001-url-metadata.parquet
```

Proposed schema:

| Column | Local type | Null allowed |
|---|---|---:|
| `sha256` | Arrow string | No |
| `url` | Arrow string | No |
| `label` | Arrow string | No |
| `date` | Arrow `date32` | No |

Keep source order and original string values. Do not add HTML. Provenance belongs in a sanitized sidecar manifest, for example `data/raw/phreshphish/train-001-url-metadata.manifest.json`, containing no row values. Suggested manifest fields include dataset name, pinned revision, source split and shard, expected and observed sizes, selected columns, row count, label counts, date range, validation counts, application-body bytes, HTTP request count, PyArrow version, output SHA-256 checksum, and completion status.

The repository's `.gitignore` already excludes contents under `data/raw/` while allowing DVC pointer files. Before implementation, verify the exact final paths with `git check-ignore`. Raw Parquet and any manifest containing sensitive operational detail must stay outside ordinary Git tracking. Sanitized aggregate documentation may be committed separately.

## Later DVC integration

DVC has not been initialized. In a later approved phase, it should track data artifacts and the preparation process without placing large or sensitive data in Git.

A practical future design is:

- add the immutable source manifest and ingestion parameters as tracked configuration;
- track the local raw Parquet artifact with a `.dvc` pointer or a `dvc.yaml` stage;
- define an ingestion stage that produces the raw selected-column Parquet and sanitized run manifest;
- define separate validation and preparation stages that produce reports and `data/interim/` outputs;
- let `dvc.lock` record command, dependency, parameter, and output hashes;
- configure a suitable private DVC remote only after credentials and access rules are approved;
- keep credentials in ignored environment or local DVC configuration, never in Git.

DVC reproducibility does not make a changing web source immutable by itself. Reproduction depends on the pinned dataset revision remaining available and the source manifest matching the expected file.

## Offline tests required before larger ingestion

Before increasing live scope, the test suite should cover:

- configuration rejects mutable revisions, unknown hosts, unexpected paths, sizes, splits, and columns;
- HTTP 200 fallback, malformed or duplicate headers, unsafe redirects, truncated and oversized bodies, request limits, and uncertain byte accounting;
- trailer and footer size validation;
- schema mismatch, missing fields, multiple or empty row groups, external column files, invalid offsets, and overlapping chunks;
- selected ranges never intersect any unselected column range;
- PyArrow makes no unexpected HTTP request after selected chunks are cached;
- cumulative budget includes metadata and every selected chunk;
- batch validation for nulls, invalid labels, invalid dates, malformed identifiers, and unusual URL formatting;
- duplicate and conflicting-identifier detection across batch boundaries;
- sanitized diagnostics cannot contain raw URLs, identifiers, HTML, signed redirects, tokens, or exception text;
- temporary output is removed or clearly marked incomplete after failure;
- final Parquet schema, row count, checksum, and atomic rename behavior;
- deterministic output from the same pinned input and tool versions.

Use only synthetic Parquet data for these tests and block socket connections. Include large synthetic HTML so accidental HTML-range reads are detectable.

## Success criteria

The first ingestion run succeeds only when all of the following are true:

- the pinned source identity, file size, trailer, footer, schema, row-group count, and row count match expectations;
- every HTTP data response passes range validation;
- application-body usage and request count stay within configured limits, with complete accounting;
- only the trailer, footer, and four selected column chunks are requested;
- no selected range overlaps an unselected column;
- exactly 5,780 records pass required-field, label, date, and identifier validation;
- no identifier has conflicting row data;
- the local Parquet output has exactly the four approved columns and no nulls;
- the final output checksum and sanitized provenance manifest are written only after complete validation;
- logs and reports contain no raw URLs, identifiers, HTML, secrets, signed URLs, or raw exception text.

## Failure conditions

Stop without publishing a completed output when any of these occurs:

- source revision, path, size, schema, row count, or row-group count differs;
- a response is not an exact validated HTTP 206 range response;
- a redirect target is outside the allowlist;
- footer or column metadata is invalid, external, overlapping, or outside file boundaries;
- PyArrow requests any unauthorized byte range;
- estimated selected bytes exceed the remaining budget;
- request limit, body budget, or accounting certainty is lost;
- required data is missing or invalid;
- a label is outside `benign` and `phish`;
- duplicate identifiers contain conflicting data;
- the output schema, row count, or checksum validation fails;
- an error path cannot guarantee that partial output is distinguishable from complete output.

Failures should use fixed categories and aggregate counts. They must not expose raw records or transport details.

## Planned implementation sequence

1. Extract the tested range reader and metadata planner into `src/adaptivephish/data/` without changing behavior.
2. Add an immutable source-manifest model and configuration validation.
3. Add streaming batch validation and sanitized aggregate reporting.
4. Add temporary Parquet writing, final validation, checksum generation, and atomic promotion.
5. Add offline tests for the complete pipeline and failure cleanup.
6. Run one approved ingestion of the pinned shard under the 2 MiB application-body budget.
7. Review the resulting aggregate report before proposing another shard.
8. Introduce DVC only in its separately approved project phase.

## Current status

The safe remote range reader, metadata inspection, four-column projection, and one five-record live proof are implemented and tested. Reusable package APIs, full-shard materialization, data validation, local output writing, provenance manifests, multi-shard collection, and DVC integration are **planned only**.

No production code, network request, package installation, DVC initialization, commit, or push was performed for this design step.

# AdaptivePhish: Column-Selective Dataset Access Strategy

Phase 2, Step 2.3. Investigated on **2026-10-24**.

## Decision

Remote Parquet column selection is a documented alternative to the unreliable Dataset Viewer `/rows` requests. The official Parquet listing worked, and a revision-pinned shard's HTTP headers indicated byte-range support. **A four-column record read was not performed:** neither DuckDB nor PyArrow is installed in the project's environment, and no verified strict transfer limiter was available. No claim is made that a 50 MB limit was enforced for a Parquet reader.

Read [dataset_investigation.md](dataset_investigation.md) and [dataset_sample_audit.md](dataset_sample_audit.md) first. The prior audit's 17 HTTP 500 responses motivate this alternative; they do not prove that the underlying Parquet files are inaccessible.

## How selected-column access works

Parquet stores each column separately within groups of rows. Its footer contains metadata locating those column chunks. A remote reader can fetch the footer, then request only the byte ranges containing selected columns. This avoids requiring a whole-file download. Fetching the footer is different from merely reading HTTP headers. [Apache Parquet file format](https://parquet.apache.org/docs/file-format/)

The desired projection is exactly `sha256`, `url`, `label`, and `date`. A projection tells the reader which columns to retrieve. It must be applied at the Parquet reader, not after loading all columns.

| Method | Documented behavior | Does it avoid the large HTML payload? | Status here |
|---|---|---|---|
| Hugging Face `/parquet` endpoint | Lists Parquet URLs, split/configuration, filenames, and sizes; it is not a four-column query endpoint. | Listing alone transfers no table rows. Opening a listed file normally could still download the whole shard. | Listing tested successfully. |
| DuckDB over HTTPS with `httpfs` | Uses Parquet metadata and HTTP ranges to read columns required by the query. | Yes in the documented partial-reading path; not a requirement to transfer the whole shard. Actual byte ranges need measurement. | Documentation checked; engine not installed or run. |
| PyArrow with a seekable HTTP range-backed source | `ParquetFile` supports selected-column and row-group reads through a file-like source. | Can avoid HTML column chunks if the underlying source genuinely performs partial reads. A whole-file download wrapped in memory would not qualify. | Documentation checked; engine not installed or run. |
| Download shard first, select columns afterward | Selection occurs after the file transfer. | No: HTML bytes have already been downloaded. | Rejected for this task. |

Hugging Face publishes converted Parquet paths under `refs/convert/parquet`; the listing is discovery metadata, not a promise of a small download. This dataset also has original Parquet shards under its source revision. [Hugging Face Parquet endpoint documentation](https://huggingface.co/docs/dataset-viewer/parquet)

DuckDB documents projection pushdown and remote range access. A future query could select the four columns from one explicit shard, but `LIMIT 10` controls output rows, not network bytes. A memory limit also does not provide a network quota. Required components would include DuckDB and its `httpfs` extension; extension acquisition must be approved rather than silently auto-installed. [DuckDB HTTP access](https://duckdb.org/docs/stable/core_extensions/httpfs/https), [Parquet projection](https://duckdb.org/docs/stable/data/parquet/overview)

For PyArrow, use `read_row_group(..., columns=[...])` or `iter_batches(..., columns=[...])` over a genuinely seekable remote source. Its `pre_buffer` option can coalesce reads and issue them in parallel. Explicitly control buffering and threads for a measured test; defaults depend on the installed version. Batch size limits returned rows, not compressed bytes fetched. [PyArrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)

An HTTP filesystem adapter such as fsspec is one possible source; it introduces its own HTTP dependency and caching/block behavior. Arrow documents integration with fsspec filesystems. A custom budgeted seekable adapter is another proposal, requiring careful implementation and validation. Neither was built here. [Arrow filesystem integration](https://arrow.apache.org/docs/python/filesystems.html), [fsspec HTTP filesystem](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.http.HTTPFileSystem)

Column selection does not mean literally zero bytes related to other columns: the footer describes the full schema and may contain statistics. Reader buffering, range coalescing, or filesystem read-ahead may fetch adjacent bytes. Avoiding the large HTML column's data chunks must be demonstrated through actual range logs, not inferred solely from the returned table's columns.

## Verified tests and transfer observations

Only Python standard-library networking was used. The initial sandbox HEAD request failed with Windows socket error 10013. The approved network-enabled checks below succeeded. No Parquet GET request was issued.

### Official listing

Endpoint: [PhreshPhish Parquet listing](https://datasets-server.huggingface.co/parquet?dataset=phreshphish%2Fphreshphish).

- HTTP status: **200**.
- JSON response body read by the application: **18,495 bytes**.
- Listed files: **77**; `partial: false`; `pending` and `failed` were empty lists.
- First entry: configuration `default`, split `test`, filename `0000.parquet`, size **55,340,975 bytes**, under the converted Parquet branch.
- The metadata body read had a 1 MiB plus one-byte oversize-detection cap. This was an application read cap, not a transport-level quota. No listed shard was downloaded.

### Revision-pinned shard header checks

Source: [train-001.parquet at the previously recorded revision](https://huggingface.co/datasets/phreshphish/phreshphish/resolve/eca603e0ecd4f698f8d4d226c088659e72681239/data/train-001.parquet).

Revision: `eca603e0ecd4f698f8d4d226c088659e72681239`. This is the pinned revision from the preceding investigation, not a newly asserted latest revision. The final response host was `us.aws.cdn.hf.co`; temporary signed redirect URLs were not recorded.

| Check | Ordinary HEAD | HEAD with `Range: bytes=-8` and `Accept-Encoding: identity` |
|---|---|---|
| Status | 200 | 206 Partial Content |
| `Accept-Ranges` | `bytes` | `bytes` |
| `Content-Length` | 322,018,201 | 8 |
| `Content-Range` | Absent | `bytes 322018193-322018200/322018201` |
| `Content-Encoding` | Absent | Absent |
| Parquet body bytes read | 0 | 0 |

These headers show advertised range support and a range-specific HEAD response for this particular shard. The eight-byte length is what the response headers described; **eight footer bytes were not actually downloaded**. HEAD does not deliver the Parquet footer. Thus, no tested method here has yet decoded remote Parquet metadata or sample records without downloading a whole shard. Other shards, converted paths, and future requests remain untested.

## Proof-of-concept decision and 50 MB accounting

Treat the requested budget as **50,000,000 bytes**, not 50 MiB. The four-column proof of concept was **skipped**, not attempted and failed, because a hard cumulative transfer cap could not be verified with the available setup. Dependency checks in `.venv` found `duckdb`, `pyarrow`, `fsspec`, and `requests` absent. No installation was attempted or requested during this documentation step.

Measured application response-body bytes for the successful API listing were 18,495; the two HEAD checks read no Parquet body. Headers, redirects, TLS/TCP overhead, and documentation-browser traffic were not metered as network traffic. These measurements must not be presented as total on-wire usage or as proof that a 50 MB network quota was enforced.

An ordinary reader plus a row limit would not meet the requirement. Counting returned rows or dataframe memory misses compressed column chunks, footer reads, speculative prefetch, retries, and network buffers. Closing a response after a capped application read does not prove the network stack received only that many bytes.

## Recommended practical approach for later collection

**Proposal, not implemented:** use PyArrow with an instrumented, budgeted HTTP range-backed file source for a first controlled test; consider DuckDB for subsequent scans once transfer behavior is understood. PyArrow's explicit row-group interface gives a clear unit for the first test, but it is not inherently a hard-budget transport.

1. Obtain approval for only the chosen reader and required transport dependencies. Pin their versions. Do not install both readers unless comparison is needed.
2. Use a manifest of explicit original shard paths at a commit SHA, including split and expected sizes. Avoid broad globs and mutable `main` URLs. If converted files are used instead, resolve and record the converted branch's own immutable revision; do not assume it equals the source SHA.
3. Define and validate the budget mechanism before any data GET. Count all attempts, footer requests, redirects, and retries. For an absolute network-byte limit, use a validated transport/proxy or network enforcement mechanism; an application-body counter alone is insufficient. Leave conservative headroom and fail closed when remaining budget is insufficient.
4. Test one small range first, requiring status 206, the exact expected `Content-Range`, and an acceptable length/encoding. Reject a 200 whole-file fallback. Fetch only the bounded footer needed to plan the first selected row-group read. Log byte intervals without dumping footer statistics or sample values.
5. Disable uncontrolled read-ahead, parallel fetching, and automatic retries for that first test. Select only the four approved columns. Compare requests against footer column-chunk offsets to establish whether HTML data ranges were avoided. Abort if the footer or required column chunks exceed the budget.
6. Only after a successful bounded test, separately approve a larger metadata extraction. Record source revisions, file identifiers, row groups, library versions, row counts, failures, and transfer measurements. Keep raw URLs and identifiers in ignored data storage, with sanitized aggregate reports in Git.

Full metadata collection will still require reading selected column chunks across all relevant shards; its cost is unknown until measured. A small successful read would not establish full-dataset reliability, complete date boundaries, missing-value rates, or absence of duplicates. Do not finalize temporal splitting until those checks have been completed on the intended collection scope.

## Final status

**Verified:** official listing access and range-specific HTTP headers for one pinned shard. **Not verified:** remote footer decoding, four-column data extraction, zero HTML-chunk transfer, or a strict 50 MB transfer limiter. No records were read in this step. Only this document was created; no other project files were modified, no packages installed, no models trained, no DVC initialized, and no commit or push performed.

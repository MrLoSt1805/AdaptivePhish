# AdaptivePhish: Dataset Investigation

Phase 2, Step 2.1. Investigated on **2026-10-24**.

## Scope and evidence

Official dataset: [phreshphish/phreshphish](https://huggingface.co/datasets/phreshphish/phreshphish).

Inspected the dataset card, repository metadata, schema, server statistics, the original research paper, and three preview rows from each split. No Parquet shards were downloaded or saved. No packages were installed. The initial PowerShell network requests were blocked by the sandbox; approved read-only requests succeeded, so internet access was available.

Repository revision reported by the [repository API](https://huggingface.co/api/datasets/phreshphish/phreshphish): `eca603e0ecd4f698f8d4d226c088659e72681239`. The card lists release v1.0.1, dated 2026-02-07, adding samples collected during March–December 2025. The API's builder version `0.0.0` is separate from that release label. Live metadata endpoints can change; pin the repository revision in future ingestion work.

## Configurations, splits, formats, and size

The [splits API](https://datasets-server.huggingface.co/splits?dataset=phreshphish%2Fphreshphish) reports one configuration, `default`, and two splits, `train` and `test`. No validation split is listed. A configuration is a named dataset variant; a split is a partition of its rows.

The [info API](https://datasets-server.huggingface.co/info?dataset=phreshphish%2Fphreshphish) and [file metadata API](https://huggingface.co/api/datasets/phreshphish/phreshphish/tree/main/data?limit=1000) report:

| Split | Rows | Parquet shards | Stored shard bytes | Reported data bytes |
|---|---:|---:|---:|---:|
| train | 498,255 | 56 | 27,400,613,420 | 144,732,047,535 |
| test | 168,060 | 21 | 9,175,760,158 | 48,413,394,715 |
| Total | 666,315 | 77 | 36,576,373,578 | 193,145,442,250 |

Files are `data/train-000.parquet` through `data/train-055.parquet` and `data/test-000.parquet` through `data/test-020.parquet`. Parquet is a column-oriented table format. Stored files total approximately **36.6 GB**; the reported expanded data size is approximately **193.1 GB** (decimal units). These are different measurements, not conflicting estimates. Actual disk and memory needs depend on how data is loaded and cached.

The [dataset card](https://huggingface.co/datasets/phreshphish/phreshphish#benchmarks) also describes 975 benchmark subsets and a full benchmark of 118,738 rows. These use text files of sample identifiers under `benchmark/`; they are subsets of test, not additional independent training data or configurations.

## Actual columns

All eight fields below are verified in the [schema metadata](https://datasets-server.huggingface.co/info?dataset=phreshphish%2Fphreshphish). Meanings follow the [official card](https://huggingface.co/datasets/phreshphish/phreshphish#getting-started).

| Column | API type | Meaning and relevance |
|---|---|---|
| `sha256` | `large_string` | Sample identifier; useful for tracking and overlap checks. Its exact hashing input is not established here. |
| `url` | `large_string` | Page URL; intended input for URL feature engineering. |
| `label` | `large_string` | Class name: `phish` or `benign`. |
| `target` | `large_string` | Impersonated brand; annotation, not the binary class. |
| `date` | `date32` | Collection date, with day-level precision. |
| `lang` | `large_string` | Detected page language. |
| `lang_score` | `float32` | Language confidence score. |
| `html` | `large_string` | Collected page HTML. |

There is no separate time-of-day timestamp, first-seen timestamp, or label-availability timestamp in this schema. The presence of a column does not prove that every row has a value.

## Labels and class balance

`phish` means phishing; `benign` means legitimate/non-phishing in the dataset. They are strings, not stored numeric 0/1 labels. A future AdaptivePhish convention could map `phish` to 1 and `benign` to 0, but that mapping is a recommendation and has not been implemented.

Publisher-reported counts from the [card](https://huggingface.co/datasets/phreshphish/phreshphish):

| Split | Benign | Phish | Phishing share, calculated from these counts |
|---|---:|---:|---:|
| train | 276,729 | 221,526 | About 44.46% |
| test | 91,260 | 76,800 | About 45.70% |

These counts were not independently recomputed from all rows. The classes are fairly close in size, but the full splits have far more phishing than the publisher's low-rate benchmarks (0.05%–5%). Accuracy alone would be insufficient for those benchmarks.

The [original paper, sections 3.2–3.3](https://arxiv.org/html/2507.10854v1) describes phishing feeds, benign browsing/search sources, and automated plus human-assisted cleaning. This is historical methodology, not proof that every current row was individually reviewed or correctly labeled.

## Can dates support chronological evaluation?

**Yes, potentially at day-level resolution; complete current split separation remains unverified.** Chronological evaluation trains on earlier observations and tests on later ones.

Bounded API previews returned these actual dates:

| Preview | Row indices | Dates, in returned order |
|---|---|---|
| [train preview](https://datasets-server.huggingface.co/rows?dataset=phreshphish%2Fphreshphish&config=default&split=train&offset=0&length=3) | 0, 1, 2 | 2025-08-03; 2025-09-02; 2024-09-13 |
| [test preview](https://datasets-server.huggingface.co/rows?dataset=phreshphish%2Fphreshphish&config=default&split=test&offset=0&length=3) | 0, 1, 2 | 2025-10-07; 2025-10-07; 2025-10-28 |

All six rows had non-null URL and HTML fields and were labeled benign. This tiny, non-random preview cannot estimate population properties. Train preview dates are not sorted: row order must not be treated as time order.

The [original paper, section 4.1](https://arxiv.org/html/2507.10854v1) describes a strictly later test set and similarity filtering to reduce leakage. Its July 2025 publication predates the updated release; its original counts and date coverage should not be substituted for current metadata.

Before claiming temporal separation, measure complete date ranges and missing-date counts for both splits, then check whether `max(train.date) < min(test.date)`. Keep same-day records on one side of any cutoff. Collection dates do not establish when a URL became malicious or when a reliable label became available. Missing dates must not be silently invented. The current exact cutoff, complete date coverage, and per-month class coverage are unknown.

## Small inspection without a full download

**Confirmed:** the browser viewer and bounded `/rows` requests work. Only three rows per split were requested in this investigation; no dataset files were persisted. A small row count can still include large HTML strings, so it is not a strict byte limit.

The card also documents `streaming=True` for later use with the Hugging Face Datasets library. This was not executed and the library was not installed. Streaming avoids eagerly fetching the whole dataset but can still transfer data blocks; do not assume an ordinary non-streaming row slice avoids full downloads.

## Data-quality risks and limits

The official [train statistics](https://datasets-server.huggingface.co/statistics?dataset=phreshphish%2Fphreshphish&config=default&split=train) cover 20,413 examples, and [test statistics](https://datasets-server.huggingface.co/statistics?dataset=phreshphish%2Fphreshphish&config=default&split=test) cover 18,553. Both explicitly report `partial: true`. Neither supplies date statistics. Treat these as partial observations, not whole-dataset audits.

| Risk | Evidence or uncertainty | Required later check |
|---|---|---|
| Duplicate URLs and related pages | The original paper describes URL deduplication and similarity filtering; current duplicate rates are unknown. Different URLs can still share content or campaigns. | Check identifiers, exact URLs, carefully normalized URLs, and domain overlap within and across splits; consider HTML similarity separately. |
| Missing dates | Six preview dates exist, but no full date-null count was obtained. | Count nulls and invalid dates across all selected records; document exclusions. |
| Missing fields | Partial statistics report no missing URL, label, or HTML values, but missing language values occur: 460 train and 675 test. | Repeat completeness checks on the eventual data scope. Do not generalize zero missing values to all rows. |
| Annotation leakage | `target` describes the impersonated brand and is often null; using annotation availability could reveal the answer indirectly. | Exclude `target`, `label`, and identifiers from model inputs; keep `date` for splitting/monitoring initially. |
| Temporal and preprocessing leakage | Future observations or preprocessing fitted on test data can inflate scores. | Preserve chronological boundaries; fit preprocessing on training data only; reserve final test data from retraining decisions. |
| Class and source bias | Full splits differ markedly from low-rate benchmarks; source and language mixes may vary over time. | Count classes and languages by month; report precision, recall, and false-positive behavior later. |
| Very large HTML and URLs | Partial train statistics include HTML length 76,259,682 and URL length 25,515; these are observed character-length maxima in that partial view. | Prefer URL/date/label metadata for the next inspection; define resource limits before HTML processing. |
| URL formatting | The viewer includes a URL without a scheme. | Inspect parsing edge cases; preserve original strings and avoid blindly lowercasing case-sensitive paths. |
| Imperfect labels and collection artifacts | Historical cleaning reduces errors but cannot guarantee perfect labels. | Inspect suspicious examples as inert text; do not use scrape failures as reliable class signals. |
| Benchmark reuse | Benchmark subsets come from test and share records. | Do not count them as independent new data or mix them into training. |

## Recommended next inspection step

Request approval for a **bounded metadata audit**, initially up to 100 rows from each split at documented offsets. Inspect `sha256`, `url`, `label`, and `date`; avoid rendering HTML or visiting dataset URLs. Record the revision, selected offsets, sample counts, missing values, label counts, URL formatting issues, observed date ranges, and duplicate/overlap checks. Spread offsets across each split, while clearly stating that this is still not a representative or exhaustive audit.

Only after that should a separately approved metadata-only full-split scan be planned to verify chronological boundaries and month-level coverage. A small sample can reveal problems but cannot prove their absence. No cutoff date, DVC configuration, training, or retraining logic should be finalized from these previews.

The [official card](https://huggingface.co/datasets/phreshphish/phreshphish#license--terms-of-use) lists CC BY 4.0 and anti-phishing research use. Preserve attribution in later research outputs.

**Status:** investigation only. No dataset ingestion, package installation, model training, DVC initialization, commit, or push was performed.

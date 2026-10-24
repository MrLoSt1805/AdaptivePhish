# AdaptivePhish: Bounded Dataset Sample Audit

Phase 2, Step 2.2. Audit date: **2026-10-24**.

## Outcome and scope

Read [the preceding investigation](dataset_investigation.md) before running this audit. Attempted to inspect 100 records per split through the official Hugging Face Dataset Viewer API. **Only 20 train records and 10 test records were successfully returned.** Seventeen of the twenty row requests failed with HTTP 500 (Internal Server Error). Findings below describe only the 30 returned records; failed requests are not treated as empty or clean records.

Inspected only `sha256`, `url`, `label`, and `date`. Responses were processed in memory and immediately reduced to these fields. The documented `/rows` interface returns whole rows, so other fields may have been transferred in the JSON response, but HTML was not inspected, printed, rendered, or saved. No raw sample URLs or identifiers are included in this document. No dataset URLs were visited, no Parquet files downloaded, and no packages installed.

## Sources and revision

- [Official dataset](https://huggingface.co/datasets/phreshphish/phreshphish).
- [Repository revision API](https://huggingface.co/api/datasets/phreshphish/phreshphish).
- [Dataset info API](https://datasets-server.huggingface.co/info?dataset=phreshphish%2Fphreshphish).
- [Official row-slice API documentation](https://huggingface.co/docs/dataset-viewer/rows).

Repository revision before and after the requests: `eca603e0ecd4f698f8d4d226c088659e72681239`. Every successful row response also supplied this same value in its `X-Revision` header. Requests used the live API rather than a revision-pinned endpoint; matching headers establish the revision observed for the successful responses.

Configuration: `default`. Metadata reported 498,255 train rows and 168,060 test rows. These totals were used only to choose offsets, not to infer sample findings.

## Sampling method and exact request log

For each split containing N rows, selected ten offsets using `floor(i * (N - 10) / 9)` for i = 0 through 9. Each request asked for ten consecutive rows. This spreads planned windows from the beginning to the end of each split. It is deterministic sampling, not random sampling or date-stratified sampling.

Request template (substitute SPLIT and OFFSET):

```text
https://datasets-server.huggingface.co/rows?dataset=phreshphish%2Fphreshphish&config=default&split=SPLIT&offset=OFFSET&length=10
```

Offsets and row indices are zero-based. For every successful request, all ten indices from offset through offset + 9 were returned. No selected field was marked truncated by the API.

| Request | Train offset | Train result | Test offset | Test result |
|---|---:|---|---:|---|
| 1 | 0 | 10 rows: 0–9 | 0 | 10 rows: 0–9 |
| 2 | 55,360 | HTTP 500 | 18,672 | HTTP 500 |
| 3 | 110,721 | HTTP 500 | 37,344 | HTTP 500 |
| 4 | 166,081 | HTTP 500 | 56,016 | HTTP 500 |
| 5 | 221,442 | HTTP 500 | 74,688 | HTTP 500 |
| 6 | 276,802 | HTTP 500 | 93,361 | HTTP 500 |
| 7 | 332,163 | HTTP 500 | 112,033 | HTTP 500 |
| 8 | 387,523 | HTTP 500 | 130,705 | HTTP 500 |
| 9 | 442,884 | HTTP 500 | 149,377 | HTTP 500 |
| 10 | 498,245 | 10 rows: 498245–498254 | 168,050 | HTTP 500 |
| **Total inspected** | | **20** | | **10** |

An initial sandbox attempt failed on the repository metadata request with Windows socket error 10013, before any row requests. An approved network-enabled run obtained metadata and attempted all twenty planned row requests once. HTTP 500 responses came from the service; their underlying cause was not established. No alternate offsets were substituted, and no failed request contributed records. Consequently, actual test coverage is only its first ten rows, despite the spread-out plan.

## Labels and date formats

| Finding | Train sample | Test sample |
|---|---:|---:|
| Records inspected | 20 | 10 |
| `benign` labels | 12 | 6 |
| `phish` labels | 8 | 4 |
| Other or missing labels | 0 | 0 |
| Valid `YYYY-MM-DD` dates | 20 | 10 |
| Invalid or missing dates | 0 | 0 |
| Earliest observed date | 2024-07-10 | 2025-09-18 |
| Latest observed date | 2025-09-02 | 2025-11-05 |

Labels were the expected strings: `benign` for legitimate/non-phishing and `phish` for phishing. No numeric label conversion was performed. Dates matched the exact ten-character `YYYY-MM-DD` format and passed Python's standard-library calendar-date parser. They have day-level precision, not time-of-day information.

The latest sampled train date precedes the earliest sampled test date. This establishes separation only among these inspected rows, not the complete dataset. Neither the full temporal cutoff nor complete date boundaries can be inferred.

## Missing values

Defined missing as an absent field, JSON null, an empty string, or a string containing only whitespace. A literal nonempty placeholder such as `unknown` would not automatically count as missing; the date and label checks separately tested expected values.

| Field | Missing in 20 train rows | Missing in 10 test rows |
|---|---:|---:|
| `sha256` | 0 | 0 |
| `url` | 0 | 0 |
| `label` | 0 | 0 |
| `date` | 0 | 0 |

All 30 identifiers also matched a 64-character hexadecimal format. This format check does not verify how the identifier was generated or prove that its content hash is correct.

## Exact duplicates

Compared original strings with case-sensitive equality. No URL normalization, case conversion, whitespace stripping, or hash recomputation was applied for duplicate comparisons. Missing values were excluded. Within-split counts below count excess occurrences beyond the first appearance of a value; cross-split counts count distinct values shared by both samples.

| Check | Exact URL matches | Exact identifier matches |
|---|---:|---:|
| Duplicate excess within train sample | 0 | 0 |
| Duplicate excess within test sample | 0 | 0 |
| Distinct values shared across train and test samples | 0 | 0 |

These results do not exclude duplicates outside the sample, differently written URLs for the same page, shared domains, near-duplicate pages, or recurring campaigns. HTML similarity was not examined.

## URL formatting checks

URLs were inspected only as strings using regular expressions and the standard-library URL parser. Parsing performs no network lookup. No raw URLs are reproduced here.

| Observation | Train sample | Test sample |
|---|---:|---:|
| HTTPS scheme | 17 | 9 |
| HTTP scheme | 2 | 1 |
| No explicit scheme / scheme separator | 1 | 0 |
| Non-HTTP(S) explicit scheme | 0 | 0 |
| Leading or trailing whitespace | 0 | 0 |
| Any whitespace | 0 | 0 |
| ASCII control characters | 0 | 0 |
| Non-ASCII characters | 0 | 0 |
| Percent signs without two following hexadecimal digits | 0 | 0 |
| HTTP(S) URL without a parsed hostname | 0 | 0 |
| Parser or port-validation error | 0 | 0 |
| Parsed user-information component | 0 | 0 |

The scheme-less URL needs an explicit handling rule in future feature engineering; a parser may treat it as a path rather than a hostname. Preserve the original value and document any parsing-only assumption. HTTP alone does not mean phishing, and HTTPS does not guarantee legitimacy. These checks are limited formatting checks, not proof that URLs are valid, reachable, or safe.

## What this sample cannot establish

- **Complete date boundaries:** an uninspected row could be earlier or later than every sampled row. The observed ranges must not become dataset-wide split dates.
- **Duplicate rates or absence of leakage:** duplicate pairs can easily be missed when only one or neither member is sampled. Zero observed duplicates is not a clean bill of health for all rows.
- **Complete class balance:** both returned samples happened to contain 40% phishing, but small deterministic windows are not representative random samples. API failures further biased coverage toward accessible windows. Do not replace publisher-reported totals with these percentages.
- **Complete missing-value rates:** no missing values among 30 records says nothing definitive about uninspected records.
- **Label correctness:** checking the spelling of a label does not independently verify whether the page was actually phishing.

## Recommended next step

In a separately approved step, retry the failed documented offsets with a bounded retry count and small request sizes. Record successes and failures without presenting a partly accessible sample as complete. If row access remains unreliable, investigate an official route that supports selecting only the four needed columns before planning any larger transfer.

After access is reliable, plan a revision-pinned, metadata-only full-split audit to count missing dates, date ranges, labels by month, and duplicate/overlap values. Verify transfer behavior and storage limits before that scan; it was not performed here. Only a sufficiently comprehensive audit can support firm chronological split decisions.

**Stopped after Step 2.2.** This document is the only project file created or modified for this step. No full dataset download, package installation, model training, DVC initialization, commit, or push was performed.

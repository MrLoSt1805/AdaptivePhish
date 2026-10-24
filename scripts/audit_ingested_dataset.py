"""Read-only quality audit for the single approved local PhreshPhish shard."""

import collections
import json
import sys

import pyarrow.parquet as pq

from adaptivephish.data import output_writer
from adaptivephish.data.source_config import SourceConfigError, load_source_config
from adaptivephish.data.validation import ValidationError, validate_batches


DATASET_PATH = (
    output_writer.WORKSPACE_ROOT
    / 'data' / 'raw' / 'phreshphish'
    / 'train-001-url-metadata.parquet'
)
BATCH_SIZE = 1_024


def empty_result(category='audit_failed'):
    return {
        'success': False,
        'failure_category': category,
        'source_identity': None,
        'output_verification_status': 'not_verified',
        'total_rows': None,
        'label_counts': {},
        'date_range': None,
        'missing_fields': {},
        'invalid_values': {},
        'missing_url_scheme_count': None,
        'duplicate_counts': {},
        'monthly_distribution': {},
        'scope': 'approved_single_shard',
    }


def _expected_source(config):
    return {
        'dataset': config.dataset,
        'revision': config.revision,
        'split': config.split,
        'shard': config.shard,
    }


def _manifest_matches_contract(document, config):
    return (
        isinstance(document, dict)
        and document.get('completion_status') == 'complete'
        and document.get('source') == _expected_source(config)
        and document.get('selected_columns') == list(config.selected_columns)
        and document.get('row_count') == config.expected_rows
    )


def _read_batches():
    parquet = pq.ParquetFile(DATASET_PATH)
    batches = parquet.iter_batches(
        batch_size=BATCH_SIZE,
        columns=['sha256', 'url', 'label', 'date'],
        use_threads=False,
    )
    try:
        return list(batches)
    finally:
        batches.close()
        parquet.close()


def _monthly_distribution(batches):
    counts = collections.defaultdict(lambda: {'benign': 0, 'phish': 0})
    for batch in batches:
        labels = batch.column('label')
        dates = batch.column('date')
        for index in range(batch.num_rows):
            # Validation has already established non-null labels and dates.
            month = dates[index].as_py().strftime('%Y-%m')
            counts[month][labels[index].as_py()] += 1
    return {month: counts[month] for month in sorted(counts)}


def _metrics_match_manifest(summary, document):
    return (
        document.get('row_count') == summary.row_count
        and document.get('label_counts') == summary.label_counts
        and document.get('date_range') == {
            'earliest': summary.earliest_date,
            'latest': summary.latest_date,
        }
        and document.get('duplicate_counts') == {
            'sha256': summary.duplicate_sha256_count,
            'url': summary.duplicate_url_count,
            'row': summary.duplicate_row_count,
            'conflicting_sha256': summary.conflicting_sha256_count,
        }
    )


def audit_approved_dataset():
    """Verify and independently audit the fixed local dataset."""
    result = empty_result()
    try:
        config = load_source_config()
        document = output_writer.verify_completed_output(DATASET_PATH)
        if not _manifest_matches_contract(document, config):
            result['failure_category'] = 'source_manifest_mismatch'
            return result
        result['output_verification_status'] = 'verified'
        result['source_identity'] = _expected_source(config)

        batches = _read_batches()
        summary = validate_batches(batches)
        if not _metrics_match_manifest(summary, document):
            result['failure_category'] = 'quality_manifest_mismatch'
            return result

        result.update(
            success=True,
            failure_category=None,
            total_rows=summary.row_count,
            label_counts=dict(summary.label_counts),
            date_range={
                'earliest': summary.earliest_date,
                'latest': summary.latest_date,
            },
            missing_fields=dict(summary.missing_fields),
            invalid_values=dict(summary.invalid_values),
            missing_url_scheme_count=summary.url_missing_scheme_count,
            duplicate_counts={
                'sha256': summary.duplicate_sha256_count,
                'url': summary.duplicate_url_count,
                'row': summary.duplicate_row_count,
                'conflicting_sha256': summary.conflicting_sha256_count,
            },
            monthly_distribution=_monthly_distribution(batches),
        )
    except SourceConfigError:
        result['failure_category'] = 'source_configuration_failed'
    except output_writer.OutputWriterError:
        result['failure_category'] = 'output_verification_failed'
    except ValidationError:
        result['failure_category'] = 'dataset_validation_failed'
    except Exception:
        result['failure_category'] = 'audit_failed'
    return result


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    result = (
        audit_approved_dataset()
        if not arguments
        else empty_result('unexpected_arguments')
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else (2 if arguments else 1)


if __name__ == '__main__':
    sys.exit(main())

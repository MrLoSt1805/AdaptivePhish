"""Explicit command-line gate for the single approved PhreshPhish shard."""

import json
from pathlib import Path
import sys

from adaptivephish.data import ingestion, output_writer
from adaptivephish.data.source_config import SourceConfigError, load_source_config


OUTPUT_PATH = (
    output_writer.WORKSPACE_ROOT
    / 'data' / 'raw' / 'phreshphish'
    / 'train-001-url-metadata.parquet'
)
SAFE_FAILURE_CATEGORIES = frozenset({
    'approved_row_count_mismatch', 'batch_processing_error',
    'budget_precheck', 'completion_verification_failed',
    'conflicting_identifier', 'incomplete_output', 'ingestion_failed',
    'invalid_arrow_types', 'invalid_column_schema',
    'invalid_parquet_metadata', 'invalid_required_value',
    'missing_required_value', 'output_not_git_ignored',
    'output_verification_failed', 'output_write_failed',
    'parquet_read_error', 'source_configuration_failed',
    'source_contract_mismatch', 'transport_error',
    'transport_unmetered', 'unsafe_schema_conversion',
    'unapproved_output_path', 'unapproved_range',
})


def empty_result(category='live_flag_required'):
    return {
        'success': False,
        'failure_category': category,
        'output_row_count': None,
        'label_counts': {},
        'date_range': None,
        'duplicate_counts': {},
        'source_identity': None,
        'output_verification_status': 'not_attempted',
    }


def _success_result(document, verification_status='verified'):
    """Select only approved aggregate fields from a verified manifest."""
    try:
        config = load_source_config()
        expected_source = {
            'dataset': config.dataset,
            'revision': config.revision,
            'split': config.split,
            'shard': config.shard,
        }
        labels = document['label_counts']
        dates = document['date_range']
        duplicates = document['duplicate_counts']
        if (document.get('completion_status') != 'complete'
                or document.get('source') != expected_source
                or document.get('selected_columns') != list(config.selected_columns)
                or type(document.get('row_count')) is not int
                or set(labels) != {'benign', 'phish'}
                or any(type(value) is not int for value in labels.values())
                or set(dates) != {'earliest', 'latest'}
                or any(value is not None and not isinstance(value, str)
                       for value in dates.values())
                or set(duplicates) != {
                    'sha256', 'url', 'row', 'conflicting_sha256'
                }
                or any(type(value) is not int for value in duplicates.values())):
            raise ValueError
    except (KeyError, TypeError, ValueError, SourceConfigError):
        raise ingestion.IngestionError('completion_verification_failed') from None
    return {
        'success': True,
        'failure_category': None,
        'output_row_count': document['row_count'],
        'label_counts': dict(labels),
        'date_range': dict(dates),
        'duplicate_counts': dict(duplicates),
        'source_identity': dict(expected_source),
        'output_verification_status': verification_status,
    }


def _existing_output_result():
    sidecar = output_writer.manifest_path(OUTPUT_PATH)
    if not OUTPUT_PATH.exists() and not sidecar.exists():
        return None
    try:
        document = output_writer.verify_completed_output(OUTPUT_PATH)
        _success_result(document, 'verified_existing')
        result = empty_result('existing_complete_output')
        result['output_verification_status'] = 'verified_existing'
        return result
    except Exception:
        result = empty_result('existing_incomplete_output')
        result['output_verification_status'] = 'incomplete_existing'
        return result


def run_live_ingestion():
    existing = _existing_output_result()
    if existing is not None:
        return existing
    try:
        document = ingestion.ingest_approved_shard(OUTPUT_PATH)
        verified = output_writer.verify_completed_output(OUTPUT_PATH)
        if verified != document:
            raise ingestion.IngestionError('completion_verification_failed')
        return _success_result(verified)
    except ingestion.IngestionError as error:
        category = (
            error.category
            if error.category in SAFE_FAILURE_CATEGORIES
            else 'ingestion_failed'
        )
        return empty_result(category)
    except Exception:
        return empty_result('ingestion_failed')


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    result = run_live_ingestion() if arguments == ['--live'] else empty_result()
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else (2 if arguments != ['--live'] else 1)


if __name__ == '__main__':
    sys.exit(main())

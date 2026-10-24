"""Fail-closed metadata-only preflight for the pinned ingestion shard.

Network access requires exactly ``--live``. The configured application-body
budget is not a hard total-network cap; headers, redirects, TLS, TCP, socket
buffering, and unexposed bytes from transport failures are outside it.
"""

import json
import struct
import sys

import pyarrow as pa

from adaptivephish.data import remote_parquet
from adaptivephish.data.ingestion import IngestionError, _load_approved_contract
from adaptivephish.data.source_config import SourceConfigError


EXPECTED_SOURCE_COLUMNS = (
    'sha256', 'url', 'label', 'target', 'date', 'lang', 'lang_score', 'html'
)
PLANNED_LOGICAL_REQUESTS = 7  # header, trailer, footer, four selected chunks


def empty_result():
    return {
        'success': False,
        'failure_category': 'live_flag_required',
        'source': None,
        'row_count': None,
        'row_group_count': None,
        'selected_columns': [],
        'selected_compressed_bytes': None,
        'metadata_application_body_bytes': None,
        'estimated_application_body_bytes': None,
        'application_body_budget': None,
        'maximum_http_requests': None,
        'planned_logical_requests': PLANNED_LOGICAL_REQUESTS,
        'http_request_count': 0,
    }


def _source_identity(config):
    return {
        'dataset': config.dataset,
        'revision': config.revision,
        'split': config.split,
        'shard': config.shard,
    }


def _selected_schema_is_supported(metadata, selected_columns):
    schema = metadata.schema.to_arrow_schema()
    if tuple(schema.names) != EXPECTED_SOURCE_COLUMNS:
        return False
    expected = {
        'sha256': (pa.string(), pa.large_string()),
        'url': (pa.string(), pa.large_string()),
        'label': (pa.string(), pa.large_string()),
        'date': (pa.date32(),),
    }
    return all(schema.field(name).type in expected[name]
               for name in selected_columns)


def inspect_preflight(reader, config):
    """Inspect only trailer/footer bytes and return sanitized diagnostics."""
    result = empty_result()
    result.update(
        source=_source_identity(config),
        selected_columns=list(config.selected_columns),
        application_body_budget=config.application_body_budget,
        maximum_http_requests=config.maximum_http_requests,
    )
    try:
        if (reader.size != config.expected_file_size
                or reader.budget != config.application_body_budget
                or tuple(config.selected_columns) != remote_parquet.COLUMNS
                or config.maximum_http_requests != 32
                or PLANNED_LOGICAL_REQUESTS > config.maximum_http_requests):
            raise remote_parquet.SafetyError('Source contract mismatch')

        trailer_start = reader.size - 8
        reader.allowed = [(trailer_start, reader.size)]
        trailer = reader.fetch(trailer_start, reader.size)
        if trailer[4:] != b'PAR1':
            raise remote_parquet.SafetyError('Invalid Parquet header')
        footer_length = struct.unpack('<I', trailer[:4])[0]
        if not 0 < footer_length <= min(
                remote_parquet.MAX_FOOTER, reader.size - 12):
            raise remote_parquet.SafetyError('Invalid or oversized footer')

        footer_start = trailer_start - footer_length
        reader.allowed.append((footer_start, trailer_start))
        footer = reader.fetch(footer_start, trailer_start)
        metadata = remote_parquet.pq.read_metadata(
            remote_parquet.pa.BufferReader(b'PAR1' + footer + trailer)
        )
        if (metadata.num_row_groups != config.expected_row_groups
                or metadata.num_rows != config.expected_rows):
            raise remote_parquet.SafetyError('Unexpected source dimensions')
        if not _selected_schema_is_supported(metadata, config.selected_columns):
            raise remote_parquet.SafetyError('Unsupported source schema')

        selected = remote_parquet.plan_first_group(
            reader, metadata, footer_start
        )
        selected_bytes = sum(end - start for start, end in selected.values())
        # The production reader also verifies the four-byte leading magic.
        metadata_bytes = 4 + 8 + footer_length
        estimated_bytes = metadata_bytes + selected_bytes
        if estimated_bytes > config.application_body_budget:
            raise remote_parquet.SafetyError('budget_precheck')

        result.update(
            success=True,
            failure_category=None,
            row_count=metadata.num_rows,
            row_group_count=metadata.num_row_groups,
            selected_compressed_bytes=selected_bytes,
            metadata_application_body_bytes=metadata_bytes,
            estimated_application_body_bytes=estimated_bytes,
        )
    except remote_parquet.SafetyError as error:
        if error.category in {
            'Source contract mismatch', 'Unexpected source dimensions',
            'Unsupported source schema',
        }:
            result['failure_category'] = 'source_metadata_mismatch'
        else:
            result['failure_category'] = remote_parquet._failure_category(
                error.category
            )
    except Exception:
        result['failure_category'] = (
            reader.violation_category or 'metadata_parse_error'
        )
    finally:
        result['http_request_count'] = reader.request_count
        reader.close()
    return result


def run_live_preflight():
    """Construct only the immutable approved source reader."""
    try:
        config = _load_approved_contract()
        reader = remote_parquet.RangeFile(
            url=remote_parquet.URL,
            size=config.expected_file_size,
            budget=config.application_body_budget,
        )
        return inspect_preflight(reader, config)
    except (IngestionError, SourceConfigError):
        result = empty_result()
        result['failure_category'] = 'source_configuration_failed'
        return result
    except Exception:
        result = empty_result()
        result['failure_category'] = 'preflight_initialization_failed'
        return result


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    result = run_live_preflight() if arguments == ['--live'] else empty_result()
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else (2 if arguments != ['--live'] else 1)


if __name__ == '__main__':
    sys.exit(main())

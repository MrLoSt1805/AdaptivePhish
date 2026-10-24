"""Inspect only a pinned Parquet footer when explicitly run with --live.

The 64 KiB limit counts response-body bytes returned to this application. It
is not a hard network-transfer cap: redirects, headers, TLS, socket buffering,
and bytes consumed before an unmetered transport exception are outside it.
No column chunks or records are authorized or fetched.
"""

import importlib.util
import json
import struct
import sys
from pathlib import Path


APPLICATION_BUDGET = 64 * 1024
EXPECTED_COLUMNS = (
    'sha256', 'url', 'label', 'target', 'date', 'lang', 'lang_score', 'html'
)


def empty_result():
    return {
        'success': False,
        'failure_category': 'live_flag_required',
        'http_request_count': 0,
        'application_response_body_bytes': 0,
        'footer_length_bytes': None,
        'row_group_count': None,
        'total_rows': None,
        'column_names': [],
        'selected_column_compressed_sizes': {},
    }


def parse_metadata(module, footer, trailer, footer_start):
    """Parse an in-memory synthetic wrapper; this cannot access the network."""
    wrapper = module.pa.BufferReader(b'PAR1' + footer + trailer)
    metadata = module.pq.read_metadata(wrapper)
    names = metadata.schema.names
    if names != list(EXPECTED_COLUMNS) or metadata.num_row_groups < 1:
        raise module.SafetyError('Invalid Parquet schema')
    if metadata.num_rows < 1:
        raise module.SafetyError('Empty Parquet file')

    selected_sizes = {name: 0 for name in module.COLUMNS}
    for group_index in range(metadata.num_row_groups):
        group = metadata.row_group(group_index)
        if group.num_rows < 0 or group.num_columns != len(EXPECTED_COLUMNS):
            raise module.SafetyError('Invalid Parquet metadata')
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            if column.file_path or column.total_compressed_size <= 0:
                raise module.SafetyError('Invalid Parquet metadata')
            offsets = [column.data_page_offset]
            if column.has_dictionary_page:
                offsets.append(column.dictionary_page_offset)
            if any(offset is None or offset < 4 for offset in offsets):
                raise module.SafetyError('Invalid Parquet metadata')
            start = min(offsets)
            end = start + column.total_compressed_size
            if not 4 <= start < end <= footer_start:
                raise module.SafetyError('Invalid Parquet metadata')
            if column.path_in_schema in selected_sizes:
                selected_sizes[column.path_in_schema] += column.total_compressed_size

    return metadata, names, selected_sizes


def main():
    result = empty_result()
    if sys.argv[1:] != ['--live']:
        print(json.dumps(result, sort_keys=True))
        return 2

    reader = None
    try:
        source = Path(__file__).with_name('test_remote_parquet.py')
        spec = importlib.util.spec_from_file_location('remote_parquet_reader', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        reader = module.RangeFile(
            url=module.URL, size=module.SIZE, budget=APPLICATION_BUDGET
        )

        trailer_start = module.SIZE - 8
        reader.allowed = [(trailer_start, module.SIZE)]
        trailer = reader.fetch(trailer_start, module.SIZE)
        if trailer[4:] != b'PAR1':
            raise module.SafetyError('Invalid Parquet header')

        footer_length = struct.unpack('<I', trailer[:4])[0]
        if not 0 < footer_length <= min(module.MAX_FOOTER, module.SIZE - 12):
            raise module.SafetyError('Invalid or oversized footer')
        if footer_length > reader.remaining:
            raise module.SafetyError('budget_precheck')

        footer_start = trailer_start - footer_length
        if footer_start < 4 or footer_start >= trailer_start:
            raise module.SafetyError('Invalid or oversized footer')
        reader.allowed.append((footer_start, trailer_start))
        footer = reader.fetch(footer_start, trailer_start)

        metadata, names, sizes = parse_metadata(
            module, footer, trailer, footer_start
        )
        result.update(
            success=True,
            failure_category=None,
            footer_length_bytes=footer_length,
            row_group_count=metadata.num_row_groups,
            total_rows=metadata.num_rows,
            column_names=names,
            selected_column_compressed_sizes=sizes,
        )
    except Exception as exc:
        # Never expose raw exception text, response data, or redirect URLs.
        if reader is not None and 'module' in locals() and isinstance(
                exc, module.SafetyError):
            category = module._failure_category(exc.category)
            if exc.category in {'Invalid Parquet schema', 'Invalid Parquet metadata'}:
                category = 'invalid_parquet_metadata'
            result['failure_category'] = category
        else:
            result['failure_category'] = 'metadata_parse_error'
    finally:
        if reader is not None:
            result['http_request_count'] = reader.request_count
            result['application_response_body_bytes'] = reader.body_bytes
            reader.close()

    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    sys.exit(main())

"""Read four columns from one pinned Parquet row group with --live.

The 2 MiB limit counts response-body bytes returned to this application. It
is not a hard network-transfer cap: redirects, headers, TLS, socket buffering,
and bytes hidden by an unmetered transport exception are outside it.

A five-row output batch may require fetching complete compressed chunks for
the four selected columns. Exact byte-range authorization prevents reads from
HTML and every other unselected column.
"""

import collections
import importlib.util
import json
import struct
import sys
from pathlib import Path


APPLICATION_BUDGET = 2 * 1024 * 1024
EXPECTED_COLUMNS = (
    'sha256', 'url', 'label', 'target', 'date', 'lang', 'lang_score', 'html'
)
MAX_OUTPUT_ROWS = 5


def empty_result():
    return {
        'success': False,
        'failure_category': 'live_flag_required',
        'http_request_count': 0,
        'application_response_body_bytes': 0,
        'row_count_returned': 0,
        'column_names': [],
        'label_counts': {},
    }


def parse_footer(module, footer, trailer):
    """Parse metadata only from bytes already held in memory."""
    wrapper = module.pa.BufferReader(b'PAR1' + footer + trailer)
    return module.pq.read_metadata(wrapper)


def validate_and_plan(module, metadata, footer_start):
    """Return exact selected chunks after validating every column chunk."""
    if metadata.schema.names != list(EXPECTED_COLUMNS):
        raise module.SafetyError('Invalid Parquet schema')
    if metadata.num_row_groups != 1 or metadata.num_rows < 1:
        raise module.SafetyError('Invalid Parquet metadata')

    selected = {}
    unauthorized = []
    all_chunks = []
    group = metadata.row_group(0)
    if group.num_rows < 1 or group.num_columns != len(EXPECTED_COLUMNS):
        raise module.SafetyError('Invalid Parquet metadata')

    for index in range(group.num_columns):
        column = group.column(index)
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

        interval = (start, end)
        all_chunks.append(interval)
        if column.path_in_schema in module.COLUMNS:
            if column.path_in_schema in selected:
                raise module.SafetyError('Invalid Parquet metadata')
            selected[column.path_in_schema] = interval
        else:
            unauthorized.append(interval)

    if set(selected) != set(module.COLUMNS):
        raise module.SafetyError('Invalid Parquet schema')
    ordered = sorted(all_chunks)
    if any(left[1] > right[0] for left, right in zip(ordered, ordered[1:])):
        raise module.SafetyError('Invalid Parquet metadata')
    if any(start < blocked_end and blocked_start < end
           for start, end in selected.values()
           for blocked_start, blocked_end in unauthorized):
        raise module.SafetyError('unauthorized_overlap')
    return selected


def read_sample(module, reader, metadata, selected):
    """Fetch approved chunks exactly, then return one batch of at most 5 rows."""
    for name in module.COLUMNS:
        reader.fetch(*selected[name])

    parquet = module.pq.ParquetFile(
        reader,
        metadata=metadata,
        pre_buffer=False,
        buffer_size=0,
        arrow_extensions_enabled=False,
    )
    batches = parquet.iter_batches(
        batch_size=MAX_OUTPUT_ROWS,
        row_groups=[0],
        columns=list(module.COLUMNS),
        use_threads=False,
    )
    try:
        batch = next(batches)
        if not 0 < batch.num_rows <= MAX_OUTPUT_ROWS:
            raise module.SafetyError('Unexpected output shape')
        if batch.schema.names != list(module.COLUMNS):
            raise module.SafetyError('Unexpected output shape')
        labels = batch.column('label').to_pylist()
        if any(label not in ('benign', 'phish') for label in labels):
            raise module.SafetyError('Unexpected label')
        return batch.num_rows, batch.schema.names, dict(collections.Counter(labels))
    finally:
        batches.close()
        parquet.close()


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
        footer_start = trailer_start - footer_length
        if footer_start < 4 or footer_start >= trailer_start:
            raise module.SafetyError('Invalid or oversized footer')
        if footer_length > reader.remaining:
            raise module.SafetyError('budget_precheck')

        reader.allowed.append((footer_start, trailer_start))
        footer = reader.fetch(footer_start, trailer_start)
        metadata = parse_footer(module, footer, trailer)
        selected = validate_and_plan(module, metadata, footer_start)

        expected_column_bytes = sum(end - start for start, end in selected.values())
        if expected_column_bytes > reader.remaining:
            raise module.SafetyError('budget_precheck')

        # This is the complete authorization set: trailer, footer, and exactly
        # four selected chunks. Any PyArrow read outside it fails in RangeFile.
        reader.allowed.extend(selected.values())
        row_count, names, labels = read_sample(
            module, reader, metadata, selected
        )
        result.update(
            success=True,
            failure_category=None,
            row_count_returned=row_count,
            column_names=names,
            label_counts=labels,
        )
    except Exception as exc:
        # Do not expose exception text, data values, URLs, redirects, or tokens.
        if reader is not None and 'module' in locals() and isinstance(
                exc, module.SafetyError):
            category = module._failure_category(exc.category)
            if exc.category in {
                'Invalid Parquet schema', 'Invalid Parquet metadata',
                'unauthorized_overlap',
            }:
                category = 'invalid_parquet_metadata'
            result['failure_category'] = category
        elif reader is not None and reader.violation_category:
            result['failure_category'] = reader.violation_category
        else:
            result['failure_category'] = 'parquet_read_error'
    finally:
        if reader is not None:
            result['http_request_count'] = reader.request_count
            result['application_response_body_bytes'] = reader.body_bytes
            reader.close()

    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    sys.exit(main())

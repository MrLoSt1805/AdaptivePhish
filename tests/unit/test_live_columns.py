"""Offline end-to-end tests for the controlled live column workflow.

Five returned rows do not imply five rows' worth of compressed I/O. The
workflow deliberately fetches complete selected-column chunks first.
"""

import contextlib
import datetime
import importlib.util
import io
import json
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


reader_module = load_module(
    'live_columns_range_reader', ROOT / 'scripts' / 'test_remote_parquet.py'
)
columns_module = load_module(
    'live_columns_check', ROOT / 'scripts' / 'check_live_columns.py'
)


def make_parquet():
    rows = 12
    table = pa.table({
        'sha256': [f'{index:064x}' for index in range(rows)],
        'url': [f'https://private-{index}.invalid/path' for index in range(rows)],
        'label': ['benign', 'phish'] * (rows // 2),
        'target': [None, 'synthetic'] * (rows // 2),
        'date': [datetime.date(2025, 2, 1) + datetime.timedelta(days=index)
                 for index in range(rows)],
        'lang': ['en'] * rows,
        'lang_score': [0.75] * rows,
        'html': [('PRIVATE_HTML_' + str(index) + '_') * 25_000
                 for index in range(rows)],
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=rows, compression='NONE')
    return sink.getvalue().to_pybytes()


def interval(column):
    offsets = [column.data_page_offset]
    if column.has_dictionary_page:
        offsets.append(column.dictionary_page_offset)
    start = min(offsets)
    return start, start + column.total_compressed_size


def overlaps(left, right):
    return left[0] < right[1] and right[0] < left[1]


class Response:
    status = 206

    def __init__(self, data, start, end, total):
        self.body = io.BytesIO(data)
        self.headers = [
            ('Content-Range', f'bytes {start}-{end - 1}/{total}'),
            ('Content-Length', str(end - start)),
        ]

    def getheaders(self):
        return self.headers

    def read(self, size):
        return self.body.read(size)


class LocalRangeTransport:
    def __init__(self, data):
        self.data = data
        self.requests = []

    @contextlib.contextmanager
    def __call__(self, url, headers):
        start, inclusive_end = map(int, headers['Range'][6:].split('-'))
        end = inclusive_end + 1
        self.requests.append((start, end))
        yield Response(self.data[start:end], start, end, len(self.data))


class LiveColumnsWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch(
            'socket.socket.connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)

        self.data = make_parquet()
        self.transport = LocalRangeTransport(self.data)
        self.reader = reader_module.RangeFile(
            size=len(self.data),
            budget=columns_module.APPLICATION_BUDGET,
            transport=self.transport,
        )
        self.addCleanup(self.reader.close)

    def prepare_metadata(self):
        trailer_start = len(self.data) - 8
        self.reader.allowed = [(trailer_start, len(self.data))]
        trailer = self.reader.fetch(trailer_start, len(self.data))
        self.assertEqual(trailer[4:], b'PAR1')
        footer_length = struct.unpack('<I', trailer[:4])[0]
        footer_start = trailer_start - footer_length
        self.reader.allowed.append((footer_start, trailer_start))
        footer = self.reader.fetch(footer_start, trailer_start)
        metadata = columns_module.parse_footer(reader_module, footer, trailer)
        return metadata, footer_start

    def test_end_to_end_plan_prefetch_cache_and_sample(self):
        metadata, footer_start = self.prepare_metadata()
        selected = columns_module.validate_and_plan(
            reader_module, metadata, footer_start
        )
        group = metadata.row_group(0)
        all_ranges = {
            group.column(index).path_in_schema: interval(group.column(index))
            for index in range(group.num_columns)
        }
        unselected = [all_ranges[name] for name in columns_module.EXPECTED_COLUMNS
                      if name not in reader_module.COLUMNS]

        expected_bytes = sum(end - start for start, end in selected.values())
        self.assertLessEqual(expected_bytes, self.reader.remaining)
        self.reader.allowed.extend(selected.values())
        expected_authorization = {
            (len(self.data) - 8, len(self.data)),
            (footer_start, len(self.data) - 8),
            *selected.values(),
        }
        self.assertEqual(set(self.reader.allowed), expected_authorization)
        self.assertTrue(all(
            not overlaps(allowed, blocked)
            for allowed in selected.values() for blocked in unselected
        ))

        rows, names, labels = columns_module.read_sample(
            reader_module, self.reader, metadata, selected
        )

        self.assertEqual(rows, 5)
        self.assertEqual(names, list(reader_module.COLUMNS))
        self.assertEqual(labels, {'benign': 3, 'phish': 2})
        self.assertEqual(len(self.transport.requests), 6)
        self.assertEqual(
            self.transport.requests[2:],
            [selected[name] for name in reader_module.COLUMNS],
        )
        self.assertTrue(all(
            not overlaps(requested, blocked)
            for requested in self.transport.requests for blocked in unselected
        ))

        # PyArrow reads are served from RangeFile's four prefetched cache
        # entries, so reading the batch creates no duplicate HTTP requests.
        self.assertEqual(len(self.reader.cache), 6)
        diagnostic = json.dumps({
            'row_count_returned': rows,
            'column_names': names,
            'label_counts': labels,
        })
        self.assertNotIn('private-', diagnostic.lower())
        self.assertNotIn('private_html', diagnostic.lower())
        self.assertNotIn('0' * 64, diagnostic)

    def test_budget_rejects_selected_chunk_before_transport(self):
        metadata = pq.read_metadata(pa.BufferReader(self.data))
        footer_length = struct.unpack('<I', self.data[-8:-4])[0]
        footer_start = len(self.data) - 8 - footer_length
        selected = columns_module.validate_and_plan(
            reader_module, metadata, footer_start
        )
        first = selected[reader_module.COLUMNS[0]]
        transport = LocalRangeTransport(self.data)
        reader = reader_module.RangeFile(
            size=len(self.data), budget=(first[1] - first[0]) - 1,
            transport=transport,
        )
        self.addCleanup(reader.close)
        reader.allowed = list(selected.values())

        with self.assertRaises(reader_module.SafetyError) as error:
            columns_module.read_sample(reader_module, reader, metadata, selected)

        self.assertEqual(error.exception.category, 'budget_precheck')
        self.assertEqual(transport.requests, [])
        self.assertEqual(reader.body_bytes, 0)

    def test_malformed_bounds_fail_before_column_requests(self):
        metadata = pq.read_metadata(pa.BufferReader(self.data))
        with self.assertRaises(reader_module.SafetyError) as error:
            columns_module.validate_and_plan(reader_module, metadata, 4)

        self.assertEqual(error.exception.category, 'Invalid Parquet metadata')
        self.assertEqual(self.transport.requests, [])

    def test_overlapping_unselected_metadata_fails_before_column_requests(self):
        metadata = pq.read_metadata(pa.BufferReader(self.data))
        footer_length = struct.unpack('<I', self.data[-8:-4])[0]
        footer_start = len(self.data) - 8 - footer_length
        original_group = metadata.row_group
        group = original_group(0)
        selected_column = group.column(0)
        html_column = group.column(7)

        class OverlappingHtml:
            def __getattr__(self, name):
                if name == 'data_page_offset':
                    return selected_column.data_page_offset
                if name == 'dictionary_page_offset':
                    return selected_column.dictionary_page_offset
                return getattr(html_column, name)

        class Group:
            num_rows = group.num_rows
            num_columns = group.num_columns

            def column(self, index):
                return OverlappingHtml() if index == 7 else group.column(index)

        class Metadata:
            schema = metadata.schema
            num_row_groups = metadata.num_row_groups
            num_rows = metadata.num_rows

            def row_group(self, index):
                return Group()

        with self.assertRaises(reader_module.SafetyError) as error:
            columns_module.validate_and_plan(
                reader_module, Metadata(), footer_start
            )

        self.assertEqual(error.exception.category, 'Invalid Parquet metadata')
        self.assertEqual(self.transport.requests, [])


if __name__ == '__main__':
    unittest.main()

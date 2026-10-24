"""Offline tests for the metadata-only live-ingestion preflight."""

import contextlib
from dataclasses import replace
import datetime
import importlib.util
import io
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from adaptivephish.data import remote_parquet
from adaptivephish.data.source_config import load_source_config


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'ingestion_preflight', ROOT / 'scripts' / 'check_ingestion_preflight.py'
)
preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(preflight)


def make_parquet(rows=6, padding=2_000):
    table = pa.table({
        'sha256': [f'{index:064x}' for index in range(rows)],
        'url': [f'https://sample-{index}.invalid/path' for index in range(rows)],
        'label': ['benign', 'phish'] * (rows // 2),
        'target': [None] * rows,
        'date': [datetime.date(2025, 1, 1) + datetime.timedelta(days=index)
                 for index in range(rows)],
        'lang': ['en'] * rows,
        'lang_score': [0.9] * rows,
        'html': [('PRIVATE_HTML_' + str(index)) * padding for index in range(rows)],
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=rows, compression='NONE')
    return sink.getvalue().to_pybytes()


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


class FakeTransport:
    def __init__(self, data, failure=None):
        self.data = data
        self.failure = failure
        self.requests = []

    @contextlib.contextmanager
    def __call__(self, url, headers):
        if self.failure:
            raise RuntimeError(self.failure)
        start, inclusive_end = map(int, headers['Range'][6:].split('-'))
        end = inclusive_end + 1
        self.requests.append((start, end))
        yield Response(self.data[start:end], start, end, len(self.data))


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)
        self.data = make_parquet()

    def config(self, **changes):
        values = {
            'expected_file_size': len(self.data),
            'expected_rows': 6,
            'expected_row_groups': 1,
        }
        values.update(changes)
        return replace(load_source_config(), **values)

    def reader(self, config=None, transport=None):
        config = config or self.config()
        transport = transport or FakeTransport(self.data)
        reader = remote_parquet.RangeFile(
            size=len(self.data), budget=config.application_body_budget,
            transport=transport,
        )
        return reader, transport

    def test_success_reads_only_trailer_and_footer(self):
        config = self.config()
        reader, transport = self.reader(config)
        result = preflight.inspect_preflight(reader, config)
        footer_length = int.from_bytes(self.data[-8:-4], 'little')
        selected = pq.read_metadata(pa.BufferReader(self.data)).row_group(0)
        selected_bytes = sum(
            selected.column(index).total_compressed_size
            for index in range(selected.num_columns)
            if selected.column(index).path_in_schema in remote_parquet.COLUMNS
        )
        self.assertTrue(result['success'])
        self.assertEqual(result['row_count'], 6)
        self.assertEqual(result['row_group_count'], 1)
        self.assertEqual(result['selected_compressed_bytes'], selected_bytes)
        self.assertEqual(result['metadata_application_body_bytes'],
                         footer_length + 12)
        self.assertEqual(result['estimated_application_body_bytes'],
                         footer_length + 12 + selected_bytes)
        self.assertEqual(result['http_request_count'], 2)
        self.assertEqual(transport.requests, [
            (len(self.data) - 8, len(self.data)),
            (len(self.data) - 8 - footer_length, len(self.data) - 8),
        ])

    def test_metadata_mismatch_fails_closed(self):
        config = self.config(expected_rows=7)
        reader, _ = self.reader(config)
        result = preflight.inspect_preflight(reader, config)
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_category'], 'source_metadata_mismatch')

    def test_forbidden_overlap_fails_before_column_reads(self):
        config = self.config()
        reader, transport = self.reader(config)
        with patch.object(
                preflight.remote_parquet, 'plan_first_group',
                side_effect=remote_parquet.SafetyError('html_overlap')):
            result = preflight.inspect_preflight(reader, config)
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_category'], 'invalid_parquet_metadata')
        self.assertEqual(len(transport.requests), 2)

    def test_selected_bytes_over_budget_fail_closed(self):
        footer_length = int.from_bytes(self.data[-8:-4], 'little')
        config = self.config(application_body_budget=footer_length + 20)
        reader, _ = self.reader(config)
        result = preflight.inspect_preflight(reader, config)
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_category'], 'budget_precheck')

    def test_no_live_flag_refuses_without_constructing_reader(self):
        output = io.StringIO()
        with patch.object(preflight, 'run_live_preflight') as live, \
                contextlib.redirect_stdout(output):
            exit_code = preflight.main([])
        result = json.loads(output.getvalue())
        self.assertEqual(exit_code, 2)
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_category'], 'live_flag_required')
        live.assert_not_called()

    def test_transport_failure_is_sanitized(self):
        config = self.config()
        transport = FakeTransport(self.data, 'SIGNED_URL_PRIVATE_TOKEN')
        reader, _ = self.reader(config, transport)
        result = preflight.inspect_preflight(reader, config)
        serialized = json.dumps(result)
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_category'], 'transport_error')
        self.assertNotIn('SIGNED_URL', serialized)
        self.assertNotIn('PRIVATE_TOKEN', serialized)


if __name__ == '__main__':
    unittest.main()

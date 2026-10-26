"""Fully offline tests for multi-shard preflight and ingestion."""

import contextlib
import datetime
import io
import json
from pathlib import Path
import socket
import struct
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from adaptivephish.data import multi_shard, output_writer, remote_parquet
from adaptivephish.data.shard_inventory import ShardSpec


def make_parquet(rows=18):
    table = pa.table({
        'sha256': [f'{index:064x}' for index in range(rows)],
        'url': [f'https://synthetic-{index}.invalid/path' for index in range(rows)],
        'label': ['benign', 'phish'] * (rows // 2),
        'target': [None] * rows,
        'date': [datetime.date(2025, 1, 1) + datetime.timedelta(index)
                 for index in range(rows)],
        'lang': ['en'] * rows,
        'lang_score': [0.9] * rows,
        'html': [('PRIVATE_HTML_' + str(index)) * 10_000 for index in range(rows)],
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=6, compression='NONE')
    return sink.getvalue().to_pybytes(), table


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


class Transport:
    def __init__(self, data):
        self.data, self.requests = data, []

    @contextlib.contextmanager
    def __call__(self, url, headers):
        start, inclusive_end = map(int, headers['Range'][6:].split('-'))
        end = inclusive_end + 1
        self.requests.append((start, end))
        yield Response(self.data[start:end], start, end, len(self.data))


def overlaps(left, right):
    return left[0] < right[1] and right[0] < left[1]


class MultiShardTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('network forbidden')
        )
        self.network.start()
        self.addCleanup(self.network.stop)
        self.data, self.table = make_parquet()
        self.shard = ShardSpec(
            'train', 'data/train-000.parquet', len(self.data),
            self.table.num_rows, 3, 2 * 1024 * 1024, 32,
        )

    def reader(self, budget=None):
        transport = Transport(self.data)
        shard = self.shard
        if budget is not None:
            shard = ShardSpec(shard.split, shard.path, shard.expected_file_size,
                              shard.expected_rows, shard.expected_row_groups,
                              budget, shard.maximum_http_requests)
        reader = remote_parquet.RangeFile(
            url=shard.url, size=len(self.data), budget=shard.application_body_budget,
            transport=transport, request_limit=shard.maximum_http_requests,
        )
        return reader, transport, shard

    def test_multi_row_group_preflight_never_reads_columns(self):
        reader, transport, shard = self.reader()
        metadata, plan = multi_shard.preflight_reader(
            reader, shard, shard.application_body_budget
        )
        self.assertEqual(plan.row_groups, 3)
        self.assertEqual(plan.rows, 18)
        self.assertEqual(len(plan.selected_ranges), 12)
        self.assertEqual(len(transport.requests), 3)
        html_ranges = []
        for group_index in range(metadata.num_row_groups):
            html = metadata.row_group(group_index).column(7)
            start = min(html.data_page_offset, html.dictionary_page_offset)
            html_ranges.append((start, start + html.total_compressed_size))
        self.assertTrue(all(not overlaps(selected, html)
                            for selected in plan.selected_ranges
                            for html in html_ranges))
        reader.close()

    def test_budget_and_exact_approval_fail_before_column_reads(self):
        footer_length = struct.unpack('<I', self.data[-8:-4])[0]
        reader, transport, shard = self.reader(budget=footer_length + 112)
        with self.assertRaises(multi_shard.MultiShardError) as error:
            multi_shard.preflight_reader(reader, shard, footer_length + 112)
        self.assertEqual(str(error.exception), 'budget_precheck')
        self.assertEqual(len(transport.requests), 3)
        reader.close()

        reader, transport, shard = self.reader()
        with tempfile.TemporaryDirectory() as directory, patch.object(
                output_writer, 'APPROVED_DIRECTORY', Path(directory)), patch.object(
                multi_shard, 'APPROVED_DIRECTORY', Path(directory)), patch.object(
                output_writer, '_require_git_ignored'):
            with self.assertRaises(multi_shard.MultiShardError) as mismatch:
                multi_shard.ingest_reader(
                    reader, shard, Path(directory) / 'train-000-url-metadata.parquet',
                    shard.application_body_budget, approved_estimate=1,
                )
        self.assertEqual(str(mismatch.exception), 'preflight_approval_mismatch')
        self.assertEqual(len(transport.requests), 3)

    def test_ingestion_preserves_values_and_skips_verified_existing(self):
        plan_reader, _, shard = self.reader()
        _, plan = multi_shard.preflight_reader(
            plan_reader, shard, shard.application_body_budget
        )
        plan_reader.close()
        with tempfile.TemporaryDirectory() as directory, patch.object(
                output_writer, 'APPROVED_DIRECTORY', Path(directory)), patch.object(
                multi_shard, 'APPROVED_DIRECTORY', Path(directory)), patch.object(
                output_writer, '_require_git_ignored'):
            output = Path(directory) / 'train-000-url-metadata.parquet'
            reader, transport, _ = self.reader()
            outcome = multi_shard.ingest_reader(
                reader, shard, output, shard.application_body_budget,
                approved_estimate=plan.estimated_body_bytes,
            )
            self.assertEqual(outcome['status'], 'ingested')
            actual = pq.read_table(output)
            self.assertEqual(actual.to_pylist(), self.table.select(
                list(remote_parquet.COLUMNS)).to_pylist())
            self.assertFalse(any('PRIVATE_HTML' in str(value)
                                 for value in outcome['manifest'].values()))
            original = output.read_bytes()
            second, second_transport, _ = self.reader()
            skipped = multi_shard.ingest_reader(
                second, shard, output, shard.application_body_budget,
                approved_estimate=plan.estimated_body_bytes,
            )
            self.assertEqual(skipped['status'], 'verified_existing')
            self.assertEqual(second_transport.requests, [])
            self.assertEqual(output.read_bytes(), original)

    def test_forbidden_html_range_remains_unauthorized(self):
        reader, transport, shard = self.reader()
        metadata, _ = multi_shard.preflight_reader(
            reader, shard, shard.application_body_budget
        )
        html = metadata.row_group(0).column(7)
        start = min(html.data_page_offset, html.dictionary_page_offset)
        with self.assertRaises(remote_parquet.SafetyError):
            reader.fetch(start, start + html.total_compressed_size)
        self.assertEqual(len(transport.requests), 3)
        reader.close()

    def test_incomplete_existing_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
                output_writer, 'APPROVED_DIRECTORY', Path(directory)), patch.object(
                multi_shard, 'APPROVED_DIRECTORY', Path(directory)):
            output = Path(directory) / 'train-000-url-metadata.parquet'
            output.write_bytes(b'INCOMPLETE_PRIVATE_BYTES')
            original = output.read_bytes()
            reader, transport, shard = self.reader()
            with self.assertRaises(multi_shard.MultiShardError) as error:
                multi_shard.ingest_reader(
                    reader, shard, output, shard.application_body_budget,
                    approved_estimate=1,
                )
            self.assertEqual(str(error.exception),
                             'existing_output_not_replaceable')
            self.assertEqual(transport.requests, [])
            self.assertEqual(output.read_bytes(), original)

    def test_metadata_discovered_shard_requires_explicit_preflight_budget(self):
        shard = ShardSpec(
            'train', 'data/train-000.parquet', len(self.data), None, None,
            None, 32, 'metadata_discovered',
        )
        transport = Transport(self.data)
        with self.assertRaises(multi_shard.MultiShardError) as error:
            multi_shard.make_reader(shard, transport=transport)
        self.assertEqual(str(error.exception), 'footer_preflight_required')

        reader = multi_shard.make_reader(
            shard, transport=transport, budget=2 * 1024 * 1024
        )
        metadata, plan = multi_shard.preflight_reader(
            reader, shard, 2 * 1024 * 1024
        )
        self.assertEqual(metadata.num_rows, self.table.num_rows)
        self.assertGreater(plan.estimated_body_bytes, 0)
        self.assertEqual(len(transport.requests), 3)
        reader.close()

    def test_persisted_approved_estimate_is_required_before_network_reads(self):
        shard = ShardSpec(
            'train', 'data/train-000.parquet', len(self.data),
            self.table.num_rows, 3, 2 * 1024 * 1024, 32,
            'footer_verified', 70_613,
        )
        transport = Transport(self.data)
        reader = multi_shard.make_reader(shard, transport=transport)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(multi_shard.MultiShardError) as error:
                multi_shard.ingest_reader(
                    reader, shard, Path(directory) / 'unused.parquet',
                    100_000, approved_estimate=70_612,
                )
        self.assertEqual(str(error.exception), 'preflight_approval_mismatch')
        self.assertEqual(transport.requests, [])

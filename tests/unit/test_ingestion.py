"""Offline end-to-end tests for the ingestion orchestration layer."""

import contextlib
import datetime
import io
import socket
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from adaptivephish.data import ingestion, output_writer, remote_parquet


def make_source(rows=None, large_strings=False):
    rows = rows or [
        ('a' * 64, 'https://first.invalid/path', 'benign', datetime.date(2025, 1, 1)),
        ('b' * 64, 'second.invalid/path', 'phish', datetime.date(2025, 1, 2)),
        ('a' * 64, 'https://first.invalid/path', 'benign', datetime.date(2025, 1, 1)),
    ]
    text_type = pa.large_string() if large_strings else pa.string()
    table = pa.table({
        'sha256': pa.array([row[0] for row in rows], type=text_type),
        'url': pa.array([row[1] for row in rows], type=text_type),
        'label': pa.array([row[2] for row in rows], type=text_type),
        'target': [None] * len(rows),
        'date': [row[3] for row in rows],
        'lang': ['en'] * len(rows),
        'lang_score': [0.9] * len(rows),
        'html': [('PRIVATE_HTML_' + str(index) + '_') * 20_000
                 for index in range(len(rows))],
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=len(rows), compression='NONE')
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


def interval(column):
    offsets = [column.data_page_offset]
    if column.has_dictionary_page:
        offsets.append(column.dictionary_page_offset)
    start = min(offsets)
    return start, start + column.total_compressed_size


def overlaps(left, right):
    return left[0] < right[1] and right[0] < left[1]


class IngestionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.test_directory = Path(self.temporary_directory.name)
        self.directory_patch = patch.object(
            output_writer, 'APPROVED_DIRECTORY', self.test_directory
        )
        self.directory_patch.start()
        self.addCleanup(self.directory_patch.stop)
        self.ignore_patch = patch.object(output_writer, '_require_git_ignored')
        self.ignore_patch.start()
        self.addCleanup(self.ignore_patch.stop)
        self.output = output_writer.APPROVED_DIRECTORY / 'ingestion-offline.parquet'
        self.sidecar = output_writer.manifest_path(self.output)

    def reader(self, data):
        transport = LocalRangeTransport(data)
        reader = remote_parquet.RangeFile(
            size=len(data), budget=2 * 1024 * 1024, transport=transport
        )
        return reader, transport

    def test_reader_validator_writer_success_preserves_records(self):
        data, source = make_source()
        reader, transport = self.reader(data)
        document = ingestion._ingest_with_offline_reader(
            reader, self.output, expected_rows=source.num_rows
        )
        result = pq.read_table(self.output)
        expected = source.select(list(remote_parquet.COLUMNS))
        self.assertTrue(result.equals(expected))
        self.assertEqual(document['row_count'], 3)
        self.assertEqual(document['duplicate_counts'], {
            'sha256': 1, 'url': 1, 'row': 1, 'conflicting_sha256': 0,
        })
        self.assertEqual(output_writer.verify_completed_output(self.output), document)

        metadata = pq.read_metadata(pa.BufferReader(data))
        blocked = [
            interval(metadata.row_group(0).column(index))
            for index in range(metadata.row_group(0).num_columns)
            if metadata.row_group(0).column(index).path_in_schema
            not in remote_parquet.COLUMNS
        ]
        self.assertTrue(all(
            not overlaps(requested, denied)
            for requested in transport.requests for denied in blocked
        ))

        serialized = self.sidecar.read_text(encoding='utf-8')
        for secret in ('first.invalid', 'second.invalid', 'a' * 64,
                       'PRIVATE_HTML'):
            self.assertNotIn(secret, serialized)

    def test_invalid_records_fail_without_output(self):
        rows = [('bad-id', 'https://invalid.invalid', 'PRIVATE_LABEL',
                 datetime.date(2025, 1, 1))]
        data, source = make_source(rows)
        reader, _ = self.reader(data)
        with self.assertRaises(ingestion.IngestionError) as error:
            ingestion._ingest_with_offline_reader(
                reader, self.output, expected_rows=source.num_rows
            )
        self.assertEqual(error.exception.category, 'validation_failed')
        self.assertNotIn('PRIVATE_LABEL', str(error.exception))
        self.assertFalse(self.output.exists())
        self.assertFalse(self.sidecar.exists())
        self.assertEqual(list(output_writer.APPROVED_DIRECTORY.glob('.adaptivephish-*')), [])

    def test_large_string_source_is_published_canonically_without_changes(self):
        rows = [
            ('c' * 64, 'HTTPS://MixedCase.invalid/A Path', 'benign',
             datetime.date(2025, 4, 1)),
            ('d' * 64, 'no-scheme.invalid/Exact?Q=Value', 'phish',
             datetime.date(2025, 4, 2)),
        ]
        data, source = make_source(rows, large_strings=True)
        reader, _ = self.reader(data)
        ingestion._ingest_with_offline_reader(
            reader, self.output, expected_rows=source.num_rows
        )
        result = pq.read_table(self.output)
        self.assertEqual(result.schema, output_writer.EXPECTED_SCHEMA)
        self.assertEqual(result.to_pylist(),
                         source.select(list(remote_parquet.COLUMNS)).to_pylist())

    def test_unexpected_dimensions_fail_before_publication(self):
        data, source = make_source()
        reader, _ = self.reader(data)
        with self.assertRaises(ingestion.IngestionError) as error:
            ingestion._ingest_with_offline_reader(
                reader, self.output, expected_rows=source.num_rows + 1
            )
        self.assertEqual(error.exception.category, 'parquet_read_error')
        self.assertFalse(self.output.exists())
        self.assertFalse(self.sidecar.exists())

    def test_offline_path_rejects_real_transport_and_changed_url(self):
        data, source = make_source()
        live_reader = remote_parquet.RangeFile(size=len(data))
        with self.assertRaises(ingestion.IngestionError) as live_error:
            ingestion._ingest_with_offline_reader(
                live_reader, self.output, source.num_rows
            )
        self.assertEqual(live_error.exception.category,
                         'offline_test_source_rejected')

        reader, _ = self.reader(data)
        reader.url = 'https://example.invalid/private'
        with self.assertRaises(ingestion.IngestionError) as url_error:
            ingestion._ingest_with_offline_reader(
                reader, self.output, source.num_rows
            )
        self.assertEqual(url_error.exception.category,
                         'offline_test_source_rejected')

    def test_range_reader_rejects_html_and_unapproved_ranges(self):
        data, _ = make_source()
        reader, transport = self.reader(data)
        metadata, footer_start = remote_parquet.inspect_footer(reader)
        remote_parquet.plan_first_group(reader, metadata, footer_start)
        html = metadata.row_group(0).column(7)
        html_range = interval(html)
        with self.assertRaises(remote_parquet.SafetyError) as error:
            reader.fetch(*html_range)
        self.assertEqual(error.exception.category, 'unapproved_range')
        self.assertTrue(all(not overlaps(request, html_range)
                            for request in transport.requests))
        reader.close()

    def test_writer_failure_is_sanitized_and_cleans_temporary_files(self):
        data, source = make_source()
        reader, _ = self.reader(data)
        with patch.object(output_writer.pq, 'ParquetWriter',
                          side_effect=OSError('PRIVATE_TRANSPORT_EXCEPTION')):
            with self.assertRaises(ingestion.IngestionError) as error:
                ingestion._ingest_with_offline_reader(
                    reader, self.output, expected_rows=source.num_rows
                )
        self.assertEqual(error.exception.category, 'output_write_failed')
        self.assertNotIn('PRIVATE', str(error.exception))
        self.assertFalse(self.output.exists())
        self.assertFalse(self.sidecar.exists())
        self.assertEqual(list(output_writer.APPROVED_DIRECTORY.glob('.adaptivephish-*')), [])

    def test_production_path_enforces_approved_contract_and_row_count(self):
        config = ingestion._load_approved_contract()
        self.assertEqual(config.expected_rows, 5_780)
        with patch.object(ingestion.remote_parquet, 'RangeFile') as range_file:
            sentinel = object()
            range_file.return_value = sentinel
            with patch.object(ingestion, '_publish', return_value={}) as publish:
                ingestion.ingest_approved_shard(self.output)
        range_file.assert_called_once_with(
            url=remote_parquet.URL,
            size=322_018_201,
            budget=2_097_152,
        )
        publish.assert_called_once_with(
            sentinel,
            self.output,
            expected_row_groups=1,
            expected_rows=5_780,
            approved_shard=True,
        )


if __name__ == '__main__':
    unittest.main()

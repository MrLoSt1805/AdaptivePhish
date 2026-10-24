"""Offline validation of four-column Parquet projection.

A small output batch can still require complete compressed column chunks. The
tests verify byte-range selection; they do not claim that batch size limits
the amount of compressed data read.
"""

import contextlib
import datetime
import importlib.util
import io
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
READER_PATH = ROOT / 'scripts' / 'test_remote_parquet.py'
SPEC = importlib.util.spec_from_file_location('projection_range_reader', READER_PATH)
reader_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reader_module)

SELECTED = ('sha256', 'url', 'label', 'date')


def make_parquet():
    rows = 20
    table = pa.table({
        'sha256': [f'{index:064x}' for index in range(rows)],
        'url': [f'https://synthetic-{index}.invalid/path' for index in range(rows)],
        'label': ['benign', 'phish'] * (rows // 2),
        'target': [None, 'synthetic'] * (rows // 2),
        'date': [datetime.date(2025, 1, 1) + datetime.timedelta(days=index)
                 for index in range(rows)],
        'lang': ['en'] * rows,
        'lang_score': [0.9] * rows,
        # Unique, uncompressed values keep this column deliberately large.
        'html': [('HTML_ONLY_' + str(index) + '_') * 20_000
                 for index in range(rows)],
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=rows, compression='NONE')
    return sink.getvalue().to_pybytes()


def chunk_ranges(metadata):
    ranges = {}
    group = metadata.row_group(0)
    for index in range(group.num_columns):
        column = group.column(index)
        offsets = [column.data_page_offset]
        if column.has_dictionary_page:
            offsets.append(column.dictionary_page_offset)
        start = min(offsets)
        ranges[column.path_in_schema] = (
            start, start + column.total_compressed_size
        )
    return ranges


def overlaps(left, right):
    return left[0] < right[1] and right[0] < left[1]


class TrackingFile(io.RawIOBase):
    """Seekable local file that records every range returned to PyArrow."""

    def __init__(self, data):
        super().__init__()
        self.data = data
        self.position = 0
        self.reads = []

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        base = 0 if whence == 0 else self.position if whence == 1 else len(self.data)
        target = base + offset
        if target < 0 or target > len(self.data):
            raise ValueError('invalid seek')
        self.position = target
        return target

    def read(self, size=-1):
        if size is None or size < 0:
            end = len(self.data)
        else:
            end = min(len(self.data), self.position + size)
        start = self.position
        self.position = end
        if end > start:
            self.reads.append((start, end))
        return self.data[start:end]

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


class Response:
    status = 206

    def __init__(self, data, start, end, total):
        self.data = io.BytesIO(data)
        self.headers = [
            ('Content-Range', f'bytes {start}-{end - 1}/{total}'),
            ('Content-Length', str(end - start)),
        ]

    def getheaders(self):
        return self.headers

    def read(self, size):
        return self.data.read(size)


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


class ColumnProjectionTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch(
            'socket.socket.connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)
        self.data = make_parquet()
        self.metadata = pq.read_metadata(pa.BufferReader(self.data))
        self.ranges = chunk_ranges(self.metadata)
        footer_length = struct.unpack('<I', self.data[-8:-4])[0]
        self.footer_range = (len(self.data) - 8 - footer_length, len(self.data))

    def test_projection_reads_only_selected_column_data(self):
        tracked = TrackingFile(self.data)
        parquet = pq.ParquetFile(
            tracked,
            metadata=self.metadata,
            pre_buffer=False,
            buffer_size=0,
            arrow_extensions_enabled=False,
        )
        try:
            table = parquet.read_row_group(
                0, columns=list(SELECTED), use_threads=False
            )
        finally:
            parquet.close()

        self.assertEqual(table.column_names, list(SELECTED))
        self.assertEqual(table.num_rows, 20)
        self.assertNotIn('html', table.column_names)

        footer_reads = [item for item in tracked.reads
                        if overlaps(item, self.footer_range)]
        html_reads = [item for item in tracked.reads
                      if overlaps(item, self.ranges['html'])]
        selected_reads = {
            name: [item for item in tracked.reads
                   if overlaps(item, self.ranges[name])]
            for name in SELECTED
        }

        # Metadata was supplied from a separate in-memory parse. These are
        # column-data reads, distinguished explicitly from footer reads.
        self.assertEqual(footer_reads, [])
        self.assertEqual(html_reads, [])
        self.assertTrue(all(selected_reads[name] for name in SELECTED))

    def test_small_batch_excludes_html_without_implying_small_io(self):
        tracked = TrackingFile(self.data)
        parquet = pq.ParquetFile(
            tracked, metadata=self.metadata, pre_buffer=False, buffer_size=0,
            arrow_extensions_enabled=False,
        )
        batches = parquet.iter_batches(
            batch_size=3, row_groups=[0], columns=list(SELECTED),
            use_threads=False,
        )
        try:
            batch = next(batches)
        finally:
            batches.close()
            parquet.close()

        self.assertEqual(batch.num_rows, 3)
        self.assertEqual(batch.schema.names, list(SELECTED))
        self.assertNotIn('html', batch.schema.names)
        self.assertFalse(any(overlaps(item, self.ranges['html'])
                             for item in tracked.reads))
        # At least one read may span a complete selected compressed chunk even
        # though only three output rows were requested.
        self.assertTrue(tracked.reads)

    def test_range_authorization_rejects_html_chunk(self):
        transport = LocalRangeTransport(self.data)
        reader = reader_module.RangeFile(
            size=len(self.data), budget=reader_module.BUDGET,
            transport=transport,
        )
        self.addCleanup(reader.close)
        reader.allowed = [self.ranges[name] for name in SELECTED]

        html_start, html_end = self.ranges['html']
        with self.assertRaises(reader_module.SafetyError) as error:
            reader.fetch(html_start, html_end)

        self.assertEqual(error.exception.category, 'unapproved_range')
        self.assertEqual(transport.requests, [])
        self.assertEqual(reader.body_bytes, 0)


if __name__ == '__main__':
    unittest.main()

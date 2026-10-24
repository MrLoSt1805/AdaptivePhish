"""Offline tests for the live metadata check's local footer parser."""

import datetime
import importlib.util
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
    'offline_remote_parquet_reader', ROOT / 'scripts' / 'test_remote_parquet.py'
)
metadata_module = load_module(
    'offline_live_metadata', ROOT / 'scripts' / 'check_live_metadata.py'
)


def make_parquet(column_names=metadata_module.EXPECTED_COLUMNS):
    values = {
        'sha256': ['a' * 64, 'b' * 64, 'c' * 64, 'd' * 64],
        'url': [f'https://synthetic-{index}.invalid/' for index in range(4)],
        'label': ['benign', 'phish', 'benign', 'phish'],
        'target': [None, 'synthetic', None, 'synthetic'],
        'date': [datetime.date(2025, 1, day) for day in range(1, 5)],
        'lang': ['en'] * 4,
        'lang_score': [0.9, 0.8, 0.7, 0.6],
        'html': [f'<html>synthetic-{index}</html>' for index in range(4)],
    }
    table = pa.table({name: values[name] for name in column_names})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=2, compression='NONE')
    return sink.getvalue().to_pybytes()


def footer_parts(parquet_bytes):
    trailer = parquet_bytes[-8:]
    footer_length = struct.unpack('<I', trailer[:4])[0]
    footer_start = len(parquet_bytes) - 8 - footer_length
    return parquet_bytes[footer_start:-8], trailer, footer_start


class LiveMetadataParserTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch(
            'socket.socket.connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)

    def test_real_footer_parses_from_in_memory_wrapper(self):
        parquet_bytes = make_parquet()
        footer, trailer, footer_start = footer_parts(parquet_bytes)

        metadata, names, selected_sizes = metadata_module.parse_metadata(
            reader_module, footer, trailer, footer_start
        )

        original = pq.read_metadata(pa.BufferReader(parquet_bytes))
        expected_sizes = {name: 0 for name in reader_module.COLUMNS}
        for group_index in range(original.num_row_groups):
            group = original.row_group(group_index)
            for column_index in range(group.num_columns):
                column = group.column(column_index)
                if column.path_in_schema in expected_sizes:
                    expected_sizes[column.path_in_schema] += column.total_compressed_size

        self.assertEqual(trailer[4:], b'PAR1')
        self.assertEqual(metadata.num_rows, 4)
        self.assertEqual(metadata.num_row_groups, 2)
        self.assertEqual(names, list(metadata_module.EXPECTED_COLUMNS))
        self.assertEqual(selected_sizes, expected_sizes)
        self.assertTrue(all(size > 0 for size in selected_sizes.values()))

    def test_invalid_schema_is_rejected(self):
        columns = tuple(
            name for name in metadata_module.EXPECTED_COLUMNS if name != 'html'
        )
        parquet_bytes = make_parquet(columns)
        footer, trailer, footer_start = footer_parts(parquet_bytes)

        with self.assertRaises(reader_module.SafetyError) as error:
            metadata_module.parse_metadata(
                reader_module, footer, trailer, footer_start
            )

        self.assertEqual(error.exception.category, 'Invalid Parquet schema')

    def test_column_chunk_outside_original_file_is_rejected(self):
        parquet_bytes = make_parquet()
        footer, trailer, _ = footer_parts(parquet_bytes)

        # A boundary before every real column chunk simulates invalid metadata
        # offsets without changing or hand-crafting the Thrift footer.
        invalid_footer_start = 4
        with self.assertRaises(reader_module.SafetyError) as error:
            metadata_module.parse_metadata(
                reader_module, footer, trailer, invalid_footer_start
            )

        self.assertEqual(error.exception.category, 'Invalid Parquet metadata')


if __name__ == '__main__':
    unittest.main()

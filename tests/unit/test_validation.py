"""Fully offline tests for Arrow RecordBatch validation."""

import datetime
import json
import socket
import struct
import unittest
from unittest.mock import patch

import pyarrow as pa

from adaptivephish.data.validation import ValidationError, validate_batches


COLUMNS = ('sha256', 'url', 'label', 'date')


def batch(rows, schema=None):
    schema = schema or pa.schema([
        ('sha256', pa.string()),
        ('url', pa.string()),
        ('label', pa.string()),
        ('date', pa.date32()),
    ])
    arrays = [pa.array([row[index] for row in rows], type=field.type)
              for index, field in enumerate(schema)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)
        self.rows = [
            ('a' * 64, 'https://first.invalid/path', 'benign',
             datetime.date(2025, 1, 2)),
            ('b' * 64, 'second.invalid/path', 'phish',
             datetime.date(2025, 1, 5)),
        ]

    def test_valid_batches_produce_only_sanitized_aggregates(self):
        summary = validate_batches([batch(self.rows[:1]), batch(self.rows[1:])])
        self.assertTrue(summary.valid)
        self.assertEqual(summary.row_count, 2)
        self.assertEqual(summary.label_counts, {'benign': 1, 'phish': 1})
        self.assertEqual(summary.earliest_date, '2025-01-02')
        self.assertEqual(summary.latest_date, '2025-01-05')
        self.assertEqual(summary.url_missing_scheme_count, 1)
        report = json.dumps(summary.to_dict(), sort_keys=True)
        self.assertNotIn('first.invalid', report)
        self.assertNotIn('second.invalid', report)
        self.assertNotIn('a' * 64, report)

    def test_nulls_are_counted_and_rejected(self):
        rows = [(None, None, None, None)]
        with self.assertRaises(ValidationError) as error:
            validate_batches([batch(rows)])
        self.assertEqual(error.exception.category, 'missing_required_value')
        self.assertEqual(error.exception.summary.missing_fields,
                         dict.fromkeys(COLUMNS, 1))

    def test_wrong_column_order_and_types_are_rejected(self):
        wrong_order = pa.record_batch([
            pa.array(['https://x.invalid']), pa.array(['a' * 64]),
            pa.array(['benign']), pa.array([datetime.date(2025, 1, 1)])
        ], names=['url', 'sha256', 'label', 'date'])
        with self.assertRaises(ValidationError) as order_error:
            validate_batches([wrong_order])
        self.assertEqual(order_error.exception.category, 'invalid_column_schema')

        wrong_type = pa.record_batch([
            pa.array(['a' * 64]), pa.array(['https://x.invalid']),
            pa.array(['benign']), pa.array(['2025-01-01'])
        ], names=list(COLUMNS))
        with self.assertRaises(ValidationError) as type_error:
            validate_batches([wrong_type])
        self.assertEqual(type_error.exception.category, 'invalid_arrow_types')

    def test_invalid_identifier_label_and_empty_url_are_counted(self):
        rows = [('not-a-hash', '   ', 'unknown', datetime.date(2025, 1, 1))]
        with self.assertRaises(ValidationError) as error:
            validate_batches([batch(rows)])
        self.assertEqual(error.exception.category, 'invalid_required_value')
        self.assertEqual(error.exception.summary.invalid_values,
                         {'sha256': 1, 'url': 1, 'label': 1, 'date': 0})
        self.assertNotIn('not-a-hash', str(error.exception))
        self.assertNotIn('unknown', str(error.exception))

    def test_unrepresentable_date_is_sanitized_and_rejected(self):
        invalid_date = pa.Array.from_buffers(
            pa.date32(), 1, [None, pa.py_buffer(struct.pack('<i', 2_147_483_647))]
        )
        record_batch = pa.RecordBatch.from_arrays([
            pa.array(['c' * 64]), pa.array(['https://date.invalid']),
            pa.array(['benign']), invalid_date,
        ], names=list(COLUMNS))
        with self.assertRaises(ValidationError) as error:
            validate_batches([record_batch])
        self.assertEqual(error.exception.category, 'invalid_required_value')
        self.assertEqual(error.exception.summary.invalid_values['date'], 1)

    def test_duplicates_across_batches_are_reported_without_removal(self):
        first = self.rows[0]
        summary = validate_batches([batch([first]), batch([first])])
        self.assertEqual(summary.row_count, 2)
        self.assertEqual(summary.duplicate_sha256_count, 1)
        self.assertEqual(summary.duplicate_url_count, 1)
        self.assertEqual(summary.duplicate_row_count, 1)
        self.assertEqual(summary.conflicting_sha256_count, 0)

    def test_conflicting_identifier_across_batches_is_rejected(self):
        original = self.rows[0]
        conflict = (original[0], 'https://changed.invalid', 'phish', original[3])
        with self.assertRaises(ValidationError) as error:
            validate_batches([batch([original]), batch([conflict])])
        self.assertEqual(error.exception.category, 'conflicting_identifier')
        self.assertEqual(error.exception.summary.conflicting_sha256_count, 1)
        serialized = json.dumps(error.exception.summary.to_dict())
        self.assertNotIn('changed.invalid', serialized)
        self.assertNotIn(original[0], serialized)

    def test_non_batch_and_processing_failures_are_sanitized(self):
        with self.assertRaises(ValidationError) as type_error:
            validate_batches([object()])
        self.assertEqual(type_error.exception.category, 'invalid_batch')

        def broken_batches():
            yield batch(self.rows)
            raise RuntimeError('https://secret.invalid/token')

        with self.assertRaises(ValidationError) as processing_error:
            validate_batches(broken_batches())
        self.assertEqual(processing_error.exception.category,
                         'batch_processing_error')
        self.assertNotIn('secret.invalid', str(processing_error.exception))


if __name__ == '__main__':
    unittest.main()

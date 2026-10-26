"""Offline tests for safe local Parquet publication."""

import datetime
import hashlib
import json
import socket
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from adaptivephish.data import output_writer


def make_batch(rows=None):
    rows = rows or [
        ('a' * 64, 'https://first.invalid/path', 'benign', datetime.date(2025, 1, 1)),
        ('b' * 64, 'second.invalid/path', 'phish', datetime.date(2025, 1, 2)),
    ]
    schema = output_writer.EXPECTED_SCHEMA
    return pa.RecordBatch.from_arrays([
        pa.array([row[index] for row in rows], type=field.type)
        for index, field in enumerate(schema)
    ], schema=schema)


class OutputWriterTests(unittest.TestCase):
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
        self.output = output_writer.APPROVED_DIRECTORY / 'offline-test.parquet'
        self.sidecar = output_writer.manifest_path(self.output)

    def test_success_schema_checksum_manifest_and_completion(self):
        document = output_writer.write_validated_parquet(
            [make_batch()], self.output
        )
        table = pq.read_table(self.output)
        self.assertEqual(table.schema, output_writer.EXPECTED_SCHEMA)
        self.assertEqual(table.num_rows, 2)
        checksum = hashlib.sha256(self.output.read_bytes()).hexdigest()
        self.assertEqual(document['output_sha256'], checksum)
        self.assertEqual(document['completion_status'], 'complete')
        self.assertEqual(document['label_counts'], {'benign': 1, 'phish': 1})
        self.assertEqual(output_writer.verify_completed_output(self.output), document)

        serialized = self.sidecar.read_text(encoding='utf-8')
        for raw_value in ('first.invalid', 'second.invalid', 'a' * 64, 'b' * 64):
            self.assertNotIn(raw_value, serialized)
        self.assertNotIn('html', serialized.lower())

    def test_invalid_data_leaves_no_output_or_temporary_files(self):
        invalid = make_batch([
            ('bad', 'https://invalid.invalid', 'unknown', datetime.date(2025, 1, 1))
        ])
        with self.assertRaises(output_writer.OutputWriterError) as error:
            output_writer.write_validated_parquet([invalid], self.output)
        self.assertEqual(error.exception.category, 'validation_failed')
        self.assertFalse(self.output.exists())
        self.assertFalse(self.sidecar.exists())
        self.assertEqual(list(output_writer.APPROVED_DIRECTORY.glob('.adaptivephish-*')), [])

    def test_wrong_and_traversal_paths_are_rejected(self):
        paths = [
            output_writer.WORKSPACE_ROOT / 'outside.parquet',
            output_writer.APPROVED_DIRECTORY / '..' / 'escaped.parquet',
            output_writer.APPROVED_DIRECTORY / 'wrong.csv',
            output_writer.APPROVED_DIRECTORY / 'nested' / 'file.parquet',
        ]
        for path in paths:
            with self.subTest(path=path.name):
                with self.assertRaises(output_writer.OutputWriterError) as error:
                    output_writer.write_validated_parquet([make_batch()], path)
                self.assertEqual(error.exception.category, 'unapproved_output_path')

    def test_approved_mode_requires_exact_source_row_count(self):
        with self.assertRaises(output_writer.OutputWriterError) as error:
            output_writer.write_validated_parquet(
                [make_batch()], self.output, approved_shard=True
            )
        self.assertEqual(error.exception.category, 'approved_row_count_mismatch')
        self.assertFalse(self.output.exists())

    def test_write_failure_preserves_existing_complete_output_and_cleans_temp(self):
        original = output_writer.write_validated_parquet([make_batch()], self.output)
        original_bytes = self.output.read_bytes()
        original_manifest = self.sidecar.read_bytes()
        with patch.object(output_writer.pq, 'ParquetWriter',
                          side_effect=OSError('PRIVATE_VALUE')):
            with self.assertRaises(output_writer.OutputWriterError) as error:
                output_writer.write_validated_parquet([make_batch()], self.output)
        self.assertEqual(error.exception.category, 'output_write_failed')
        self.assertEqual(self.output.read_bytes(), original_bytes)
        self.assertEqual(self.sidecar.read_bytes(), original_manifest)
        self.assertEqual(output_writer.verify_completed_output(self.output), original)
        self.assertEqual(list(output_writer.APPROVED_DIRECTORY.glob('.adaptivephish-*')), [])

    def test_manifest_publication_failure_is_detected_as_incomplete(self):
        original_replace = output_writer.os.replace
        calls = 0

        def fail_second(source, destination):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError('PRIVATE_MANIFEST_FAILURE')
            return original_replace(source, destination)

        with patch.object(output_writer.os, 'replace', side_effect=fail_second):
            with self.assertRaises(output_writer.OutputWriterError) as error:
                output_writer.write_validated_parquet([make_batch()], self.output)
        self.assertEqual(error.exception.category, 'output_write_failed')
        self.assertTrue(self.output.exists())
        self.assertFalse(self.sidecar.exists())
        with self.assertRaises(output_writer.OutputWriterError) as verify_error:
            output_writer.verify_completed_output(self.output)
        self.assertEqual(verify_error.exception.category, 'incomplete_output')
        self.assertEqual(list(output_writer.APPROVED_DIRECTORY.glob('.adaptivephish-*')), [])

    def test_parquet_replacement_failure_after_invalidation_is_incomplete(self):
        output_writer.write_validated_parquet([make_batch()], self.output)
        original_bytes = self.output.read_bytes()
        with patch.object(output_writer.os, 'replace',
                          side_effect=OSError('PRIVATE_REPLACE_FAILURE')):
            with self.assertRaises(output_writer.OutputWriterError) as error:
                output_writer.write_validated_parquet([make_batch()], self.output)
        self.assertEqual(error.exception.category, 'output_write_failed')
        self.assertEqual(self.output.read_bytes(), original_bytes)
        self.assertFalse(self.sidecar.exists())
        with self.assertRaises(output_writer.OutputWriterError) as verify_error:
            output_writer.verify_completed_output(self.output)
        self.assertEqual(verify_error.exception.category, 'incomplete_output')
        self.assertEqual(list(output_writer.APPROVED_DIRECTORY.glob('.adaptivephish-*')), [])

    def test_successful_replacement_publishes_matching_manifest_last(self):
        output_writer.write_validated_parquet([make_batch()], self.output)
        replacement = make_batch([
            ('c' * 64, 'https://replacement.invalid', 'phish',
             datetime.date(2025, 2, 1))
        ])
        document = output_writer.write_validated_parquet(
            [replacement], self.output
        )
        verified = output_writer.verify_completed_output(self.output)
        self.assertEqual(verified, document)
        self.assertEqual(document['row_count'], 1)
        self.assertEqual(document['label_counts'], {'benign': 0, 'phish': 1})

    def test_existing_manifest_cannot_validate_replaced_different_parquet(self):
        output_writer.write_validated_parquet([make_batch()], self.output)
        old_manifest = self.sidecar.read_text(encoding='utf-8')
        replacement = make_batch([
            ('c' * 64, 'https://replacement.invalid', 'phish',
             datetime.date(2025, 2, 1))
        ])
        output_writer.write_validated_parquet([replacement], self.output)
        self.sidecar.write_text(old_manifest, encoding='utf-8')
        with self.assertRaises(output_writer.OutputWriterError) as error:
            output_writer.verify_completed_output(self.output)
        self.assertIn(error.exception.category,
                      {'incomplete_output', 'output_verification_failed'})

    def test_git_ignore_failure_stops_before_writing(self):
        failure = output_writer.OutputWriterError('output_not_git_ignored')
        with patch.object(output_writer, '_require_git_ignored',
                          side_effect=failure):
            with self.assertRaises(output_writer.OutputWriterError) as error:
                output_writer.write_validated_parquet([make_batch()], self.output)
        self.assertEqual(error.exception.category, 'output_not_git_ignored')
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()

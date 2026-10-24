"""Offline tests for the fixed-path local dataset quality audit."""

import contextlib
import datetime
import importlib.util
import io
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from adaptivephish.data import output_writer


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'dataset_audit', ROOT / 'scripts' / 'audit_ingested_dataset.py'
)
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)


def make_batch():
    rows = [
        ('a' * 64, 'https://first.invalid/A', 'benign', datetime.date(2025, 1, 2)),
        ('b' * 64, 'second.invalid/B', 'phish', datetime.date(2025, 1, 20)),
        ('c' * 64, 'https://third.invalid/C', 'benign', datetime.date(2025, 2, 1)),
    ]
    return pa.RecordBatch.from_arrays([
        pa.array([row[index] for row in rows], type=field.type)
        for index, field in enumerate(output_writer.EXPECTED_SCHEMA)
    ], schema=output_writer.EXPECTED_SCHEMA)


class DatasetAuditTests(unittest.TestCase):
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
        self.output = self.test_directory / 'audit-offline-test.parquet'
        self.sidecar = output_writer.manifest_path(self.output)
        self.path_patch = patch.object(audit, 'DATASET_PATH', self.output)
        self.path_patch.start()
        self.addCleanup(self.path_patch.stop)

    def create_output(self):
        document = output_writer.write_validated_parquet(
            [make_batch()], self.output
        )
        # Offline synthetic data intentionally has fewer than 5,780 rows.
        return document

    def test_successful_audit_recomputes_sanitized_metrics(self):
        document = self.create_output()
        with patch.object(audit, '_manifest_matches_contract', return_value=True):
            result = audit.audit_approved_dataset()
        self.assertTrue(result['success'])
        self.assertEqual(result['total_rows'], 3)
        self.assertEqual(result['label_counts'], {'benign': 2, 'phish': 1})
        self.assertEqual(result['date_range'], {
            'earliest': '2025-01-02', 'latest': '2025-02-01'
        })
        self.assertEqual(result['missing_url_scheme_count'], 1)
        self.assertEqual(result['monthly_distribution'], {
            '2025-01': {'benign': 1, 'phish': 1},
            '2025-02': {'benign': 1, 'phish': 0},
        })
        self.assertEqual(result['duplicate_counts'], document['duplicate_counts'])
        serialized = json.dumps(result)
        for value in ('first.invalid', 'second.invalid', 'third.invalid', 'a' * 64):
            self.assertNotIn(value, serialized)

    def test_manifest_metric_mismatch_fails_closed(self):
        document = self.create_output()
        mismatched = dict(document)
        mismatched['label_counts'] = {'benign': 3, 'phish': 0}
        with patch.object(audit.output_writer, 'verify_completed_output',
                          return_value=mismatched), patch.object(
                              audit, '_manifest_matches_contract', return_value=True):
            result = audit.audit_approved_dataset()
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_category'], 'quality_manifest_mismatch')

    def test_missing_and_corrupt_outputs_fail_verification(self):
        missing = audit.audit_approved_dataset()
        self.assertFalse(missing['success'])
        self.assertEqual(missing['failure_category'], 'output_verification_failed')

        self.create_output()
        self.output.write_bytes(b'PRIVATE_CORRUPT_PARQUET')
        corrupt = audit.audit_approved_dataset()
        self.assertFalse(corrupt['success'])
        self.assertEqual(corrupt['failure_category'], 'output_verification_failed')
        self.assertNotIn('PRIVATE_CORRUPT', json.dumps(corrupt))

    def test_source_identity_mismatch_fails_before_record_scan(self):
        document = self.create_output()
        changed = dict(document)
        changed['source'] = dict(document['source'], revision='main')
        with patch.object(audit.output_writer, 'verify_completed_output',
                          return_value=changed), patch.object(
                              audit, '_read_batches') as read_batches:
            result = audit.audit_approved_dataset()
        self.assertEqual(result['failure_category'], 'source_manifest_mismatch')
        read_batches.assert_not_called()
        self.assertNotIn('main', json.dumps(result))

    def test_unexpected_arguments_do_not_read_dataset(self):
        output = io.StringIO()
        with patch.object(audit, 'audit_approved_dataset') as run, \
                contextlib.redirect_stdout(output):
            exit_code = audit.main(['private-path'])
        result = json.loads(output.getvalue())
        self.assertEqual(exit_code, 2)
        self.assertEqual(result['failure_category'], 'unexpected_arguments')
        run.assert_not_called()

    def test_unrelated_temporary_artifact_is_not_deleted(self):
        unrelated = self.test_directory / '.adaptivephish-unrelated.tmp'
        unrelated.write_bytes(b'OWNED_BY_ANOTHER_PROCESS')
        self.create_output()
        with patch.object(audit, '_manifest_matches_contract', return_value=True):
            result = audit.audit_approved_dataset()
        self.assertTrue(result['success'])
        self.assertTrue(unrelated.exists())
        self.assertEqual(unrelated.read_bytes(), b'OWNED_BY_ANOTHER_PROCESS')


if __name__ == '__main__':
    unittest.main()

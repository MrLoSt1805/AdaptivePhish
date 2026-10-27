"""Offline tests for the explicit production-ingestion command gate."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from adaptivephish.data import output_writer
from adaptivephish.data.source_config import load_source_config


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'ingest_phreshphish_command', ROOT / 'scripts' / 'ingest_phreshphish.py'
)
command = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(command)
PRODUCTION_OUTPUT_PATH = command.OUTPUT_PATH


def completed_manifest():
    config = load_source_config()
    return {
        'completion_status': 'complete',
        'source': {
            'dataset': config.dataset,
            'revision': config.revision,
            'split': config.split,
            'shard': config.shard,
        },
        'selected_columns': list(config.selected_columns),
        'row_count': 5_780,
        'label_counts': {'benign': 3_000, 'phish': 2_780},
        'date_range': {'earliest': '2024-01-01', 'latest': '2025-09-30'},
        'duplicate_counts': {
            'sha256': 2, 'url': 3, 'row': 1, 'conflicting_sha256': 0,
        },
        'output_sha256': 'f' * 64,
    }


class IngestionCommandTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.output = Path(self.temporary_directory.name) / 'approved.parquet'
        self.path_patch = patch.object(command, 'OUTPUT_PATH', self.output)
        self.path_patch.start()
        self.addCleanup(self.path_patch.stop)

    def invoke(self, arguments):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exit_code = command.main(arguments)
        return exit_code, json.loads(output.getvalue())

    def test_production_output_path_is_fixed(self):
        self.assertEqual(
            PRODUCTION_OUTPUT_PATH,
            output_writer.WORKSPACE_ROOT / 'data' / 'raw' / 'phreshphish'
            / 'train-001-url-metadata.parquet',
        )

    def test_missing_live_refuses_before_ingestion(self):
        with patch.object(command.ingestion, 'ingest_approved_shard') as ingest:
            exit_code, result = self.invoke([])
        self.assertEqual(exit_code, 2)
        self.assertEqual(result['failure_category'], 'live_flag_required')
        ingest.assert_not_called()

    def test_unexpected_arguments_refuse_before_ingestion(self):
        cases = [['--help'], ['--live', 'extra'], ['--output', 'private']]
        for arguments in cases:
            with self.subTest(arguments=arguments), patch.object(
                    command.ingestion, 'ingest_approved_shard') as ingest:
                exit_code, result = self.invoke(arguments)
                self.assertEqual(exit_code, 2)
                self.assertEqual(result['failure_category'], 'live_flag_required')
                ingest.assert_not_called()

    def test_success_calls_existing_ingestion_and_verifies_output(self):
        document = completed_manifest()
        with patch.object(command.ingestion, 'ingest_approved_shard',
                          return_value=document) as ingest, patch.object(
                              command.output_writer, 'verify_completed_output',
                              return_value=document) as verify:
            exit_code, result = self.invoke(['--live'])
        self.assertEqual(exit_code, 0)
        self.assertTrue(result['success'])
        self.assertEqual(result['output_row_count'], 5_780)
        self.assertEqual(result['output_verification_status'], 'verified')
        ingest.assert_called_once_with(self.output)
        verify.assert_called_once_with(self.output)
        serialized = json.dumps(result)
        self.assertNotIn('output_sha256', serialized)

    def test_failures_are_sanitized(self):
        with patch.object(
                command.ingestion, 'ingest_approved_shard',
                side_effect=RuntimeError('SIGNED_URL_PRIVATE_IDENTIFIER')):
            exit_code, result = self.invoke(['--live'])
        self.assertEqual(exit_code, 1)
        self.assertEqual(result['failure_category'], 'ingestion_failed')
        serialized = json.dumps(result)
        self.assertNotIn('SIGNED_URL', serialized)
        self.assertNotIn('PRIVATE_IDENTIFIER', serialized)

    def test_existing_completed_output_is_not_overwritten(self):
        self.output.write_bytes(b'PARQUET')
        output_writer.manifest_path(self.output).write_text('{}', encoding='utf-8')
        document = completed_manifest()
        with patch.object(command.output_writer, 'verify_completed_output',
                          return_value=document), patch.object(
                              command.ingestion, 'ingest_approved_shard') as ingest:
            exit_code, result = self.invoke(['--live'])
        self.assertEqual(exit_code, 1)
        self.assertEqual(result['failure_category'], 'existing_complete_output')
        self.assertEqual(result['output_verification_status'], 'verified_existing')
        ingest.assert_not_called()

    def test_incomplete_prior_output_is_not_replaced(self):
        self.output.write_bytes(b'INCOMPLETE_PRIVATE_BYTES')
        with patch.object(
                command.output_writer, 'verify_completed_output',
                side_effect=output_writer.OutputWriterError('incomplete_output')), \
                patch.object(command.ingestion, 'ingest_approved_shard') as ingest:
            exit_code, result = self.invoke(['--live'])
        self.assertEqual(exit_code, 1)
        self.assertEqual(result['failure_category'], 'existing_incomplete_output')
        self.assertEqual(result['output_verification_status'],
                         'incomplete_existing')
        ingest.assert_not_called()
        self.assertEqual(self.output.read_bytes(), b'INCOMPLETE_PRIVATE_BYTES')
        self.assertNotIn('PRIVATE_BYTES', json.dumps(result))


if __name__ == '__main__':
    unittest.main()

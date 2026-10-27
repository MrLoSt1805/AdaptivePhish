"""Live expansion commands refuse network access without --live."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]


def load(name):
    path = ROOT / 'scripts' / f'{name}.py'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


discovery = load('discover_phreshphish_inventory')
preflight = load('preflight_dataset_shard')
ingestion = load('ingest_dataset_shard')
batch = load('batch_dataset_shards')


class ExpansionCommandTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('network forbidden')
        )
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_discovery_requires_live(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = discovery.main([])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output.getvalue())['failure_category'],
                         'live_flag_required')

    def test_preflight_and_ingestion_require_live_before_reader(self):
        for module, arguments in (
            (preflight, ['--shard', 'train-001', '--session-budget', '2097152']),
            (ingestion, ['--shard', 'train-001', '--approved-estimate', '1',
                         '--session-budget', '2097152']),
        ):
            output = io.StringIO()
            with self.subTest(module=module.__name__), patch.object(
                    module, 'make_reader') as make_reader, contextlib.redirect_stdout(output):
                code = module.main(arguments)
            self.assertEqual(code, 2)
            make_reader.assert_not_called()
            self.assertNotIn('https://', output.getvalue())

    def test_batch_preflight_and_ingestion_require_live(self):
        commands = (
            ['preflight', '--split', 'train', '--max-shards', '1',
             '--per-shard-byte-limit', '1000000',
             '--session-byte-limit', '1000000',
             '--session-request-limit', '10', '--runtime-seconds', '60'],
            ['ingest', '--approval-id', '0' * 64,
             '--session-byte-limit', '1000000',
             '--session-request-limit', '10', '--runtime-seconds', '60'],
        )
        for arguments in commands:
            output = io.StringIO()
            with self.subTest(command=arguments[0]), patch.object(
                    batch, 'preflight_batch') as preflight, patch.object(
                    batch, 'ingest_approved_batch') as ingest, \
                    contextlib.redirect_stdout(output):
                code = batch.main(arguments)
            self.assertEqual(code, 2)
            preflight.assert_not_called()
            ingest.assert_not_called()
            self.assertEqual(json.loads(output.getvalue())['failure_category'],
                             'live_flag_required')

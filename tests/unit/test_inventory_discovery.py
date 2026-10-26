"""Offline tests for revision-pinned Hugging Face inventory discovery."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import unittest
import urllib.error
from unittest.mock import patch


SCRIPT = (Path(__file__).resolve().parents[2] / 'scripts' /
          'discover_phreshphish_inventory.py')
SPEC = importlib.util.spec_from_file_location('inventory_discovery', SCRIPT)
discovery = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(discovery)


def metadata():
    siblings = [
        {'rfilename': f'data/train-{index:03}.parquet',
         'lfs': {'size': 1_000 + index}}
        for index in range(56)
    ]
    siblings.extend(
        {'rfilename': f'data/test-{index:03}.parquet',
         'size': 2_000 + index}
        for index in range(21)
    )
    siblings.append({'rfilename': 'README.md', 'size': 100})
    return {'sha': discovery.REVISION, 'siblings': siblings}


class Response:
    status = 200

    def __init__(self, document):
        self.body = (document if isinstance(document, bytes)
                     else json.dumps(document).encode('utf-8'))

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def read(self, count):
        return self.body[:count]


def opener_for(document):
    def opener(request, timeout):
        if request.full_url != discovery._revision_metadata_url():
            raise AssertionError('unexpected endpoint')
        if request.headers.get('Accept-encoding') != 'identity' or timeout != 30:
            raise AssertionError('unsafe request settings')
        return Response(document)
    return opener


class InventoryDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('network forbidden')
        )
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_success_uses_revision_metadata_and_validates_splits(self):
        result = discovery.discover(opener_for(metadata()))
        self.assertTrue(result['success'])
        self.assertEqual(result['shard_count'], 77)
        self.assertEqual(result['split_counts'], {'train': 56, 'test': 21})
        self.assertEqual(result['revision'], discovery.REVISION)
        self.assertEqual(len({item['path'] for item in result['shards']}), 77)
        self.assertTrue(all(item['size'] >= 12 for item in result['shards']))

    def assert_category(self, document, category):
        with self.assertRaises(discovery.DiscoveryError) as error:
            discovery.discover(opener_for(document))
        self.assertEqual(error.exception.category, category)

    def test_revision_and_response_schema_fail_closed(self):
        changed = metadata()
        changed['sha'] = '0' * 40
        self.assert_category(changed, 'revision_mismatch')
        self.assert_category({'sha': discovery.REVISION},
                             'response_schema_mismatch')
        self.assert_category(b'not-json', 'response_schema_mismatch')

    def test_count_duplicate_split_and_size_fail_closed(self):
        shortened = metadata()
        shortened['siblings'].pop(0)
        self.assert_category(shortened, 'shard_count_mismatch')

        duplicate = metadata()
        duplicate['siblings'].append(dict(duplicate['siblings'][0]))
        self.assert_category(duplicate, 'duplicate_shard_path')

        wrong_split = metadata()
        wrong_split['siblings'][0]['rfilename'] = 'data/validation-000.parquet'
        self.assert_category(wrong_split, 'publisher_split_mismatch')

        invalid_size = metadata()
        invalid_size['siblings'][0]['lfs']['size'] = 0
        self.assert_category(invalid_size, 'response_schema_mismatch')

    def test_http_timeout_and_transport_categories(self):
        failures = [
            (urllib.error.HTTPError('private', 404, 'private', {}, None),
             'revision_not_found', 404),
            (urllib.error.HTTPError('private', 503, 'private', {}, None),
             'http_status_error', 503),
            (socket.timeout('private'), 'timeout', None),
            (urllib.error.URLError('private'), 'transport_error', None),
        ]
        for raised, category, status in failures:
            with self.subTest(category=category):
                def failing(*unused, exception=raised, **unused_keywords):
                    raise exception
                with self.assertRaises(discovery.DiscoveryError) as error:
                    discovery.discover(failing)
                self.assertEqual(error.exception.category, category)
                self.assertEqual(error.exception.http_status, status)

    def test_diagnostic_output_is_fixed_and_sanitized(self):
        failure = discovery.DiscoveryError(
            'http_status_error', 'fetch_revision_metadata', 503
        )
        output = io.StringIO()
        with patch.object(discovery, 'discover', side_effect=failure), \
                contextlib.redirect_stdout(output):
            code = discovery.main(['--live', '--diagnostic'])
        result = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertEqual(result['failure_category'], 'inventory_discovery_failed')
        self.assertEqual(result['diagnostic'], {
            'category': 'http_status_error',
            'stage': 'fetch_revision_metadata',
            'http_status': 503,
        })
        rendered = output.getvalue().lower()
        self.assertNotIn('https://', rendered)
        self.assertNotIn('private', rendered)

    def test_no_live_diagnostic_never_calls_discovery(self):
        output = io.StringIO()
        with patch.object(discovery, 'discover') as discover, \
                contextlib.redirect_stdout(output):
            code = discovery.main(['--diagnostic'])
        discover.assert_not_called()
        result = json.loads(output.getvalue())
        self.assertEqual(code, 2)
        self.assertEqual(result['diagnostic']['category'], 'live_flag_required')

    def test_persist_is_explicit_and_uses_validated_discovery(self):
        result = {
            'success': True, 'failure_category': None,
            'dataset': discovery.DATASET, 'revision': discovery.REVISION,
            'shard_count': 77, 'split_counts': {'train': 56, 'test': 21},
            'shards': [],
        }
        persisted = {'inventory_status': 'metadata_complete'}
        output = io.StringIO()
        with patch.object(discovery, 'discover', return_value=result), \
                patch.object(discovery, 'merge_discovered_inventory',
                             return_value=persisted) as merge, \
                contextlib.redirect_stdout(output):
            code = discovery.main(['--live', '--persist'])
        self.assertEqual(code, 0)
        merge.assert_called_once_with(result)
        rendered = json.loads(output.getvalue())
        self.assertTrue(rendered['persisted'])
        self.assertEqual(rendered['inventory_status'], 'metadata_complete')


if __name__ == '__main__':
    unittest.main()

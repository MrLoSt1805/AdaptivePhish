"""Offline tests for the immutable PhreshPhish source configuration."""

import socket
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from adaptivephish.data import source_config


VALID_TOML = '''\
dataset = "phreshphish/phreshphish"
revision = "eca603e0ecd4f698f8d4d226c088659e72681239"
split = "train"
shard = "data/train-001.parquet"
expected_file_size = 322018201
expected_row_groups = 1
expected_rows = 5780
selected_columns = ["sha256", "url", "label", "date"]
application_body_budget = 2097152
maximum_http_requests = 32
'''


class SourceConfigTests(unittest.TestCase):
    def setUp(self):
        self.network_block = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('Network forbidden')
        )
        self.network_block.start()
        self.addCleanup(self.network_block.stop)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)

    def write(self, text):
        path = Path(self.temporary_directory.name) / 'source.toml'
        path.write_text(text, encoding='utf-8')
        return path

    def test_repository_configuration_is_valid_and_immutable(self):
        config = source_config.load_source_config()
        self.assertEqual(config.dataset, 'phreshphish/phreshphish')
        self.assertEqual(config.selected_columns, ('sha256', 'url', 'label', 'date'))
        self.assertEqual(config.application_body_budget, 2_097_152)
        self.assertEqual(config.maximum_http_requests, 32)
        with self.assertRaises((AttributeError, TypeError)):
            config.revision = 'main'

    def assert_rejected(self, old, new, category):
        path = self.write(VALID_TOML.replace(old, new))
        with self.assertRaises(source_config.SourceConfigError) as error:
            source_config.load_source_config(path)
        self.assertEqual(error.exception.category, category)
        self.assertNotIn(new, str(error.exception))

    def test_changed_identity_revision_and_path_are_rejected(self):
        cases = [
            ('phreshphish/phreshphish', 'another/dataset', 'invalid_dataset'),
            ('eca603e0ecd4f698f8d4d226c088659e72681239', 'main', 'invalid_revision'),
            ('data/train-001.parquet', 'data/train-002.parquet', 'invalid_shard'),
            ('split = "train"', 'split = "test"', 'invalid_split'),
        ]
        for old, new, category in cases:
            with self.subTest(category=category):
                self.assert_rejected(old, new, category)

    def test_changed_columns_and_sizes_are_rejected(self):
        cases = [
            ('["sha256", "url", "label", "date"]',
             '["sha256", "url", "label", "html"]',
             'invalid_selected_columns'),
            ('322018201', '322018202', 'invalid_expected_file_size'),
            ('expected_row_groups = 1', 'expected_row_groups = 2',
             'invalid_expected_row_groups'),
            ('expected_rows = 5780', 'expected_rows = 5781',
             'invalid_expected_rows'),
        ]
        for old, new, category in cases:
            with self.subTest(category=category):
                self.assert_rejected(old, new, category)

    def test_changed_limits_are_rejected(self):
        cases = [
            ('2097152', '2097153', 'invalid_application_body_budget'),
            ('maximum_http_requests = 32', 'maximum_http_requests = 33',
             'invalid_maximum_http_requests'),
        ]
        for old, new, category in cases:
            with self.subTest(category=category):
                self.assert_rejected(old, new, category)

    def test_malformed_missing_extra_and_wrong_type_are_sanitized(self):
        cases = [
            ('dataset = [', 'malformed_configuration'),
            (VALID_TOML.replace('expected_rows = 5780\n', ''),
             'invalid_configuration_shape'),
            (VALID_TOML + 'source_url = "https://private.invalid/token"\n',
             'invalid_configuration_shape'),
            (VALID_TOML.replace('maximum_http_requests = 32',
                                'maximum_http_requests = "32"'),
             'invalid_maximum_http_requests'),
        ]
        for text, category in cases:
            with self.subTest(category=category):
                with self.assertRaises(source_config.SourceConfigError) as error:
                    source_config.load_source_config(self.write(text))
                self.assertEqual(error.exception.category, category)
                self.assertNotIn('private.invalid', str(error.exception))

    def test_missing_file_is_sanitized(self):
        missing = Path(self.temporary_directory.name) / 'missing.toml'
        with self.assertRaises(source_config.SourceConfigError) as error:
            source_config.load_source_config(missing)
        self.assertEqual(error.exception.category, 'malformed_configuration')
        self.assertNotIn(str(missing), str(error.exception))


if __name__ == '__main__':
    unittest.main()

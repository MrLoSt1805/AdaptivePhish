"""Aggregate cross-shard duplicate policy tests."""

import datetime
import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'expanded_verify', ROOT / 'scripts' / 'verify_expanded_dataset.py'
)
verify = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify)


class ExpandedVerificationTests(unittest.TestCase):
    def test_duplicates_across_splits_are_aggregated(self):
        row = {'sha256': 'a' * 64, 'url': 'https://same.invalid',
               'label': 'benign', 'date': datetime.date(2025, 1, 1)}
        counts, duplicates = verify._aggregate_records([
            ('train', row), ('test', dict(row)),
        ])
        self.assertEqual(counts[('train', 'benign')], 1)
        self.assertEqual(counts[('test', 'benign')], 1)
        self.assertEqual(duplicates['sha256'], 1)
        self.assertEqual(duplicates['url'], 1)
        self.assertEqual(duplicates['row'], 1)
        self.assertEqual(duplicates['conflicting_sha256'], 0)

    def test_conflicting_identifier_is_detected_without_exposing_value(self):
        first = {'sha256': 'b' * 64, 'url': 'https://one.invalid',
                 'label': 'benign', 'date': datetime.date(2025, 1, 1)}
        second = {'sha256': 'b' * 64, 'url': 'https://two.invalid',
                  'label': 'phish', 'date': datetime.date(2025, 1, 2)}
        _, duplicates = verify._aggregate_records([
            ('train', first), ('test', second),
        ])
        self.assertEqual(duplicates['conflicting_sha256'], 1)

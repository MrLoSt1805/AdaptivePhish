"""Offline local registry tests."""

import tempfile
from pathlib import Path
import unittest

from adaptivephish.registry.local_registry import (
    RegistryError, initialize, resolve_champion,
)


METRICS = {'f1': .8, 'recall': .8, 'roc_auc': .9, 'false_positive_rate': .1, 'prediction_latency_seconds_per_row': .001}


class LocalRegistryTests(unittest.TestCase):
    def test_promotion_and_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / 'candidate.joblib'
            candidate.write_bytes(b'trusted synthetic artifact')
            registry = Path(directory) / 'registry'
            document = initialize(candidate, registry, 'v1', METRICS, {})
            artifact, resolved = resolve_champion(registry)
            self.assertEqual(artifact.read_bytes(), candidate.read_bytes())
            self.assertEqual(resolved, document)

    def test_tampering_and_version_overwrite_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / 'candidate.joblib'
            candidate.write_bytes(b'one')
            registry = Path(directory) / 'registry'
            initialize(candidate, registry, 'v1', METRICS, {})
            with self.assertRaises(RegistryError):
                initialize(candidate, registry, 'v1', METRICS, {})
            (registry / 'v1.joblib').write_bytes(b'tampered')
            with self.assertRaises(RegistryError):
                resolve_champion(registry)


if __name__ == '__main__':
    unittest.main()

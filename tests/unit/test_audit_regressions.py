"""Regression coverage for independently reproduced audit failures."""

import datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from adaptivephish.api.app import app
from adaptivephish.monitoring.drift import population_stability_index
from adaptivephish.pipelines.research import research_partitions
from adaptivephish.registry.local_registry import (
    RegistryError, checksum, initialize, promote, resolve_champion, rollback,
)
from adaptivephish.retraining.governance import GovernanceError, promotion_decision


METRICS = {'f1': .8, 'recall': .8, 'roc_auc': .9, 'false_positive_rate': .1,
           'prediction_latency_seconds_per_row': .001}


class AuditRegressions(unittest.TestCase):
    def test_nonfinite_and_out_of_range_metrics_fail_closed(self):
        for key in METRICS:
            for invalid in (float('nan'), float('inf'), -1, None, True, '0.9'):
                with self.subTest(key=key, invalid=invalid), self.assertRaises(GovernanceError):
                    promotion_decision(METRICS, {**METRICS, key: invalid})
        with self.assertRaises(GovernanceError):
            promotion_decision(METRICS, {**METRICS, 'f1': 1.1})

    def test_missing_metric_with_extra_keys_is_rejected(self):
        candidate = {**METRICS, 'extra': 1}
        del candidate['roc_auc']
        with self.assertRaises(GovernanceError):
            promotion_decision(METRICS, candidate)

    def test_recall_and_latency_are_gates(self):
        for values in ({'recall': .7}, {'prediction_latency_seconds_per_row': .1}):
            self.assertFalse(promotion_decision(METRICS, {**METRICS, **values})['approved'])

    def test_constant_and_binary_feature_shifts(self):
        self.assertEqual(population_stability_index([0] * 100, [0] * 100), 0)
        for shift in (1, -1):
            self.assertGreater(population_stability_index([0] * 100, [shift] * 100), .25)
        self.assertGreater(population_stability_index([0, 1] * 50, [1] * 100), .25)

    def test_validation_never_echoes_sensitive_payload(self):
        client = TestClient(app)
        for body in ({'url': {'password': 'SENSITIVE'}},
                     {'url': 'a', 'SENSITIVE': 'secret'}, {'url': 'SENSITIVE' * 3000}):
            response = client.post('/predict', json=body)
            self.assertEqual(response.status_code, 422)
            self.assertEqual(response.json(), {'detail': 'invalid_prediction_input'})
        response = client.post('/predict', content='{"SENSITIVE":', headers={'Content-Type': 'application/json'})
        self.assertNotIn('SENSITIVE', response.text)

    def test_readiness_missing_model(self):
        from adaptivephish.models.artifact import ModelArtifactError
        with patch('adaptivephish.api.app.load_trusted_model', side_effect=ModelArtifactError('missing')):
            self.assertEqual(TestClient(app).get('/ready').status_code, 503)
            self.assertEqual(TestClient(app).get('/model/info').status_code, 503)

    def test_temporal_partitions_purge_repeated_urls_and_order_dates(self):
        rows = [{'url': str(i), 'label': 'benign' if i % 2 else 'phish',
                 'date': datetime.date(2025, 1, 1) + datetime.timedelta(days=i)} for i in range(100)]
        rows.append({**rows[0], 'date': rows[-1]['date']})
        groups, _, purged = research_partitions(rows)
        self.assertEqual(purged, 1)
        for first, second in zip(groups, groups[1:]):
            self.assertLess(max(r['date'] for r in first), min(r['date'] for r in second))
            self.assertFalse({r['url'] for r in first} & {r['url'] for r in second})

    def test_registry_requires_governance_and_supports_verified_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / 'model.joblib'
            artifact.write_bytes(b'trusted fixture')
            registry = root / 'registry'
            with self.assertRaises(RegistryError):
                promote(artifact, registry, 'bypass', METRICS)
            initialize(artifact, registry, 'v1', METRICS, {})
            evidence = {'dataset_fingerprint': 'data', 'evaluation_fingerprint': 'test',
                        'mlflow_run_id': 'fixture', 'fit_evaluation_overlap': 0,
                        'champion_sha256': checksum(artifact), 'candidate_sha256': checksum(artifact)}
            for candidate, proof in (({**METRICS, 'f1': .1}, evidence),
                                     (METRICS, {**evidence, 'fit_evaluation_overlap': 1}),
                                     ({**METRICS, 'roc_auc': float('nan')}, evidence)):
                with self.assertRaises(RegistryError):
                    promote(artifact, registry, 'v2', candidate, champion_metrics=METRICS,
                            evidence=proof, expected_champion='v1')
                self.assertEqual(resolve_champion(registry)[1]['version'], 'v1')
            promote(artifact, registry, 'v2', METRICS, champion_metrics=METRICS,
                    evidence=evidence, expected_champion='v1')
            self.assertEqual(rollback(registry)['version'], 'v1')
            self.assertTrue((registry / 'v2.joblib').exists())

    def test_atomic_pointer_failure_preserves_champion(self):
        with tempfile.TemporaryDirectory() as directory:
            from adaptivephish.registry import local_registry as registry_module
            root = Path(directory)
            artifact = root / 'fixture'
            artifact.write_bytes(b'fixture')
            initialize(artifact, root / 'registry', 'v1', METRICS, {})
            original = registry_module.atomic_json
            def fail_pointer(path, document):
                if path.name == 'champion.json':
                    raise OSError('simulated interruption')
                return original(path, document)
            evidence = {'dataset_fingerprint': 'data', 'evaluation_fingerprint': 'test',
                        'mlflow_run_id': 'fixture', 'fit_evaluation_overlap': 0,
                        'champion_sha256': checksum(artifact), 'candidate_sha256': checksum(artifact)}
            with patch.object(registry_module, 'atomic_json', side_effect=fail_pointer):
                with self.assertRaises(OSError):
                    promote(artifact, root / 'registry', 'v2', METRICS,
                            champion_metrics=METRICS, evidence=evidence, expected_champion='v1')
            self.assertEqual(resolve_champion(root / 'registry')[1]['version'], 'v1')

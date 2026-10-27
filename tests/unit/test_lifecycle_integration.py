"""Actual model fitting, tracking, governance, publication and rollback integration."""
from pathlib import Path
import tempfile
import unittest

from sklearn.dummy import DummyClassifier

from adaptivephish.evaluation.metrics import evaluate_classifier
from adaptivephish.registry.local_registry import initialize, resolve_champion
from adaptivephish.retraining.lifecycle import labels, matrix, run_lifecycle, save_bundle
from adaptivephish.retraining.simulation import run_simulation, synthetic_rows


class LifecycleIntegrationTests(unittest.TestCase):
    def test_real_synthetic_lifecycle_promotes_then_rolls_back(self):
        result = run_simulation()
        self.assertTrue(result['promoted'])
        self.assertEqual(result['rollback_version'], 'synthetic-v1')
        self.assertTrue(result['mlflow_run_id'])
        self.assertEqual(result['audit_events'], 1)

    def test_no_trigger_insufficient_labels_and_leakage(self):
        initial = synthetic_rows(100, 1)
        update = synthetic_rows(20, 2, 1000, True)
        gate = synthetic_rows(100, 3, 2000, True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = DummyClassifier(strategy='constant', constant=0)
            model.fit(matrix(initial), labels(initial))
            save_bundle(model, initial, root / 'initial.joblib', 'v1')
            initialize(root / 'initial.joblib', root / 'registry', 'v1',
                       evaluate_classifier(model, matrix(gate), labels(gate)), {})
            event = run_lifecycle(root / 'registry', initial, update, gate, root / 'tracking')
            self.assertFalse(event['trigger']['triggered'])
            self.assertEqual(event['trigger']['reason'], 'insufficient_labels')
            self.assertEqual(resolve_champion(root / 'registry')[1]['version'], 'v1')
            with self.assertRaisesRegex(ValueError, 'leakage'):
                run_lifecycle(root / 'registry', initial, update, initial, root / 'tracking')

    def test_single_class_gate_rejects_undefined_auc(self):
        initial = synthetic_rows(100, 1)
        update = synthetic_rows(100, 2, 1000, True)
        gate = synthetic_rows(100, 3, 2000, True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = DummyClassifier(strategy='constant', constant=0)
            model.fit(matrix(initial), labels(initial))
            save_bundle(model, initial, root / 'initial.joblib', 'v1')
            initialize(root / 'initial.joblib', root / 'registry', 'v1',
                       evaluate_classifier(model, matrix(gate), labels(gate)), {})
            event = run_lifecycle(root / 'registry', initial, update,
                                  [r for r in gate if r['label'] == 'phish'],
                                  root / 'tracking', minimum_labeled_rows=50)
            self.assertFalse(event['promoted'])
            self.assertEqual(event['decision']['reason'], 'undefined_or_invalid_metrics')

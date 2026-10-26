"""Offline safety tests for artifacts, reports, and optional ML dependencies."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from adaptivephish.models import artifact, training
from adaptivephish.evaluation.metrics import evaluate_classifier


HAS_ML_STACK = all(importlib.util.find_spec(name) is not None for name in (
    'numpy', 'sklearn', 'joblib', 'xgboost', 'lightgbm'
))


class ModelPipelineTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('network forbidden')
        )
        self.network.start()
        self.addCleanup(self.network.stop)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.artifact_patch = patch.object(
            artifact, 'ARTIFACT_PATH',
            Path(self.temporary_directory.name) / 'adaptivephish_model.joblib'
        )
        self.artifact_patch.start()
        self.addCleanup(self.artifact_patch.stop)

    def test_missing_and_unapproved_artifacts_fail_safely(self):
        with self.assertRaises(artifact.ModelArtifactError) as missing:
            artifact.load_trusted_model()
        self.assertEqual(str(missing.exception), 'model_artifact_missing')
        with self.assertRaises(artifact.ModelArtifactError) as unapproved:
            artifact.load_trusted_model(Path('private.joblib'))
        self.assertEqual(str(unapproved.exception), 'unapproved_artifact_path')

    def test_all_four_candidates_are_required(self):
        try:
            candidates = training.build_candidates()
        except training.TrainingDependencyError as error:
            self.assertEqual(str(error), 'ml_dependencies_unavailable')
        else:
            self.assertEqual(tuple(candidates), training.CANDIDATE_ORDER)

    def test_prediction_cli_does_not_echo_url_on_failure(self):
        from scripts import predict_url
        secret = 'https://private-value.invalid/path'
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = predict_url.main([secret])
        result = json.loads(output.getvalue())
        self.assertEqual(code, 1)
        self.assertFalse(result['success'])
        self.assertNotIn(secret, output.getvalue())

    @unittest.skipUnless(HAS_ML_STACK, 'Phase 3 ML dependencies are not installed')
    def test_all_candidates_fit_and_evaluate_on_synthetic_data(self):
        features = [[index, index % 3, index / 10] for index in range(40)]
        labels = [index % 2 for index in range(40)]
        for name, model in training.build_candidates().items():
            with self.subTest(model=name):
                model.fit(features, labels)
                metrics = evaluate_classifier(model, features, labels)
                self.assertEqual(metrics['prediction_count'], 40)
                self.assertEqual(
                    set(metrics['confusion_matrix']), {'tn', 'fp', 'fn', 'tp'}
                )

    @unittest.skipUnless(HAS_ML_STACK, 'Phase 3 ML dependencies are not installed')
    def test_artifact_round_trip_and_corrupt_file(self):
        model = training.build_candidates()['logistic_regression']
        from adaptivephish.features import feature_matrix
        urls = ['https://one.invalid', 'http://two.invalid/login'] * 10
        labels = [0, 1] * 10
        model.fit(feature_matrix(urls), labels)
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'adaptivephish_model.joblib'
            with patch.object(artifact, 'ARTIFACT_PATH', target):
                artifact.save_model(model, {'synthetic': True})
                prediction = artifact.predict_url('https://test.invalid/path')
                self.assertIn(prediction['predicted_label'], ('benign', 'phish'))
                self.assertEqual(prediction['feature_schema_version'],
                                 'url-lexical-v1')
                target.write_bytes(b'CORRUPT_PRIVATE_ARTIFACT')
                with self.assertRaises(artifact.ModelArtifactError) as error:
                    artifact.load_trusted_model()
                self.assertEqual(str(error.exception), 'corrupt_model_artifact')

"""Offline API tests; request values are never echoed."""

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from adaptivephish.api.app import app
from adaptivephish.models.artifact import ModelArtifactError


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_health(self):
        self.assertEqual(self.client.get('/health').json(), {'status': 'ok'})

    def test_home_page_contains_working_prediction_interface(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn('AdaptivePhish', response.text)
        self.assertIn("fetch('/predict'", response.text)
        self.assertIn('Check a URL', response.text)
        self.assertEqual(response.headers['x-content-type-options'], 'nosniff')

    def test_prediction_contract_does_not_echo_url(self):
        expected = {'predicted_label': 'phish', 'phishing_probability': .9,
                    'model_version': 'phase3-v1',
                    'feature_schema_version': 'url-lexical-v1'}
        with patch('adaptivephish.api.app.predict_url', return_value=expected):
            response = self.client.post('/predict', json={
                'url': 'https://private.invalid/secret'
            })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        self.assertNotIn('private.invalid', response.text)

    def test_prediction_failure_is_sanitized(self):
        with patch('adaptivephish.api.app.predict_url',
                   side_effect=ModelArtifactError('PRIVATE_EXCEPTION')):
            response = self.client.post('/predict', json={
                'url': 'https://private.invalid/secret'
            })
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {'detail': 'prediction_unavailable'})
        self.assertNotIn('PRIVATE', response.text)

    def test_validation_and_metrics(self):
        self.assertEqual(self.client.post('/predict', json={'url': ''}).status_code, 422)
        response = self.client.get('/metrics')
        self.assertEqual(response.status_code, 200)
        self.assertIn('adaptivephish_predictions_total', response.text)


if __name__ == '__main__':
    unittest.main()

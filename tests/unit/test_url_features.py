"""Offline tests for deterministic URL-only features."""

import socket
import unittest
from unittest.mock import patch

from adaptivephish.features import (
    FEATURE_NAMES, FEATURE_SCHEMA_VERSION, extract_url_features, feature_matrix,
)
from adaptivephish.features.url_features import FeatureExtractionError


class UrlFeatureTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('network forbidden')
        )
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_schema_is_stable_and_numeric(self):
        values = extract_url_features('https://login.example.invalid/a?x=1')
        self.assertEqual(FEATURE_SCHEMA_VERSION, 'url-lexical-v1')
        self.assertEqual(len(values), len(FEATURE_NAMES))
        self.assertEqual(len(FEATURE_NAMES), len(set(FEATURE_NAMES)))
        self.assertTrue(all(isinstance(value, (int, float)) for value in values))

    def test_scheme_ip_and_structure(self):
        secure = dict(zip(FEATURE_NAMES, extract_url_features(
            'https://127.0.0.1/login?account=1&verify=yes'
        )))
        no_scheme = dict(zip(FEATURE_NAMES, extract_url_features(
            'sub.example.invalid/path'
        )))
        self.assertEqual(secure['https_flag'], 1)
        self.assertEqual(secure['ip_literal_flag'], 1)
        self.assertEqual(secure['query_parameter_count'], 2)
        self.assertEqual(no_scheme['hostname_length'], len('sub.example.invalid'))
        self.assertEqual(no_scheme['subdomain_count'], 1)

    def test_empty_malformed_unicode_and_long_are_safe(self):
        cases = ['', 'http://[broken', 'https://例子.invalid/路径', 'a' * 100_000]
        for value in cases:
            with self.subTest(length=len(value)):
                self.assertEqual(len(extract_url_features(value)), len(FEATURE_NAMES))

    def test_matrix_matches_single_extractor_and_type_is_checked(self):
        urls = ['example.invalid', 'https://other.invalid/a']
        self.assertEqual(feature_matrix(urls),
                         [list(extract_url_features(url)) for url in urls])
        with self.assertRaises(FeatureExtractionError):
            extract_url_features(None)

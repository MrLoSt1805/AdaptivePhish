"""Offline split and verified-loader tests."""

import datetime
import socket
import unittest
from unittest.mock import patch

from adaptivephish.pipelines import data_preparation
from adaptivephish.data.shard_inventory import load_inventory


def rows():
    result = []
    for label_index, label in enumerate(('benign', 'phish')):
        for index in range(30):
            url = f'https://{label}-{index}.invalid/path'
            row = {'url': url, 'label': label,
                   'date': datetime.date(2025, 1, 1) + datetime.timedelta(index)}
            result.append(row)
            if index == 0:
                result.append(dict(row))
    return result


class DataPreparationTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('network forbidden')
        )
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_split_is_reproducible_stratified_and_has_no_url_overlap(self):
        first = data_preparation.split_rows(rows())
        second = data_preparation.split_rows(rows())
        self.assertEqual(first, second)
        partitions = (first.train, first.validation, first.test)
        url_sets = [set(partition.urls) for partition in partitions]
        self.assertFalse(url_sets[0] & url_sets[1])
        self.assertFalse(url_sets[0] & url_sets[2])
        self.assertFalse(url_sets[1] & url_sets[2])
        for partition in partitions:
            self.assertEqual(set(partition.labels), {0, 1})
            self.assertEqual(len(partition.urls), len(partition.dates))

    def test_conflicting_url_labels_fail_closed(self):
        conflict = rows() + [{
            'url': 'https://benign-0.invalid/path', 'label': 'phish',
            'date': datetime.date(2025, 2, 1),
        }]
        with self.assertRaises(data_preparation.DataPreparationError) as error:
            data_preparation.split_rows(conflict)
        self.assertEqual(str(error.exception), 'conflicting_url_labels')

    def test_only_url_is_predictive_input(self):
        prepared = data_preparation.split_rows(rows())
        for partition in (prepared.train, prepared.validation, prepared.test):
            self.assertFalse(hasattr(partition, 'sha256'))
            self.assertFalse(hasattr(partition, 'features'))
            self.assertTrue(all(type(label) is int for label in partition.labels))

    def test_partial_inventory_cannot_replace_phase3_dataset(self):
        with self.assertRaises(data_preparation.DataPreparationError) as error:
            data_preparation.load_verified_expanded_rows(load_inventory())
        self.assertEqual(str(error.exception), 'expanded_inventory_incomplete')

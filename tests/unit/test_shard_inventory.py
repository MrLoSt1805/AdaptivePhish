"""Offline strict inventory tests."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from adaptivephish.data import shard_inventory


class ShardInventoryTests(unittest.TestCase):
    def partial_inventory(self):
        current = json.loads(
            shard_inventory.INVENTORY_PATH.read_text(encoding='utf-8')
        )
        current['inventory_status'] = 'partial_verified'
        current['provenance']['generated_by_live_inventory'] = False
        current['shards'] = [
            item for item in current['shards']
            if item['path'] == 'data/train-001.parquet'
        ]
        return current

    def discovery(self, train_001_size=322_018_201):
        current = json.loads(
            shard_inventory.INVENTORY_PATH.read_text(encoding='utf-8')
        )
        sizes = {item['path']: item['expected_file_size']
                 for item in current['shards']}
        shards = [
            {'path': f'data/train-{index:03}.parquet', 'split': 'train',
             'size': (train_001_size if index == 1
                      else sizes[f'data/train-{index:03}.parquet'])}
            for index in range(56)
        ]
        shards.extend(
            {'path': f'data/test-{index:03}.parquet', 'split': 'test',
             'size': sizes[f'data/test-{index:03}.parquet']}
            for index in range(21)
        )
        return {
            'success': True,
            'dataset': shard_inventory.DATASET,
            'revision': shard_inventory.REVISION,
            'shard_count': 77,
            'split_counts': {'train': 56, 'test': 21},
            'shards': shards,
        }

    def test_repository_inventory_is_valid(self):
        inventory = shard_inventory.load_inventory()
        self.assertEqual(inventory.status, 'metadata_complete')
        self.assertEqual(len(inventory.shards), 77)
        self.assertEqual(inventory.get('train-000').expected_rows, 1_000)
        self.assertEqual(
            inventory.get('train-000').approved_estimated_body_bytes, 70_613
        )
        self.assertEqual(inventory.get('train-001').expected_rows, 5_780)

    def test_changed_revision_unknown_path_and_false_complete_fail(self):
        original = json.loads(shard_inventory.INVENTORY_PATH.read_text('utf-8'))
        cases = []
        changed = json.loads(json.dumps(original))
        changed['provenance']['dataset_revision'] = 'main'
        cases.append(changed)
        changed = json.loads(json.dumps(original))
        changed['shards'][0]['path'] = '../private.parquet'
        cases.append(changed)
        changed = json.loads(json.dumps(original))
        changed['inventory_status'] = 'complete_verified'
        cases.append(changed)
        with tempfile.TemporaryDirectory() as directory:
            for index, document in enumerate(cases):
                path = Path(directory) / f'{index}.json'
                path.write_text(json.dumps(document), encoding='utf-8')
                with self.subTest(case=index), self.assertRaises(
                        shard_inventory.InventoryError):
                    shard_inventory.load_inventory(path)

    def test_persistence_preserves_verified_metadata_and_is_idempotent(self):
        original = json.dumps(self.partial_inventory(), indent=2) + '\n'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'inventory.json'
            path.write_text(original, encoding='utf-8')
            document = shard_inventory.merge_discovered_inventory(
                self.discovery(), path
            )
            first_bytes = path.read_bytes()
            inventory = shard_inventory.load_inventory(path)
            self.assertEqual(document['inventory_status'], 'metadata_complete')
            self.assertEqual(len(inventory.shards), 77)
            verified = inventory.get('train-001')
            self.assertEqual(verified.expected_rows, 5_780)
            self.assertEqual(verified.expected_row_groups, 1)
            self.assertEqual(verified.application_body_budget, 2_097_152)
            self.assertEqual(verified.verification_status, 'footer_verified')
            discovered = inventory.get('train-000')
            self.assertIsNone(discovered.expected_rows)
            self.assertIsNone(discovered.expected_row_groups)
            self.assertIsNone(discovered.application_body_budget)
            self.assertIsNone(discovered.approved_estimated_body_bytes)
            self.assertEqual(discovered.verification_status,
                             'metadata_discovered')

            shard_inventory.merge_discovered_inventory(self.discovery(), path)
            self.assertEqual(path.read_bytes(), first_bytes)

    def test_revision_and_size_conflicts_do_not_replace_inventory(self):
        original = (json.dumps(self.partial_inventory(), indent=2) + '\n').encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'inventory.json'
            path.write_bytes(original)
            wrong_revision = self.discovery()
            wrong_revision['revision'] = '0' * 40
            with self.assertRaises(shard_inventory.InventoryError):
                shard_inventory.merge_discovered_inventory(wrong_revision, path)
            self.assertEqual(path.read_bytes(), original)

            with self.assertRaises(shard_inventory.InventoryError) as conflict:
                shard_inventory.merge_discovered_inventory(
                    self.discovery(train_001_size=322_018_202), path
                )
            self.assertEqual(str(conflict.exception), 'inventory_merge_conflict')
            self.assertEqual(path.read_bytes(), original)

    def test_interrupted_atomic_replace_preserves_previous_inventory(self):
        original = (json.dumps(self.partial_inventory(), indent=2) + '\n').encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'inventory.json'
            path.write_bytes(original)
            replace = Mock(side_effect=OSError('PRIVATE_PATH'))
            with self.assertRaises(shard_inventory.InventoryError) as error:
                shard_inventory.merge_discovered_inventory(
                    self.discovery(), path, replace=replace
                )
            self.assertEqual(str(error.exception), 'inventory_persistence_failed')
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(
                list(Path(directory).glob('.phreshphish-inventory-*')), []
            )

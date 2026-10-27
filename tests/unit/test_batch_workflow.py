"""Offline tests for bounded batch planning, approval, and resumption."""

import contextlib
import datetime
import io
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from adaptivephish.data import batch_workflow, multi_shard, output_writer
from adaptivephish.data.remote_parquet import RangeFile
from adaptivephish.data.shard_inventory import (
    DATASET, REVISION, SELECTED_COLUMNS, ShardInventory, ShardSpec,
)


def parquet_bytes(rows=8):
    table = pa.table({
        'sha256': [f'{index:064x}' for index in range(rows)],
        'url': [f'https://batch-{index}.invalid/path' for index in range(rows)],
        'label': ['benign', 'phish'] * (rows // 2),
        'target': [None] * rows,
        'date': [datetime.date(2025, 1, 1) + datetime.timedelta(index)
                 for index in range(rows)],
        'lang': ['en'] * rows, 'lang_score': [0.9] * rows,
        'html': [('PRIVATE_HTML_' + str(index)) * 10_000 for index in range(rows)],
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=rows, compression='NONE')
    return sink.getvalue().to_pybytes(), table


class Response:
    status = 206

    def __init__(self, data, start, end, total):
        self.body = io.BytesIO(data)
        self.headers = [('Content-Range', f'bytes {start}-{end-1}/{total}'),
                        ('Content-Length', str(end-start))]

    def getheaders(self):
        return self.headers

    def read(self, size):
        return self.body.read(size)


class Transport:
    def __init__(self, data):
        self.data, self.requests = data, []

    @contextlib.contextmanager
    def __call__(self, url, headers):
        start, inclusive = map(int, headers['Range'][6:].split('-'))
        end = inclusive + 1
        self.requests.append((start, end))
        yield Response(self.data[start:end], start, end, len(self.data))


class BatchWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(
            socket.socket, 'connect', side_effect=AssertionError('network forbidden')
        )
        self.network.start()
        self.addCleanup(self.network.stop)
        self.data, self.table = parquet_bytes()
        self.shard = ShardSpec(
            'train', 'data/train-002.parquet', len(self.data), None, None,
            None, 32, 'metadata_discovered', None,
        )

    def inventory(self, shard=None):
        return ShardInventory('metadata_complete', 666_315, 77,
                              (shard or self.shard,))

    def factory(self, transports):
        def create(shard, budget=None):
            transport = Transport(self.data)
            transports.append(transport)
            return RangeFile(url=shard.url, size=len(self.data),
                             budget=budget or shard.application_body_budget,
                             transport=transport,
                             request_limit=shard.maximum_http_requests)
        return create

    def test_selection_split_keys_and_bounds(self):
        test = ShardSpec('test', 'data/test-000.parquet', 100, None, None,
                         None, 32, 'metadata_discovered', None)
        verified = ShardSpec('train', 'data/train-000.parquet', 100, 1, 1,
                             100, 32, 'footer_verified', 100)
        inventory = ShardInventory('metadata_complete', 1, 3,
                                   (self.shard, test, verified))
        self.assertEqual([s.key for s in batch_workflow.select_shards(
            inventory, 'train', max_shards=3)], ['train-002'])
        self.assertEqual([s.key for s in batch_workflow.select_shards(
            inventory, 'all', ('test-000',), 1)], ['test-000'])
        with self.assertRaises(batch_workflow.BatchError):
            batch_workflow.select_shards(inventory, 'test', ('train-002',), 1)

    def test_metadata_only_preflight_and_limits(self):
        transports = []
        plan = batch_workflow.preflight_batch(
            (self.shard,), 2_000_000, 32, 2_000_000, 60,
            reader_factory=self.factory(transports), clock=lambda: 0,
        )
        entry = plan['shards'][0]
        self.assertEqual((entry['rows'], entry['row_groups']), (8, 1))
        self.assertEqual(plan['metadata_http_requests'], 3)
        self.assertEqual(plan['shards'][0]['approved_http_request_budget'], 32)
        self.assertEqual(len(transports[0].requests), 3)
        with self.assertRaises(batch_workflow.BatchError) as error:
            batch_workflow.preflight_batch(
                (self.shard,), 2_000_000, 2, 2_000_000, 60,
                reader_factory=self.factory([]), clock=lambda: 0,
            )
        self.assertEqual(str(error.exception), 'session_request_limit_exceeded')

    def pending_plan(self, shard, estimated=10_000):
        return {
            'schema_version': 1, 'status': 'pending_review',
            'dataset': DATASET, 'revision': REVISION,
            'selected_columns': list(SELECTED_COLUMNS),
            'limits': {'shard_count': 1, 'per_shard_application_bytes': 20_000,
                       'session_application_bytes': 20_000,
                       'session_http_requests': 32, 'wall_clock_seconds': 60},
            'metadata_application_body_bytes': 100,
            'metadata_http_requests': 3,
            'shards': [{
                'key': shard.key, 'split': shard.split, 'path': shard.path,
                'expected_file_size': shard.expected_file_size,
                'rows': 8, 'row_groups': 1, 'footer_bytes': 100,
                'selected_compressed_bytes': estimated - 112,
                'estimated_application_body_bytes': estimated,
                'planned_requests': 7,
            }],
        }

    def inventory_document(self):
        return {
            'schema_version': 1, 'dataset': DATASET,
            'expected_official_rows': 666315, 'expected_official_shards': 77,
            'inventory_status': 'metadata_complete',
            'selected_columns': list(SELECTED_COLUMNS),
            'provenance': {'dataset_revision': REVISION},
            'shards': [{
                'split': self.shard.split, 'path': self.shard.path,
                'expected_file_size': self.shard.expected_file_size,
                'expected_rows': None, 'expected_row_groups': None,
                'application_body_budget': None, 'maximum_http_requests': 32,
                'verification_status': 'metadata_discovered',
                'approved_estimated_body_bytes': None,
            }],
        }

    def test_approval_is_atomic_binds_estimate_and_adds_exact_ignores(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan_path, inventory_path = root/'plan.json', root/'inventory.json'
            ignore_path = root/'.gitignore'
            plan_path.write_text(json.dumps(self.pending_plan(self.shard)), 'utf-8')
            inventory_path.write_text(json.dumps(self.inventory_document()), 'utf-8')
            ignore_path.write_text('/existing.parquet\n', 'utf-8')
            approved = batch_workflow.approve_plan(
                plan_path, inventory_path, ignore_path
            )
            self.assertEqual(approved['status'], 'approved')
            self.assertEqual(batch_workflow.load_plan(
                plan_path, 'approved')['approval_id'], approved['approval_id'])
            item = json.loads(inventory_path.read_text('utf-8'))['shards'][0]
            self.assertEqual(item['approved_estimated_body_bytes'], 10_000)
            self.assertEqual(item['verification_status'], 'footer_verified')
            rules = ignore_path.read_text('utf-8')
            self.assertIn('/train-002-url-metadata.parquet\n', rules)
            self.assertNotIn('.dvc', rules)

    def test_plan_write_refuses_overwrite_and_cleans_failed_temp(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'plan.json'
            path.write_text('EXISTING', 'utf-8')
            with self.assertRaises(batch_workflow.BatchError):
                batch_workflow.write_pending_plan(self.pending_plan(self.shard), path)
            self.assertEqual(path.read_text('utf-8'), 'EXISTING')

    def test_atomic_replace_failure_preserves_existing_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'document.json'
            path.write_text('{"old": true}\n', 'utf-8')
            with self.assertRaises(batch_workflow.BatchError) as error:
                batch_workflow._atomic_json(
                    path, {'new': True},
                    replace=lambda *unused: (_ for _ in ()).throw(
                        OSError('PRIVATE_PATH')),
                )
            self.assertEqual(str(error.exception), 'atomic_write_failed')
            self.assertEqual(json.loads(path.read_text('utf-8')), {'old': True})
            self.assertEqual(list(Path(directory).glob('.document-*')), [])

    def test_ingestion_revalidates_approval_and_resumes_by_skipping(self):
        transports = []
        plan = batch_workflow.preflight_batch(
            (self.shard,), 2_000_000, 32, 2_000_000, 60,
            reader_factory=self.factory(transports), clock=lambda: 0,
        )
        entry = plan['shards'][0]
        approved_shard = ShardSpec(
            self.shard.split, self.shard.path, len(self.data), entry['rows'],
            entry['row_groups'], entry['estimated_application_body_bytes'], 32,
            'footer_verified', entry['estimated_application_body_bytes'],
        )
        plan['status'] = 'approved'
        plan['approval_id'] = batch_workflow._approval_digest(plan)
        with tempfile.TemporaryDirectory() as directory, patch.object(
                batch_workflow, 'load_inventory',
                return_value=self.inventory(approved_shard)), patch.object(
                multi_shard, 'APPROVED_DIRECTORY', Path(directory)), patch.object(
                output_writer, 'APPROVED_DIRECTORY', Path(directory)), patch.object(
                output_writer, '_require_git_ignored'):
            outcome = batch_workflow.ingest_approved_batch(
                plan, plan['approval_id'], 2_000_000, 32, 60,
                verifier=lambda: {'success': True},
                reader_factory=self.factory(transports), clock=lambda: 0,
            )
            self.assertEqual(outcome['completed_shards'], ['train-002'])
            resumed = batch_workflow.ingest_approved_batch(
                plan, plan['approval_id'], 2_000_000, 32, 60,
                verifier=lambda: {'success': True},
                reader_factory=self.factory(transports), clock=lambda: 0,
            )
            self.assertEqual(resumed['skipped_verified_shards'], ['train-002'])

    def test_changed_approval_and_runtime_fail_closed(self):
        plan = self.pending_plan(self.shard)
        plan['status'] = 'approved'
        plan['approval_id'] = batch_workflow._approval_digest(plan)
        with self.assertRaises(batch_workflow.BatchError):
            batch_workflow.ingest_approved_batch(
                plan, '0' * 64, 20_000, 32, 60,
                verifier=lambda: {'success': True})
        times = iter((0, 2))
        with self.assertRaises(batch_workflow.BatchError) as error:
            batch_workflow.preflight_batch(
                (self.shard,), 20_000, 32, 20_000, 1,
                reader_factory=self.factory([]), clock=lambda: next(times),
            )
        self.assertEqual(str(error.exception), 'runtime_limit_exceeded')

    def test_redirect_request_regression_and_resume_completed_shards(self):
        shards, entries = [], []
        for index in range(2, 5):
            key = f'train-{index:03}'
            shard = ShardSpec(
                'train', f'data/{key}.parquet', 1000 + index, 10, 1,
                1000, 32, 'footer_verified', 1000,
            )
            shards.append(shard)
            entries.append({
                'key': key, 'split': 'train', 'path': shard.path,
                'expected_file_size': shard.expected_file_size,
                'rows': 10, 'row_groups': 1, 'footer_bytes': 100,
                'selected_compressed_bytes': 888,
                'estimated_application_body_bytes': 1000,
                'planned_requests': 7,
            })
        plan = {
            'schema_version': 1, 'status': 'approved', 'dataset': DATASET,
            'revision': REVISION, 'selected_columns': list(SELECTED_COLUMNS),
            'limits': {'shard_count': 3,
                       'per_shard_application_bytes': 50_000_000,
                       'session_application_bytes': 150_000_000,
                       'session_http_requests': 96,
                       'wall_clock_seconds': 300},
            'metadata_application_body_bytes': 6320,
            'metadata_http_requests': 18, 'shards': entries,
        }
        plan['approval_id'] = batch_workflow._approval_digest(plan)
        inventory = ShardInventory('metadata_complete', 666_315, 77,
                                   tuple(shards))

        class Reader:
            body_bytes = request_count = 0
            def close(self):
                pass

        created = []
        def reader_factory(shard):
            reader = Reader()
            created.append(reader)
            return reader

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for key in ('train-002', 'train-003'):
                (root / f'{key}.parquet').write_bytes(b'COMPLETE')
                (root / f'{key}.manifest.json').write_text('{}', 'utf-8')

            def output_path(shard):
                return root / f'{shard.key}.parquet'

            def completed(path):
                key = Path(path).stem
                shard = next(item for item in shards if item.key == key)
                return {'source': shard.source}

            def ingest(reader, *unused, **unused_keywords):
                # Seven logical reads, each with one redirect: 14 HTTP requests.
                reader.request_count = 14
                reader.body_bytes = 1000

            common = (
                patch.object(batch_workflow, 'load_inventory',
                             return_value=inventory),
                patch.object(batch_workflow, 'local_output_path',
                             side_effect=output_path),
                patch.object(output_writer, 'verify_completed_output',
                             side_effect=completed),
                patch.object(output_writer, '_require_git_ignored'),
                patch.object(batch_workflow, 'ingest_reader', side_effect=ingest),
            )
            with common[0], common[1], common[2], common[3], common[4]:
                with self.assertRaises(batch_workflow.BatchError) as error:
                    batch_workflow.ingest_approved_batch(
                        plan, plan['approval_id'], 3000, 21, 300,
                        verifier=lambda: {'success': True},
                        reader_factory=reader_factory, clock=lambda: 0,
                    )
                self.assertEqual(str(error.exception),
                                 'session_request_limit_exceeded')
                self.assertEqual(created, [])

                outcome = batch_workflow.ingest_approved_batch(
                    plan, plan['approval_id'], 1000, 32, 300,
                    verifier=lambda: {'success': True},
                    reader_factory=reader_factory, clock=lambda: 0,
                )
            self.assertEqual(outcome['skipped_verified_shards'],
                             ['train-002', 'train-003'])
            self.assertEqual(outcome['completed_shards'], ['train-004'])
            self.assertEqual(outcome['http_request_count'], 14)


if __name__ == '__main__':
    unittest.main()

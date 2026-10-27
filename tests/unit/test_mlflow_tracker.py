"""Offline local-file MLflow tracking tests."""

import tempfile
from pathlib import Path
import unittest

from adaptivephish.tracking.mlflow_tracker import log_metrics, tracked_run


class MlflowTrackerTests(unittest.TestCase):
    def test_local_run_records_parameters_and_metrics(self):
        from mlflow.tracking import MlflowClient

        with tempfile.TemporaryDirectory() as directory:
            with tracked_run(directory, 'offline-test', 'candidate',
                             {'model': 'synthetic'}, {'scope': 'unit'}) as run_id:
                log_metrics({'f1': .75, 'ignored': 'private'})
            database = (Path(directory).resolve() / 'mlflow.db').as_posix()
            client = MlflowClient(tracking_uri=f'sqlite:///{database}')
            run = client.get_run(run_id)
            self.assertEqual(run.data.params['model'], 'synthetic')
            self.assertEqual(run.data.metrics['f1'], .75)
            self.assertNotIn('ignored', run.data.metrics)
            # Windows keeps SQLite handles open until the SQLAlchemy engine is
            # explicitly disposed; release it before TemporaryDirectory exits.
            client._tracking_client.store.engine.dispose()


if __name__ == '__main__':
    unittest.main()

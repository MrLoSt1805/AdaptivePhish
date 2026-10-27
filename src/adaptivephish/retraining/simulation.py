"""Deterministic synthetic lifecycle demonstration in an isolated registry."""

import datetime
from pathlib import Path
import tempfile

from sklearn.dummy import DummyClassifier

from adaptivephish.evaluation.metrics import evaluate_classifier
from adaptivephish.registry.local_registry import initialize, rollback, resolve_champion
from adaptivephish.retraining.lifecycle import labels, matrix, run_lifecycle, save_bundle


def synthetic_rows(count, day, offset=0, shifted=False):
    result = []
    for index in range(count):
        label = index % 2
        host = ('x' * 80 if shifted else 'site') + str(index + offset)
        url = (f'http://secure-login-{host}.invalid/account/verify/password' if label
               else f'https://{host}.invalid/home')
        result.append({'url': url, 'label': 'phish' if label else 'benign',
                       'date': datetime.date(2025, 1, day)})
    return result


def run_simulation():
    initial = synthetic_rows(200, 1)
    update = synthetic_rows(200, 2, 1000, True)
    gate = synthetic_rows(200, 3, 2000, True)
    with tempfile.TemporaryDirectory(prefix='adaptivephish-demo-') as directory:
        root = Path(directory)
        champion = DummyClassifier(strategy='constant', constant=0)
        champion.fit(matrix(initial), labels(initial))
        artifact = root / 'initial.joblib'
        save_bundle(champion, initial, artifact, 'synthetic-v1')
        initialize(artifact, root / 'registry', 'synthetic-v1',
                   evaluate_classifier(champion, matrix(gate), labels(gate)), {'synthetic': True})
        event = run_lifecycle(root / 'registry', initial, update, gate, root / 'tracking',
                              minimum_labeled_rows=50)
        promoted_version = resolve_champion(root / 'registry')[1]['version']
        rollback_version = rollback(root / 'registry')['version'] if event['promoted'] else None
        return {'synthetic': True, 'isolated_registry': True,
                'drift_status': event['drift']['status'], 'trigger': event['trigger'],
                'decision': event.get('decision'), 'promoted': event['promoted'],
                'promoted_version': promoted_version, 'rollback_version': rollback_version,
                'mlflow_run_id': event.get('mlflow_run_id'),
                'audit_events': len(list((root / 'registry').glob('event-*.json'))),
                'temporary_artifacts_removed_after_demo': True}

"""Connected offline lifecycle with disjoint labeled fit and governance windows."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import uuid

import joblib

from adaptivephish.evaluation.metrics import evaluate_classifier
from adaptivephish.features.url_features import FEATURE_NAMES, FEATURE_SCHEMA_VERSION, feature_matrix
from adaptivephish.models.training import build_candidates
from adaptivephish.monitoring.drift import drift_summary
from adaptivephish.monitoring.metrics import DRIFT, RETRAINING
from adaptivephish.registry.local_registry import atomic_json, checksum, promote, resolve_champion
from adaptivephish.retraining.governance import GovernanceError, promotion_decision, retraining_decision
from adaptivephish.tracking.mlflow_tracker import tracked_run, log_metrics


def fingerprint(rows):
    records = sorted((row['url'], row['label'], row['date'].isoformat()) for row in rows)
    return hashlib.sha256(json.dumps(records, separators=(',', ':')).encode()).hexdigest()


def url_hashes(rows):
    return sorted({hashlib.sha256(row['url'].encode()).hexdigest() for row in rows})


def matrix(rows):
    return feature_matrix([row['url'] for row in rows])


def labels(rows):
    return [int(row['label'] == 'phish') for row in rows]


def save_bundle(model, rows, path, version):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    os.close(descriptor)
    try:
        joblib.dump({'model': model, 'model_version': version,
                     'feature_schema_version': FEATURE_SCHEMA_VERSION,
                     'feature_names': list(FEATURE_NAMES),
                     'metadata': {'fit_url_hashes': url_hashes(rows),
                                  'fit_max_date': max(r['date'] for r in rows).isoformat(),
                                  'dataset_fingerprint': fingerprint(rows)}}, temporary)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def run_lifecycle(registry, reference_rows, update_rows, gate_rows, tracking,
                  minimum_labeled_rows=500):
    """Gate on labels strictly later than fitting; never receives final test rows."""
    registry = Path(registry)
    if len(reference_rows) < 10 or len(update_rows) < 10 or len(gate_rows) < 50:
        raise ValueError('insufficient_monitoring_or_governance_rows')
    if any(r.get('label') not in {'benign', 'phish'} for r in reference_rows + gate_rows):
        raise ValueError('missing_reference_or_governance_labels')
    labeled_update = [r for r in update_rows if r.get('label') in {'benign', 'phish'}]
    artifact, current = resolve_champion(registry)
    bundle = joblib.load(artifact)
    fit_rows = reference_rows + labeled_update
    gate_hashes = set(url_hashes(gate_rows))
    if (gate_hashes & (set(url_hashes(fit_rows)) | set(bundle['metadata']['fit_url_hashes']))
            or max(r['date'] for r in fit_rows) >= min(r['date'] for r in gate_rows)
            or bundle['metadata']['fit_max_date'] >= min(r['date'] for r in gate_rows).isoformat()):
        raise ValueError('lifecycle_evaluation_leakage')
    drift = drift_summary(matrix(reference_rows), matrix(update_rows), FEATURE_NAMES)
    DRIFT.set(drift['maximum_psi'])
    trigger = retraining_decision(drift['status'], len(labeled_update), minimum_labeled_rows)
    event = {'event_id': uuid.uuid4().hex, 'drift': drift, 'trigger': trigger,
             'previous_version': current['version'], 'promoted': False,
             'dataset_fingerprint': fingerprint(fit_rows),
             'evaluation_fingerprint': fingerprint(gate_rows), 'fit_evaluation_overlap': 0}
    if trigger['triggered']:
        candidate = build_candidates()['lightgbm']
        candidate.fit(matrix(fit_rows), labels(fit_rows))
        champion_metrics = evaluate_classifier(bundle['model'], matrix(gate_rows), labels(gate_rows))
        candidate_metrics = evaluate_classifier(candidate, matrix(gate_rows), labels(gate_rows))
        version = 'adaptive-' + event['event_id']
        candidate_path = registry / f'candidate-{event["event_id"]}.joblib'
        save_bundle(candidate, fit_rows, candidate_path, version)
        with tracked_run(tracking, 'AdaptivePhish-retraining', version,
                         {'fit_rows': len(fit_rows), 'gate_rows': len(gate_rows),
                          'model': 'lightgbm', 'seed': 42,
                          'hyperparameters': json.dumps(candidate.get_params(), sort_keys=True, default=str)},
                         {'dataset_fingerprint': event['dataset_fingerprint']}) as run_id:
            log_metrics(candidate_metrics)
            import mlflow
            mlflow.log_dict({'artifact_reference': candidate_path.name,
                             'sha256': checksum(candidate_path),
                             'evaluation_fingerprint': event['evaluation_fingerprint'],
                             'feature_schema': FEATURE_SCHEMA_VERSION}, 'model_reference.json')
            event['mlflow_run_id'] = run_id
        event.update(candidate_metrics=candidate_metrics, champion_metrics=champion_metrics,
                     candidate_sha256=checksum(candidate_path), champion_sha256=current['sha256'])
        try:
            event['decision'] = promotion_decision(champion_metrics, candidate_metrics)
        except GovernanceError:
            event['decision'] = {'approved': False, 'reason': 'undefined_or_invalid_metrics'}
        if event['decision']['approved']:
            promote(candidate_path, registry, version, candidate_metrics,
                    champion_metrics=champion_metrics, evidence=event,
                    expected_champion=current['version'])
            event['promoted'] = True
    atomic_json(registry / f'event-{event["event_id"]}.json', event)
    RETRAINING.labels(outcome='promoted' if event['promoted'] else
                     'rejected' if trigger['triggered'] else 'not_triggered').inc()
    return event

"""Offline DVC stages. No data download and no writes to Phase 3 artifacts."""

import argparse
from pathlib import Path

import joblib

from adaptivephish.data.output_writer import WORKSPACE_ROOT
from adaptivephish.evaluation.metrics import evaluate_classifier
from adaptivephish.features.url_features import FEATURE_NAMES, FEATURE_SCHEMA_VERSION
from adaptivephish.models.training import build_candidates
from adaptivephish.pipelines.research import research_partitions
from adaptivephish.pipelines.temporal_pipeline import load_verified_local_rows
from adaptivephish.registry.local_registry import atomic_json
from adaptivephish.retraining.lifecycle import fingerprint, labels, matrix


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=('verify', 'prepare', 'train', 'evaluate'))
    stage = parser.parse_args(argv).stage
    root = WORKSPACE_ROOT
    features = root / 'data/processed/features.joblib'
    model_path = root / 'artifacts/pipeline_model.joblib'
    if stage == 'verify':
        try:
            from scripts.verify_expanded_dataset import verify_local_inventory
        except ModuleNotFoundError:
            from verify_expanded_dataset import verify_local_inventory
        report = verify_local_inventory()
        if not report['success']:
            raise ValueError('verification_failed')
        atomic_json(root / 'reports/results/dataset_quality.json', report)
    elif stage == 'prepare':
        rows, sources = load_verified_local_rows()
        (initial, update, gate, test), _, purged = research_partitions(rows)
        prepared = {name: {'x': matrix(group), 'y': labels(group),
                           'fingerprint': fingerprint(group)}
                    for name, group in (('train', initial + update), ('validation', gate), ('test', test))}
        prepared['provenance'] = {'sources': sources, 'dataset_fingerprint': fingerprint(rows),
                                  'purged_rows': purged, 'overlap': 0}
        features.parent.mkdir(parents=True, exist_ok=True)
        _atomic_joblib(features, prepared)
    elif stage == 'train':
        data = joblib.load(features)
        models = build_candidates()
        scores = {}
        for name, model in models.items():
            model.fit(data['train']['x'], data['train']['y'])
            scores[name] = evaluate_classifier(model, data['validation']['x'], data['validation']['y'])
        selected = max(scores, key=lambda name: scores[name]['f1'])
        _atomic_joblib(model_path, {'model': models[selected], 'model_version': 'pipeline-v2',
                                   'feature_schema_version': FEATURE_SCHEMA_VERSION,
                                   'feature_names': list(FEATURE_NAMES),
                                   'metadata': {'selected_model': selected, **data['provenance']}})
        atomic_json(root / 'reports/results/pipeline_validation_v2.json', scores)
    else:
        data, bundle = joblib.load(features), joblib.load(model_path)
        atomic_json(root / 'reports/results/pipeline_test_v2.json', {
            'provenance': data['provenance'], 'model': bundle['metadata']['selected_model'],
            'test': evaluate_classifier(bundle['model'], data['test']['x'], data['test']['y'])})
    return 0


def _atomic_joblib(path, value):
    import os
    import tempfile
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    os.close(descriptor)
    try:
        joblib.dump(value, temporary)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


if __name__ == '__main__':
    raise SystemExit(main())

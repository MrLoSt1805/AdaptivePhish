"""End-to-end Phase 3 model comparison with held-out test evaluation."""

import csv
import json
import os
from pathlib import Path
import tempfile

from adaptivephish.data.output_writer import WORKSPACE_ROOT
from adaptivephish.data.source_config import load_source_config
from adaptivephish.evaluation.metrics import evaluate_classifier
from adaptivephish.features.url_features import (
    FEATURE_NAMES, FEATURE_SCHEMA_VERSION, feature_matrix,
)
from adaptivephish.models.artifact import ARTIFACT_PATH, save_model
from adaptivephish.models.training import CANDIDATE_ORDER, build_candidates
from adaptivephish.pipelines.data_preparation import load_verified_data


JSON_REPORT = WORKSPACE_ROOT / 'reports' / 'results' / 'phase3_metrics.json'
CSV_REPORT = WORKSPACE_ROOT / 'reports' / 'results' / 'phase3_model_comparison.csv'


class PipelineError(Exception):
    """Sanitized training-pipeline failure."""


def _atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.adaptivephish-report-',
                                             suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8', newline='') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _selection_key(item):
    name, metrics = item
    auc = -1.0 if metrics['roc_auc'] is None else metrics['roc_auc']
    return (metrics['f1'], auc, metrics['accuracy'], -CANDIDATE_ORDER.index(name))


def train_and_evaluate(overwrite=False):
    outputs = (ARTIFACT_PATH, JSON_REPORT, CSV_REPORT)
    if not overwrite and any(path.exists() for path in outputs):
        raise PipelineError('phase3_output_exists')
    try:
        prepared = load_verified_data()
        matrices = {
            'train': feature_matrix(prepared.train.urls),
            'validation': feature_matrix(prepared.validation.urls),
            'test': feature_matrix(prepared.test.urls),
        }
        candidates = build_candidates(prepared.seed)
        validation = {}
        for name in CANDIDATE_ORDER:
            model = candidates[name]
            model.fit(matrices['train'], prepared.train.labels)
            validation[name] = evaluate_classifier(
                model, matrices['validation'], prepared.validation.labels
            )
        selected_name = max(validation.items(), key=_selection_key)[0]

        final_model = build_candidates(prepared.seed)[selected_name]
        train_validation_x = matrices['train'] + matrices['validation']
        train_validation_y = prepared.train.labels + prepared.validation.labels
        final_model.fit(train_validation_x, train_validation_y)
        test_metrics = evaluate_classifier(
            final_model, matrices['test'], prepared.test.labels
        )
        config = load_source_config()
        report = {
            'report_version': 'phase3-v1',
            'source': {'dataset': config.dataset, 'revision': config.revision,
                       'split': config.split, 'shard': config.shard},
            'random_seed': prepared.seed,
            'split_ratios': list(prepared.ratios),
            'split_counts': {
                'train': len(prepared.train.labels),
                'validation': len(prepared.validation.labels),
                'test': len(prepared.test.labels),
            },
            'feature_schema_version': FEATURE_SCHEMA_VERSION,
            'feature_names': list(FEATURE_NAMES),
            'selection_metric': 'validation_f1',
            'tie_breakers': ['validation_roc_auc', 'validation_accuracy',
                             'candidate_order'],
            'validation_metrics': validation,
            'selected_model': selected_name,
            'test_metrics': test_metrics,
            'latency_method': 'single-threaded predict_proba plus predict wall time',
            'limitations': [
                'single source shard', 'random split is not temporal evaluation',
                'lexical URL features only',
            ],
        }
        save_model(final_model, {
            'selected_model': selected_name, 'source': report['source'],
            'random_seed': prepared.seed,
        }, overwrite=overwrite)
        _atomic_text(JSON_REPORT, json.dumps(report, indent=2, sort_keys=True) + '\n')
        rows = []
        for name in CANDIDATE_ORDER:
            row = {'scope': 'validation', 'model': name, **validation[name]}
            row['confusion_matrix'] = json.dumps(row['confusion_matrix'], sort_keys=True)
            rows.append(row)
        final_row = {'scope': 'test_selected', 'model': selected_name, **test_metrics}
        final_row['confusion_matrix'] = json.dumps(
            final_row['confusion_matrix'], sort_keys=True
        )
        rows.append(final_row)
        fields = list(rows[0])
        buffer = __import__('io').StringIO(newline='')
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
        _atomic_text(CSV_REPORT, buffer.getvalue())
        return report
    except PipelineError:
        raise
    except Exception as error:
        category = getattr(error, 'args', ['training_failed'])[0]
        allowed = {'ml_dependencies_unavailable', 'source_verification_failed'}
        raise PipelineError(category if category in allowed else 'training_failed') from None

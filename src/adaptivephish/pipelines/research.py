"""Leakage-audited partial-data research, with a held-out future test window."""

from collections import Counter
import json
import subprocess
import uuid
import hashlib
from importlib.metadata import version as package_version

import joblib

from adaptivephish.evaluation.metrics import evaluate_classifier
from adaptivephish.features.url_features import FEATURE_NAMES, FEATURE_SCHEMA_VERSION, feature_matrix
from adaptivephish.models.training import build_candidates
from adaptivephish.pipelines.data_preparation import split_rows
from adaptivephish.registry.local_registry import initialize, resolve_champion, atomic_json, checksum
from adaptivephish.retraining.lifecycle import (
    fingerprint, labels, matrix, run_lifecycle, save_bundle, url_hashes,
)
from adaptivephish.tracking.mlflow_tracker import tracked_run, log_metrics


def research_partitions(rows):
    """Use whole dates; purge repeat URLs from later partitions, reject conflicts."""
    by_url = {}
    for row in rows:
        if row['label'] not in {'benign', 'phish'}:
            raise ValueError('invalid_label')
        if row['url'] in by_url and by_url[row['url']] != row['label']:
            raise ValueError('conflicting_url_labels')
        by_url[row['url']] = row['label']
    days = sorted({r['date'] for r in rows})
    if len(days) < 10:
        raise ValueError('insufficient_temporal_coverage')
    cuts = [days[int(len(days) * ratio)] for ratio in (.5, .7, .8)]
    groups = [[], [], [], []]
    seen = set()
    purged = 0
    for row in sorted(rows, key=lambda r: (r['date'], r['url'])):
        if row['url'] in seen:
            purged += 1
            continue
        seen.add(row['url'])
        index = sum(row['date'] >= cut for cut in cuts)
        groups[index].append(row)
    if any(not group for group in groups):
        raise ValueError('empty_temporal_partition')
    return groups, cuts, purged


def _summary(rows):
    return {'rows': len(rows), 'classes': dict(Counter(r['label'] for r in rows)),
            'min_date': min(r['date'] for r in rows).isoformat(),
            'max_date': max(r['date'] for r in rows).isoformat(),
            'fingerprint': fingerprint(rows)}


def run_research(overwrite=False):
    from adaptivephish.pipelines.temporal_pipeline import (
        REPORT_PATH, TRACKING_PATH, WORKSPACE_ROOT, TemporalPipelineError,
        load_verified_local_rows,
    )
    if REPORT_PATH.exists() and not overwrite:
        raise TemporalPipelineError('temporal_output_exists')
    rows, sources = load_verified_local_rows()
    (initial, update, gate, test), cuts, purged = research_partitions(rows)
    run_version = 'research-' + uuid.uuid4().hex[:12]
    root = WORKSPACE_ROOT / 'artifacts' / run_version
    root.mkdir(parents=True)
    git_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORKSPACE_ROOT,
                                         text=True).strip()
    report = {'report_version': 'temporal-v2', 'validity': 'valid_partial_data',
              'scope': 'verified_local_train_shards_only', 'source_shards': sources,
              'row_count': len(rows), 'dataset_fingerprint': fingerprint(rows),
              'cutoff_date': cuts[-1].isoformat(), 'train_rows': len(initial) + len(update),
              'validation_rows': len(gate), 'test_rows': len(test),
              'partitions': {name: _summary(group) for name, group in
                             zip(('initial', 'update', 'governance', 'test'),
                                 (initial, update, gate, test))},
              'purged_duplicate_rows': purged, 'fit_evaluation_overlap': 0,
              'git_commit': git_commit, 'run_version': run_version,
              'source_code_fingerprint': hashlib.sha256(b''.join(
                  p.relative_to(WORKSPACE_ROOT).as_posix().encode() + p.read_bytes()
                  for folder in ('src', 'scripts') for p in sorted((WORKSPACE_ROOT / folder).rglob('*.py')))).hexdigest(),
              'package_versions': {name: package_version(name) for name in
                                   ('numpy', 'scikit-learn', 'lightgbm', 'xgboost', 'mlflow', 'pyarrow')},
              'feature_schema': FEATURE_SCHEMA_VERSION, 'feature_names': list(FEATURE_NAMES),
              'protocol': '50/20/10/20 percent of unique dates; promote on governance only; final test never trains or gates',
              'limitations': ['5 of 77 publisher shards, train split only',
                              'collection date is not label availability time; update labels assumed available at gate cutoff',
                              'one offline adaptation event; not a live deployment',
                              'random and chronological tests differ; no causal improvement claim',
                              'original phase3 champion excluded from temporal comparisons']}
    random = split_rows(rows)
    comparisons = {}
    # Both protocols select on validation only, then report an untouched test.
    for protocol, train_x, train_y, val_x, val_y, test_x, test_y in (
        ('random', feature_matrix(random.train.urls), random.train.labels,
         feature_matrix(random.validation.urls), random.validation.labels,
         feature_matrix(random.test.urls), random.test.labels),
        ('chronological', matrix(initial + update), labels(initial + update), matrix(gate), labels(gate), matrix(test), labels(test)),
    ):
        models = build_candidates()
        validation = {}
        fitting_rows = (initial + update if protocol == 'chronological' else
                        [{'url': url, 'label': 'phish' if label else 'benign', 'date': date}
                         for url, label, date in zip(random.train.urls, random.train.labels, random.train.dates)])
        for name, model in models.items():
            model.fit(train_x, train_y)
            model_file = root / f'{protocol}-{name}.joblib'
            save_bundle(model, fitting_rows, model_file, run_version + '-' + protocol + '-' + name)
            validation[name] = evaluate_classifier(model, val_x, val_y)
            with tracked_run(TRACKING_PATH, 'AdaptivePhish-research-v2', f'{protocol}-{name}',
                             {'model': name, 'split': protocol, 'seed': 42,
                              'train_rows': len(train_y), 'validation_rows': len(val_y),
                              'hyperparameters': json.dumps(model.get_params(), default=str, sort_keys=True)},
                             {'dataset_fingerprint': report['dataset_fingerprint'],
                              'git_commit': git_commit, 'feature_schema': FEATURE_SCHEMA_VERSION}) as run_id:
                log_metrics(validation[name])
                import mlflow
                mlflow.log_dict({'artifact_reference': model_file.relative_to(WORKSPACE_ROOT).as_posix(),
                                 'sha256': checksum(model_file), 'fit_fingerprint': fingerprint(fitting_rows),
                                 'feature_names': list(FEATURE_NAMES)}, 'model_reference.json')
                validation[name]['mlflow_run_id'] = run_id
        selected = max(validation, key=lambda name: validation[name]['f1'])
        comparisons[protocol] = {'validation_metrics': validation, 'selected_model': selected,
                                 'test_metrics': evaluate_classifier(models[selected], test_x, test_y),
                                 'train_rows': len(train_y), 'validation_rows': len(val_y),
                                 'test_rows': len(test_y)}
        if protocol == 'chronological':
            save_bundle(models[selected], initial + update, root / 'chronological.joblib', run_version + '-chrono')
    report['model_comparison'] = comparisons

    static = build_candidates()['lightgbm']
    static.fit(matrix(initial), labels(initial))
    static_path = root / 'static.joblib'
    save_bundle(static, initial, static_path, run_version + '-static')
    initial_metrics = evaluate_classifier(static, matrix(gate), labels(gate))
    initialize(static_path, root / 'registry', run_version + '-static', initial_metrics,
               {'dataset_fingerprint': fingerprint(initial)})
    event = run_lifecycle(root / 'registry', initial, update, gate, TRACKING_PATH)
    active_path, active_doc = resolve_champion(root / 'registry')
    adaptive = joblib.load(active_path)
    if set(adaptive['metadata']['fit_url_hashes']) & set(url_hashes(test)):
        raise TemporalPipelineError('temporal_url_leakage')
    report['lifecycle'] = event
    report['drift'] = event['drift']
    report['static_metrics'] = evaluate_classifier(static, matrix(test), labels(test))
    report['adaptive_metrics'] = evaluate_classifier(adaptive['model'], matrix(test), labels(test))
    report['active_version'] = active_doc['version']
    report['registry_path'] = str((root / 'registry').relative_to(WORKSPACE_ROOT))
    report['monthly_class_distribution'] = {
        month: dict(Counter(r['label'] for r in rows if r['date'].isoformat()[:7] == month))
        for month in sorted({r['date'].isoformat()[:7] for r in rows})}
    report['future_windows'] = []
    for month in sorted({r['date'].isoformat()[:7] for r in test}):
        window = [r for r in test if r['date'].isoformat()[:7] == month]
        report['future_windows'].append({'month': month, **_summary(window),
                                        'static': evaluate_classifier(static, matrix(window), labels(window)),
                                        'adaptive': evaluate_classifier(adaptive['model'], matrix(window), labels(window))})
    _plots(report, test, static, adaptive['model'], WORKSPACE_ROOT / 'reports' / 'figures')
    atomic_json(REPORT_PATH, report)
    atomic_json(root / 'report.json', report)
    return report


def _plots(report, test, static, adaptive, directory):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from sklearn.metrics import ConfusionMatrixDisplay, RocCurveDisplay, PrecisionRecallDisplay
    directory.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots()
    ax.bar(['Static', 'Governed adaptive'], [report['static_metrics']['f1'], report['adaptive_metrics']['f1']])
    ax.set(ylabel='F1', ylim=(0, 1), title='Untouched future window (partial dataset)')
    fig.savefig(directory / 'temporal_f1_comparison_v2.png', dpi=150)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(10, 5))
    scores = report['drift']['feature_psi']
    ax.bar(list(scores), list(scores.values()))
    ax.tick_params(axis='x', rotation=90)
    ax.set(ylabel='PSI', title='Initial versus labeled update window')
    fig.tight_layout()
    fig.savefig(directory / 'drift_v2.png', dpi=150)
    plt.close(fig)
    fig, axes = plt.subplots(2, 1, figsize=(8, 7))
    windows = report['future_windows']
    for name in ('static', 'adaptive'):
        axes[0].plot([w['month'] for w in windows], [w[name]['f1'] for w in windows], marker='o', label=name)
    axes[0].set(ylabel='F1', title='Future windows: frozen static and governed models')
    axes[0].legend()
    months = list(report['monthly_class_distribution'])
    for label in ('benign', 'phish'):
        axes[1].plot(months, [report['monthly_class_distribution'][m].get(label, 0) for m in months], label=label)
    axes[1].tick_params(axis='x', rotation=90)
    axes[1].set(ylabel='Rows', title='Monthly class coverage: all verified local rows')
    axes[1].legend()
    fig.tight_layout()
    fig.savefig(directory / 'monthly_temporal_v2.png', dpi=150)
    plt.close(fig)
    for name, model in (('static', static), ('adaptive', adaptive)):
        ConfusionMatrixDisplay.from_predictions(labels(test), model.predict(matrix(test)), labels=[0, 1])
        plt.savefig(directory / f'{name}_confusion_v2.png', dpi=150)
        plt.close()
        if len(set(labels(test))) == 2:
            probabilities = model.predict_proba(matrix(test))[:, 1]
            for kind, display in (('roc', RocCurveDisplay), ('pr', PrecisionRecallDisplay)):
                display.from_predictions(labels(test), probabilities)
                plt.savefig(directory / f'{name}_{kind}_v2.png', dpi=150)
                plt.close()

"""One-command, offline demonstration using reserved synthetic URL inputs."""

import json

from adaptivephish.models.artifact import ModelArtifactError, predict_url
from adaptivephish.retraining.simulation import run_simulation
from adaptivephish.features.url_features import FEATURE_NAMES, feature_matrix
from adaptivephish.data.output_writer import WORKSPACE_ROOT


def main():
    examples = ('https://example.invalid/home',
                'https://secure-login-check.invalid/account/verify')
    try:
        predictions = [predict_url(value) for value in examples]
        result = {
            'success': True,
            'failure_category': None,
            'examples_processed': len(predictions),
            'prediction_counts': {
                label: sum(item['predicted_label'] == label for item in predictions)
                for label in ('benign', 'phish')
            },
            'model_version': predictions[0]['model_version'],
            'raw_urls_printed': False,
            'feature_count': len(FEATURE_NAMES),
            'feature_extraction_verified': len(feature_matrix(examples)[0]) == len(FEATURE_NAMES),
            'synthetic_lifecycle': run_simulation(),
        }
        report_path = WORKSPACE_ROOT / 'reports/results/temporal_evaluation_v2.json'
        if report_path.exists():
            report = json.loads(report_path.read_text())
            result['verified_research'] = {
                'rows': report['row_count'], 'shards': len(report['source_shards']),
                'dataset_fingerprint': report['dataset_fingerprint'],
                'static_f1': report['static_metrics']['f1'],
                'adaptive_f1': report['adaptive_metrics']['f1'],
                'decision': report['lifecycle']['decision'],
                'mlflow_run_id': report['lifecycle']['mlflow_run_id'],
                'scope': report['scope'], 'cutoff_date': report['cutoff_date']}
        else:
            result['verified_research'] = {'available': False}
    except ModelArtifactError as error:
        result = {'success': False, 'failure_category': str(error),
                  'examples_processed': 0}
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

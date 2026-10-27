"""Run the local temporal experiment explicitly; never accesses the network."""

import json
import sys

from adaptivephish.pipelines.temporal_pipeline import (
    TemporalPipelineError, run_temporal_evaluation,
)


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    if arguments not in (['--run'], ['--run', '--overwrite']):
        print(json.dumps({'success': False, 'failure_category': 'run_flag_required'}))
        return 2
    try:
        report = run_temporal_evaluation(overwrite='--overwrite' in arguments)
        result = {'success': True, 'failure_category': None,
                  'row_count': report['row_count'],
                  'cutoff_date': report['cutoff_date'],
                  'train_rows': report['train_rows'],
                  'test_rows': report['test_rows'],
                  'drift_status': report['drift']['status'],
                  'report_path': 'reports/results/temporal_evaluation_v2.json'}
    except TemporalPipelineError as error:
        result = {'success': False, 'failure_category': str(error)}
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

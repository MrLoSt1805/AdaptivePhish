"""Evaluate the temporal candidate against the unchanged Phase 3 champion."""

import json

from adaptivephish.data.output_writer import WORKSPACE_ROOT
from adaptivephish.retraining.governance import promotion_decision


SOURCE = WORKSPACE_ROOT / 'reports' / 'results' / 'temporal_evaluation_v2.json'
OUTPUT = WORKSPACE_ROOT / 'reports' / 'results' / 'promotion_decision_v2.json'


def main():
    try:
        report = json.loads(SOURCE.read_text(encoding='utf-8'))
        if report.get('validity') != 'valid_partial_data' or report.get('fit_evaluation_overlap') != 0:
            raise ValueError('invalid_evaluation')
        lifecycle = report['lifecycle']
        decision = promotion_decision(lifecycle['champion_metrics'],
                                      lifecycle['candidate_metrics'])
        result = {'success': True, 'failure_category': None,
                  'champion_version': lifecycle['previous_version'],
                  'candidate': report['run_version'], **decision,
                  'automatic_promotion_performed': False}
        from adaptivephish.registry.local_registry import atomic_json
        atomic_json(OUTPUT, result)
    except Exception:
        result = {'success': False,
                  'failure_category': 'governance_evaluation_failed'}
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

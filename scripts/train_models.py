"""Train and compare all four Phase 3 candidates."""

import argparse
import json

from adaptivephish.pipelines.training_pipeline import PipelineError, train_and_evaluate


def main(argv=None):
    parser = argparse.ArgumentParser(description='Train AdaptivePhish Phase 3 models.')
    parser.add_argument('--overwrite', action='store_true',
                        help='replace existing Phase 3 artifact and aggregate reports')
    args = parser.parse_args(argv)
    try:
        report = train_and_evaluate(overwrite=args.overwrite)
        result = {'success': True, 'failure_category': None,
                  'selected_model': report['selected_model'],
                  'split_counts': report['split_counts']}
    except PipelineError as error:
        result = {'success': False, 'failure_category': str(error),
                  'selected_model': None, 'split_counts': {}}
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

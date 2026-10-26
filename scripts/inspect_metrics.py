"""Print the fixed aggregate Phase 3 metrics report."""

import argparse
import json

from adaptivephish.pipelines.training_pipeline import JSON_REPORT


def main(argv=None):
    argparse.ArgumentParser(description='Inspect aggregate Phase 3 metrics.').parse_args(argv)
    try:
        report = json.loads(JSON_REPORT.read_text(encoding='utf-8'))
        if report.get('report_version') != 'phase3-v1':
            raise ValueError
        print(json.dumps(report, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'success': False,
                          'failure_category': 'metrics_report_unavailable'}, sort_keys=True))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

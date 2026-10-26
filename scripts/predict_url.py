"""Predict one URL without echoing it."""

import argparse
import json

from adaptivephish.models.artifact import ModelArtifactError, predict_url


def main(argv=None):
    parser = argparse.ArgumentParser(description='Classify one URL locally.')
    parser.add_argument('url', help='URL text; it is never visited or echoed')
    args = parser.parse_args(argv)
    try:
        result = {'success': True, 'failure_category': None,
                  **predict_url(args.url)}
    except ModelArtifactError as error:
        result = {'success': False, 'failure_category': str(error),
                  'predicted_label': None, 'phishing_probability': None,
                  'model_version': None, 'feature_schema_version': None}
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

"""Four-byte range compatibility check; network runs only with --live.

This measures application response-body bytes, not total network traffic.
HTTP headers, redirects, TLS, and socket buffering can use additional bytes.
No footer or records are requested.
"""

import importlib.util
import json
import sys
from pathlib import Path


def main():
    result = {
        'success': False,
        'failure_category': 'live_flag_required',
        'http_request_count': 0,
        'application_response_body_bytes': 0,
    }
    if sys.argv[1:] != ['--live']:
        print(json.dumps(result, sort_keys=True))
        return 2

    reader = None
    try:
        source = Path(__file__).with_name('test_remote_parquet.py')
        spec = importlib.util.spec_from_file_location('remote_parquet_reader', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Reuse the pinned URL, expected file size, redirect restrictions, and
        # header validation from RangeFile. Only bytes 0-3 are authorized.
        reader = module.RangeFile(url=module.URL, size=module.SIZE, budget=4)
        reader.allowed = [(0, 4)]
        if reader.fetch(0, 4) == b'PAR1':
            result['success'] = True
            result['failure_category'] = None
        else:
            result['failure_category'] = 'invalid_parquet_magic'
    except Exception as exc:
        # Never print exception text, which may contain a signed URL or data.
        if reader is not None and 'module' in locals() and isinstance(exc, module.SafetyError):
            result['failure_category'] = module._failure_category(exc.category)
        else:
            result['failure_category'] = 'internal_error'
    finally:
        if reader is not None:
            result['http_request_count'] = reader.request_count
            result['application_response_body_bytes'] = reader.body_bytes
            reader.close()

    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    sys.exit(main())

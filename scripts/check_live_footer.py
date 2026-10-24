"""Check only a pinned Parquet shard's final eight bytes with --live.

The application-body counter is not a hard network-transfer cap. HTTP
headers, redirects, TLS, and socket buffering can consume additional bytes.
No footer contents, column chunks, or records are requested.
"""

import importlib.util
import json
import struct
import sys
from pathlib import Path


def main():
    result = {
        'success': False,
        'failure_category': 'live_flag_required',
        'http_request_count': 0,
        'application_response_body_bytes': 0,
        'footer_length_bytes': None,
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

        reader = module.RangeFile(url=module.URL, size=module.SIZE, budget=8)
        start = module.SIZE - 8
        reader.allowed = [(start, module.SIZE)]
        trailer = reader.fetch(start, module.SIZE)

        if trailer[4:] != b'PAR1':
            result['failure_category'] = 'invalid_parquet_magic'
        else:
            length = struct.unpack('<I', trailer[:4])[0]
            if not 0 < length <= min(module.MAX_FOOTER, module.SIZE - 12):
                result['failure_category'] = 'invalid_footer_length'
            else:
                result['footer_length_bytes'] = length
                result['failure_category'] = None
                result['success'] = True
    except Exception as exc:
        # Never include exception text, bodies, signed URLs, or tokens.
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

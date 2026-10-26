"""Discover the pinned source-shard inventory only with explicit ``--live``.

Only the official Hugging Face repository metadata API is queried. Diagnostic
output uses fixed categories and never includes request URLs, response bodies,
credentials, or exception text.
"""

import argparse
import json
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request

from adaptivephish.data.shard_inventory import (
    DATASET, REVISION, InventoryError, merge_discovered_inventory,
)


MAX_RESPONSE_BYTES = 2 * 1024 * 1024
EXPECTED_SHARDS = 77
EXPECTED_SPLIT_COUNTS = {'train': 56, 'test': 21}
EXPECTED_PATHS = {
    *(f'data/train-{index:03}.parquet' for index in range(56)),
    *(f'data/test-{index:03}.parquet' for index in range(21)),
}


class DiscoveryError(Exception):
    """Sanitized discovery failure with no untrusted remote text."""

    def __init__(self, category, stage, http_status=None):
        self.category = category
        self.stage = stage
        self.http_status = http_status
        super().__init__(category)


def _revision_metadata_url():
    dataset = '/'.join(urllib.parse.quote(part, safe='')
                       for part in DATASET.split('/'))
    revision = urllib.parse.quote(REVISION, safe='')
    return (f'https://huggingface.co/api/datasets/{dataset}/revision/'
            f'{revision}?blobs=true')


def _read_json(opener, url, stage):
    request = urllib.request.Request(
        url,
        headers={
            'Accept': 'application/json',
            'Accept-Encoding': 'identity',
            'User-Agent': 'AdaptivePhish-inventory/1',
        },
    )
    try:
        with opener(request, timeout=30) as response:
            status = getattr(response, 'status', None)
            if status is None:
                status = response.getcode()
            if status != 200:
                raise DiscoveryError('http_status_error', stage, status)
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except DiscoveryError:
        raise
    except urllib.error.HTTPError as error:
        category = 'revision_not_found' if error.code == 404 else 'http_status_error'
        raise DiscoveryError(category, stage, error.code) from None
    except (socket.timeout, TimeoutError):
        raise DiscoveryError('timeout', stage) from None
    except (urllib.error.URLError, OSError):
        raise DiscoveryError('transport_error', stage) from None

    if len(body) > MAX_RESPONSE_BYTES:
        raise DiscoveryError('response_too_large', stage)
    try:
        return json.loads(body.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise DiscoveryError('response_schema_mismatch', stage) from None


def _file_size(sibling):
    size = sibling.get('size')
    if type(size) is int:
        return size
    lfs = sibling.get('lfs')
    if isinstance(lfs, dict) and type(lfs.get('size')) is int:
        return lfs['size']
    raise DiscoveryError('response_schema_mismatch', 'parse_inventory')


def _parse_revision_metadata(document):
    if not isinstance(document, dict):
        raise DiscoveryError('response_schema_mismatch', 'parse_inventory')
    if document.get('sha') != REVISION:
        raise DiscoveryError('revision_mismatch', 'validate_revision')
    siblings = document.get('siblings')
    if not isinstance(siblings, list):
        raise DiscoveryError('response_schema_mismatch', 'parse_inventory')

    shards = []
    seen = set()
    split_counts = {'train': 0, 'test': 0}
    for sibling in siblings:
        if not isinstance(sibling, dict):
            raise DiscoveryError('response_schema_mismatch', 'parse_inventory')
        path = sibling.get('rfilename')
        if not isinstance(path, str):
            raise DiscoveryError('response_schema_mismatch', 'parse_inventory')
        if not path.endswith('.parquet'):
            continue
        if path not in EXPECTED_PATHS:
            raise DiscoveryError('publisher_split_mismatch', 'validate_inventory')
        if path.startswith('data/train-'):
            split = 'train'
        else:
            split = 'test'
        if path in seen:
            raise DiscoveryError('duplicate_shard_path', 'validate_inventory')
        size = _file_size(sibling)
        if size < 12:
            raise DiscoveryError('response_schema_mismatch', 'parse_inventory')
        seen.add(path)
        split_counts[split] += 1
        shards.append({'path': path, 'size': size, 'split': split})

    if len(shards) != EXPECTED_SHARDS:
        raise DiscoveryError('shard_count_mismatch', 'validate_inventory')
    if split_counts != EXPECTED_SPLIT_COUNTS:
        raise DiscoveryError('publisher_split_mismatch', 'validate_inventory')
    return sorted(shards, key=lambda item: item['path']), split_counts


def discover(opener=urllib.request.urlopen):
    document = _read_json(
        opener, _revision_metadata_url(), 'fetch_revision_metadata'
    )
    shards, split_counts = _parse_revision_metadata(document)
    return {
        'success': True,
        'failure_category': None,
        'dataset': DATASET,
        'revision': REVISION,
        'shard_count': len(shards),
        'split_counts': split_counts,
        'shards': shards,
        'note': ('Row counts and per-shard ingestion budgets require separate '
                 'metadata-only footer preflights.'),
    }


def _failure(error, diagnostic=False):
    result = {
        'success': False,
        'failure_category': 'inventory_discovery_failed',
        'dataset': DATASET,
        'revision': REVISION,
        'shards': [],
    }
    if diagnostic:
        result['diagnostic'] = {
            'category': error.category,
            'stage': error.stage,
            'http_status': error.http_status,
        }
    return result


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--diagnostic', action='store_true')
    parser.add_argument('--persist', action='store_true')
    arguments = parser.parse_args(argv)

    if not arguments.live:
        error = DiscoveryError('live_flag_required', 'argument_validation')
        result = _failure(error, arguments.diagnostic)
        result['failure_category'] = 'live_flag_required'
        print(json.dumps(result, sort_keys=True))
        return 2

    try:
        result = discover()
        if arguments.persist:
            document = merge_discovered_inventory(result)
            result['persisted'] = True
            result['inventory_status'] = document['inventory_status']
    except InventoryError:
        result = _failure(
            DiscoveryError('inventory_persistence_failed', 'persist_inventory'),
            arguments.diagnostic,
        )
    except DiscoveryError as error:
        result = _failure(error, arguments.diagnostic)
    except Exception:
        result = _failure(
            DiscoveryError('unexpected_error', 'inventory_discovery'),
            arguments.diagnostic,
        )
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    sys.exit(main())

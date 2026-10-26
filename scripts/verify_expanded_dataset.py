"""Aggregate-only local verification across available inventory shards."""

import collections
import json

import pyarrow.parquet as pq

from adaptivephish.data.multi_shard import local_output_path
from adaptivephish.data.output_writer import verify_completed_output
from adaptivephish.data.shard_inventory import load_inventory


def _new_aggregate():
    return {
        'counts': collections.Counter(), 'identifiers': {},
        'urls': set(), 'rows': set(), 'duplicates': collections.Counter(),
    }


def _add_record(state, split, row):
    counts, duplicates = state['counts'], state['duplicates']
    seen_identifiers = state['identifiers']
    seen_urls, seen_rows = state['urls'], state['rows']
    identifier = row['sha256']
    url = row['url']
    signature = (url, row['label'], row['date'])
    if identifier in seen_identifiers:
        duplicates['sha256'] += 1
        if seen_identifiers[identifier] != signature:
            duplicates['conflicting_sha256'] += 1
    else:
        seen_identifiers[identifier] = signature
    if url in seen_urls:
        duplicates['url'] += 1
    else:
        seen_urls.add(url)
    full_row = (identifier,) + signature
    if full_row in seen_rows:
        duplicates['row'] += 1
    else:
        seen_rows.add(full_row)
    counts[(split, row['label'])] += 1


def _aggregate_records(records):
    state = _new_aggregate()
    for split, row in records:
        _add_record(state, split, row)
    return state['counts'], state['duplicates']


def verify_local_inventory():
    inventory = load_inventory()
    aggregate = _new_aggregate()
    verified = []
    for shard in inventory.shards:
        output = local_output_path(shard)
        if not output.exists():
            continue
        document = verify_completed_output(output)
        if document.get('source') != shard.source:
            raise ValueError
        parquet = pq.ParquetFile(output)
        try:
            for batch in parquet.iter_batches(batch_size=1_024, use_threads=False):
                for row in batch.to_pylist():
                    _add_record(aggregate, shard.split, row)
        finally:
            parquet.close()
        verified.append(shard.key)
    counts, duplicates = aggregate['counts'], aggregate['duplicates']
    conflict = duplicates['conflicting_sha256'] > 0
    return {
        'success': not conflict,
        'failure_category': 'conflicting_identifier' if conflict else None,
        'inventory_status': inventory.status,
        'verified_shard_count': len(verified),
        'expected_official_shards': inventory.expected_official_shards,
        'row_counts': {
            split: {label: counts[(split, label)]
                    for label in ('benign', 'phish')}
            for split in ('train', 'test')
        },
        'duplicate_counts': {
            name: duplicates[name]
            for name in ('sha256', 'url', 'row', 'conflicting_sha256')
        },
        'policy': 'report duplicates; reject conflicting identifiers before training',
    }


def main():
    try:
        result = verify_local_inventory()
    except Exception:
        result = {'success': False, 'failure_category': 'aggregate_verification_failed'}
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get('success') else 1


if __name__ == '__main__':
    raise SystemExit(main())

"""Offline, immutable inventory-based campaign proposal; never grants approval."""
import hashlib
import json

from adaptivephish.data.shard_inventory import load_inventory, DATASET, REVISION, SELECTED_COLUMNS
from adaptivephish.data.multi_shard import local_output_path
from adaptivephish.data.output_writer import verify_completed_output, WORKSPACE_ROOT
from adaptivephish.registry.local_registry import atomic_json


def main():
    inventory = load_inventory()
    entries = []
    for shard in inventory.shards:
        output = local_output_path(shard)
        if output.exists():
            if verify_completed_output(output)['source'] != shard.source:
                raise ValueError('source_mismatch')
            continue
        entries.append({'key': shard.key, 'split': shard.split, 'path': shard.path,
                        'publisher_file_bytes': shard.expected_file_size,
                        'projected_bytes': None, 'projected_requests': None,
                        'maximum_http_requests': shard.maximum_http_requests})
    proposal = {'status': 'awaiting_metadata_preflight_authorization',
                'dataset': DATASET, 'revision': REVISION, 'columns': list(SELECTED_COLUMNS),
                'remaining_shards': len(entries), 'shards': entries,
                'network_accessed': False, 'approved': False,
                'note': 'Projected byte/request estimates require a separately budgeted metadata-only preflight. No budgets or approval are inferred.'}
    proposal['proposal_digest'] = hashlib.sha256(json.dumps(proposal, sort_keys=True).encode()).hexdigest()
    target = WORKSPACE_ROOT / 'configs/remaining_campaign_proposal.json'
    if target.exists():
        if json.loads(target.read_text()) != proposal:
            raise ValueError('immutable_proposal_exists')
    else:
        atomic_json(target, proposal)
    print(json.dumps({'remaining_shards': len(entries), 'approved': False,
                      'proposal_digest': proposal['proposal_digest']}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

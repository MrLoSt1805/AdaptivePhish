"""Plan, approve, and run bounded PhreshPhish shard batches."""

import argparse
import json

from adaptivephish.data.batch_workflow import (
    PLAN_PATH, BatchError, approve_plan, ingest_approved_batch, load_plan,
    preflight_batch, select_shards, write_pending_plan,
)
from adaptivephish.data.shard_inventory import InventoryError, load_inventory
try:
    from scripts.verify_expanded_dataset import verify_local_inventory
except ModuleNotFoundError:  # Direct execution places scripts/ on sys.path.
    from verify_expanded_dataset import verify_local_inventory


def _selection(command):
    command.add_argument('--split', choices=('all', 'train', 'test'), default='all')
    command.add_argument('--shards', help='comma-separated exact shard keys')
    command.add_argument('--max-shards', type=int, required=True)


def build_parser():
    root = argparse.ArgumentParser()
    root.add_argument('--plan-path', default=str(PLAN_PATH))
    commands = root.add_subparsers(dest='command', required=True)
    dry = commands.add_parser('dry-run')
    _selection(dry)
    preflight = commands.add_parser('preflight')
    _selection(preflight)
    preflight.add_argument('--live', action='store_true')
    preflight.add_argument('--per-shard-byte-limit', type=int, required=True)
    preflight.add_argument('--session-byte-limit', type=int, required=True)
    preflight.add_argument('--session-request-limit', type=int, required=True)
    preflight.add_argument('--runtime-seconds', type=float, required=True)
    commands.add_parser('approve')
    ingest = commands.add_parser('ingest')
    ingest.add_argument('--live', action='store_true')
    ingest.add_argument('--approval-id', required=True)
    ingest.add_argument('--session-byte-limit', type=int, required=True)
    ingest.add_argument('--session-request-limit', type=int, required=True)
    ingest.add_argument('--runtime-seconds', type=float, required=True)
    commands.add_parser('verify')
    return root


def _keys(arguments):
    return None if not arguments.shards else tuple(arguments.shards.split(','))


def main(argv=None):
    arguments = build_parser().parse_args(argv)
    result = {'success': False, 'failure_category': None}
    try:
        if arguments.command in {'dry-run', 'preflight'}:
            shards = select_shards(load_inventory(), arguments.split,
                                   _keys(arguments), arguments.max_shards)
            if arguments.command == 'dry-run':
                result = {'success': True,
                          'selected_shards': [shard.key for shard in shards],
                          'shard_count': len(shards), 'network_accessed': False}
            elif not arguments.live:
                result['failure_category'] = 'live_flag_required'
            else:
                from pathlib import Path
                if Path(arguments.plan_path).exists():
                    raise BatchError('batch_plan_exists')
                plan = preflight_batch(
                    shards, arguments.session_byte_limit,
                    arguments.session_request_limit,
                    arguments.per_shard_byte_limit,
                    arguments.runtime_seconds,
                )
                write_pending_plan(plan, path=arguments.plan_path)
                result = {
                    'success': True, 'status': 'pending_review',
                    'shard_count': len(plan['shards']),
                    'metadata_application_body_bytes': plan[
                        'metadata_application_body_bytes'],
                    'metadata_http_requests': plan['metadata_http_requests'],
                    'plan_path': 'configs/phreshphish_batch_plan.json',
                }
        elif arguments.command == 'approve':
            approved = approve_plan(path=arguments.plan_path)
            result = {'success': True, 'status': 'approved',
                      'approval_id': approved['approval_id'],
                      'shard_count': len(approved['shards'])}
        elif arguments.command == 'ingest':
            if not arguments.live:
                result['failure_category'] = 'live_flag_required'
            else:
                result = ingest_approved_batch(
                    load_plan(path=arguments.plan_path, required_status='approved'), arguments.approval_id,
                    arguments.session_byte_limit,
                    arguments.session_request_limit,
                    arguments.runtime_seconds, verify_local_inventory,
                )
        else:
            result = verify_local_inventory()
    except (BatchError, InventoryError) as error:
        result['failure_category'] = str(error)
    except Exception:
        result['failure_category'] = 'batch_command_failed'
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get('success') else (2 if result.get(
        'failure_category') == 'live_flag_required' else 1)


if __name__ == '__main__':
    raise SystemExit(main())

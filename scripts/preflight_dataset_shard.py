"""Metadata-only preflight for one explicitly selected inventory shard."""

import argparse
import json

from adaptivephish.data.multi_shard import (
    MultiShardError, make_reader, preflight_reader,
)
from adaptivephish.data.shard_inventory import InventoryError, load_inventory


def main(argv=None):
    parser = argparse.ArgumentParser(description='Preflight one approved shard.')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--shard', required=True, help='inventory key, e.g. train-001')
    parser.add_argument('--session-budget', required=True, type=int)
    args = parser.parse_args(argv)
    result = {'success': False, 'failure_category': 'live_flag_required',
              'shard': args.shard, 'plan': None, 'http_request_count': 0,
              'application_body_bytes': 0}
    if not args.live:
        print(json.dumps(result, sort_keys=True))
        return 2
    reader = None
    try:
        shard = load_inventory().get(args.shard)
        reader = make_reader(shard, budget=args.session_budget)
        _, plan = preflight_reader(reader, shard, args.session_budget)
        result.update(success=True, failure_category=None, plan={
            'rows': plan.rows, 'row_groups': plan.row_groups,
            'footer_bytes': plan.footer_bytes,
            'selected_compressed_bytes': plan.selected_bytes,
            'estimated_application_body_bytes': plan.estimated_body_bytes,
            'planned_requests': plan.planned_requests,
            'per_shard_budget': shard.application_body_budget,
            'session_budget': args.session_budget,
        })
    except InventoryError:
        result['failure_category'] = 'invalid_or_unapproved_inventory_shard'
    except MultiShardError as error:
        result['failure_category'] = str(error)
    except Exception:
        result['failure_category'] = 'preflight_failed'
    finally:
        if reader is not None:
            result['http_request_count'] = reader.request_count
            result['application_body_bytes'] = reader.body_bytes
            reader.close()
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

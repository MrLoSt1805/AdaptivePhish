"""Bounded ingestion of one inventory shard after an exact preflight approval."""

import argparse
import json

from adaptivephish.data import output_writer
from adaptivephish.data.multi_shard import (
    MultiShardError, ingest_reader, local_output_path, make_reader,
)
from adaptivephish.data.shard_inventory import InventoryError, load_inventory


def main(argv=None):
    parser = argparse.ArgumentParser(description='Ingest one approved shard.')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--shard', required=True)
    parser.add_argument('--approved-estimate', required=True, type=int)
    parser.add_argument('--session-budget', required=True, type=int)
    args = parser.parse_args(argv)
    result = {'success': False, 'failure_category': 'live_flag_required',
              'shard': args.shard, 'status': None, 'row_count': None,
              'application_body_bytes': 0}
    if not args.live:
        print(json.dumps(result, sort_keys=True))
        return 2
    reader = None
    try:
        shard = load_inventory().get(args.shard)
        output = local_output_path(shard)
        sidecar = output_writer.manifest_path(output)
        if not output.exists() and not sidecar.exists():
            output_writer._require_git_ignored(output, sidecar)
        reader = make_reader(shard)
        outcome = ingest_reader(
            reader, shard, output, args.session_budget,
            approved_estimate=args.approved_estimate,
        )
        document = outcome['manifest']
        result.update(success=True, failure_category=None,
                      status=outcome['status'], row_count=document['row_count'],
                      application_body_bytes=outcome.get(
                          'application_body_bytes', 0
                      ))
    except InventoryError:
        result['failure_category'] = 'invalid_or_unapproved_inventory_shard'
    except MultiShardError as error:
        result['failure_category'] = str(error)
    except output_writer.OutputWriterError as error:
        result['failure_category'] = error.category
    except Exception:
        result['failure_category'] = 'shard_ingestion_failed'
    finally:
        if reader is not None:
            reader.close()
    print(json.dumps(result, sort_keys=True))
    return 0 if result['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

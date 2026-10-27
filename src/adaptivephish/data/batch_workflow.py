"""Resumable orchestration around the existing safe single-shard workflow."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import shutil

from adaptivephish.data import output_writer
from adaptivephish.data.multi_shard import (
    MultiShardError, ingest_reader, local_output_path, make_reader,
    preflight_reader,
)
from adaptivephish.data.shard_inventory import (
    DATASET, INVENTORY_PATH, REVISION, SELECTED_COLUMNS, load_inventory,
)


PLAN_PATH = output_writer.WORKSPACE_ROOT / 'configs' / 'phreshphish_batch_plan.json'
IGNORE_PATH = output_writer.APPROVED_DIRECTORY / '.gitignore'
MAX_APPLICATION_BYTES = 50_000_000
MAX_SESSION_REQUESTS = 10_000
MAX_SHARDS = 77


class BatchError(Exception):
    """Fixed batch failure category without record or transport values."""


def _atomic_json(path, document, replace=os.replace, validator=None):
    destination = Path(path)
    temporary = None
    try:
        handle, temporary = tempfile.mkstemp(
            prefix=f'.{destination.stem}-', suffix='.json.tmp',
            dir=destination.parent,
        )
        with os.fdopen(handle, 'w', encoding='utf-8', newline='\n') as stream:
            json.dump(document, stream, indent=2, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        json.loads(Path(temporary).read_text(encoding='utf-8'))
        if validator is not None:
            validator(temporary)
        replace(temporary, destination)
        temporary = None
    except Exception:
        raise BatchError('atomic_write_failed') from None
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def select_shards(inventory, split='all', keys=None, max_shards=1):
    if split not in {'all', 'train', 'test'} or not 1 <= max_shards <= MAX_SHARDS:
        raise BatchError('invalid_selection')
    requested = None if keys is None else tuple(keys)
    if requested is not None and (not requested or len(set(requested)) != len(requested)):
        raise BatchError('invalid_selection')
    eligible = [shard for shard in inventory.shards
                if split == 'all' or shard.split == split]
    if requested is not None:
        by_key = {shard.key: shard for shard in eligible}
        if any(key not in by_key for key in requested):
            raise BatchError('invalid_selection')
        eligible = [by_key[key] for key in requested]
    else:
        eligible = [shard for shard in eligible
                    if shard.verification_status == 'metadata_discovered']
    return tuple(eligible[:max_shards])


def preflight_batch(shards, session_byte_limit, session_request_limit,
                    per_shard_byte_limit, runtime_limit_seconds,
                    reader_factory=make_reader, clock=time.monotonic):
    if (not shards or not 1 <= len(shards) <= MAX_SHARDS
            or not 1 <= per_shard_byte_limit <= MAX_APPLICATION_BYTES
            or not 1 <= session_byte_limit <= MAX_APPLICATION_BYTES * MAX_SHARDS
            or not 1 <= session_request_limit <= MAX_SESSION_REQUESTS
            or runtime_limit_seconds <= 0):
        raise BatchError('invalid_limits')
    if sum(shard.maximum_http_requests for shard in shards) > session_request_limit:
        raise BatchError('session_request_limit_exceeded')
    if shutil.disk_usage(output_writer.WORKSPACE_ROOT).free < sum(s.expected_file_size for s in shards):
        raise BatchError('insufficient_disk_space')
    started = clock()
    used_bytes = used_requests = 0
    entries = []
    for shard in shards:
        if clock() - started >= runtime_limit_seconds:
            raise BatchError('runtime_limit_exceeded')
        if used_requests + 3 > session_request_limit:
            raise BatchError('session_request_limit_exceeded')
        remaining = session_byte_limit - used_bytes
        if remaining < 12:
            raise BatchError('session_byte_limit_exceeded')
        reader = reader_factory(shard, budget=min(per_shard_byte_limit, remaining))
        reader.deadline, reader.clock = started + runtime_limit_seconds, clock
        try:
            _, plan = preflight_reader(reader, shard, remaining)
        except MultiShardError as error:
            raise BatchError(str(error)) from None
        finally:
            used_bytes += reader.body_bytes
            used_requests += reader.request_count
            reader.close()
        if used_bytes > session_byte_limit:
            raise BatchError('session_byte_limit_exceeded')
        if used_requests > session_request_limit:
            raise BatchError('session_request_limit_exceeded')
        entries.append({
            'key': shard.key, 'split': shard.split, 'path': shard.path,
            'expected_file_size': shard.expected_file_size,
            'rows': plan.rows, 'row_groups': plan.row_groups,
            'footer_bytes': plan.footer_bytes,
            'selected_compressed_bytes': plan.selected_bytes,
            'estimated_application_body_bytes': plan.estimated_body_bytes,
            'planned_requests': plan.planned_requests,
            'approved_http_request_budget': shard.maximum_http_requests,
        })
    return {
        'schema_version': 1, 'status': 'pending_review',
        'dataset': DATASET, 'revision': REVISION,
        'selected_columns': list(SELECTED_COLUMNS),
        'limits': {
            'shard_count': len(shards),
            'per_shard_application_bytes': per_shard_byte_limit,
            'session_application_bytes': session_byte_limit,
            'session_http_requests': session_request_limit,
            'wall_clock_seconds': runtime_limit_seconds,
        },
        'metadata_application_body_bytes': used_bytes,
        'metadata_http_requests': used_requests,
        'shards': entries,
    }


def write_pending_plan(plan, path=PLAN_PATH):
    if plan.get('status') != 'pending_review':
        raise BatchError('invalid_batch_plan')
    destination = Path(path)
    if destination.exists():
        raise BatchError('batch_plan_exists')
    _atomic_json(destination, plan,
                 validator=lambda candidate: load_plan(candidate,
                                                       'pending_review'))


def _approval_digest(document):
    candidate = dict(document)
    candidate.pop('approval_id', None)
    encoded = json.dumps(candidate, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(encoded).hexdigest()


def load_plan(path=PLAN_PATH, required_status=None):
    try:
        document = json.loads(Path(path).read_text(encoding='utf-8'))
        entries = document['shards']
        limits = document.get('limits', {})
        if (document.get('schema_version') != 1
                or document.get('dataset') != DATASET
                or document.get('revision') != REVISION
                or document.get('selected_columns') != list(SELECTED_COLUMNS)
                or not isinstance(entries, list) or not entries
                or len(entries) != document.get('limits', {}).get('shard_count')
                or len({entry.get('key') for entry in entries}) != len(entries)
                or document.get('status') not in {'pending_review', 'approved'}
                or (required_status is not None
                    and document['status'] != required_status)
                or type(limits.get('shard_count')) is not int
                or not 1 <= limits['shard_count'] <= MAX_SHARDS
                or type(limits.get('per_shard_application_bytes')) is not int
                or not 1 <= limits['per_shard_application_bytes'] <= MAX_APPLICATION_BYTES
                or type(limits.get('session_application_bytes')) is not int
                or not 1 <= limits['session_application_bytes'] <= MAX_APPLICATION_BYTES * MAX_SHARDS
                or type(limits.get('session_http_requests')) is not int
                or not 1 <= limits['session_http_requests'] <= MAX_SESSION_REQUESTS
                or not isinstance(limits.get('wall_clock_seconds'), (int, float))
                or limits['wall_clock_seconds'] <= 0):
            raise ValueError
        required = {
            'key', 'split', 'path', 'expected_file_size', 'rows', 'row_groups',
            'footer_bytes', 'selected_compressed_bytes',
            'estimated_application_body_bytes', 'planned_requests',
        }
        optional = {'approved_http_request_budget'}
        for entry in entries:
            numeric = required - {'key', 'split', 'path'}
            if (not required <= set(entry)
                    or not (set(entry) - required) <= optional
                    or entry['key'] != Path(entry['path']).stem
                    or entry['split'] not in {'train', 'test'}
                    or not entry['path'].startswith(f"data/{entry['split']}-")
                    or any(type(entry[name]) is not int or entry[name] < 1
                           for name in numeric)
                    or ('approved_http_request_budget' in entry
                        and (type(entry['approved_http_request_budget']) is not int
                             or entry['approved_http_request_budget'] < 1))):
                raise ValueError
        if (any(entry['estimated_application_body_bytes']
                > limits['per_shard_application_bytes'] for entry in entries)
                or sum(entry['estimated_application_body_bytes'] for entry in entries)
                > limits['session_application_bytes']
                or sum(entry['planned_requests'] for entry in entries)
                > limits['session_http_requests']
                or sum(entry.get('approved_http_request_budget',
                                 entry['planned_requests']) for entry in entries)
                > limits['session_http_requests']):
            raise ValueError
        if document['status'] == 'approved':
            approval = document.get('approval_id')
            if not isinstance(approval, str) or approval != _approval_digest(document):
                raise ValueError
        return document
    except BatchError:
        raise
    except Exception:
        raise BatchError('invalid_batch_plan') from None


def _write_ignore_rules(keys, path=IGNORE_PATH):
    ignore = Path(path)
    existing = ignore.read_text(encoding='utf-8').splitlines()
    additions = []
    for key in keys:
        additions.extend((f'/{key}-url-metadata.parquet',
                          f'/{key}-url-metadata.manifest.json'))
    lines = existing + [line for line in additions if line not in existing]
    temporary = ignore.with_name(f'.{ignore.name}.batch.tmp')
    try:
        temporary.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        os.replace(temporary, ignore)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise BatchError('ignore_publication_failed') from None


def approve_plan(path=PLAN_PATH, inventory_path=INVENTORY_PATH,
                 ignore_path=IGNORE_PATH):
    plan = load_plan(path, 'pending_review')
    inventory = load_inventory(inventory_path)
    by_key = {shard.key: shard for shard in inventory.shards}
    raw = json.loads(Path(inventory_path).read_text(encoding='utf-8'))
    raw_by_path = {item['path']: item for item in raw['shards']}
    for entry in plan['shards']:
        shard = by_key.get(entry['key'])
        if (shard is None or shard.path != entry['path']
                or shard.split != entry['split']
                or shard.expected_file_size != entry['expected_file_size']):
            raise BatchError('inventory_plan_mismatch')
        if entry['planned_requests'] > shard.maximum_http_requests:
            raise BatchError('inventory_plan_mismatch')
        if entry.get('approved_http_request_budget', shard.maximum_http_requests) \
                != shard.maximum_http_requests:
            raise BatchError('inventory_plan_mismatch')
        item = raw_by_path[entry['path']]
        if shard.verification_status == 'footer_verified':
            expected = (shard.expected_rows, shard.expected_row_groups,
                        shard.approved_estimated_body_bytes)
            proposed = (entry['rows'], entry['row_groups'],
                        entry['estimated_application_body_bytes'])
            if expected != proposed:
                raise BatchError('inventory_plan_mismatch')
        item.update(
            expected_rows=entry['rows'], expected_row_groups=entry['row_groups'],
            application_body_budget=entry['estimated_application_body_bytes'],
            approved_estimated_body_bytes=entry['estimated_application_body_bytes'],
            verification_status='footer_verified',
        )
    approved = dict(plan)
    approved['status'] = 'approved'
    approved['approval_id'] = _approval_digest(approved)
    _write_ignore_rules([entry['key'] for entry in plan['shards']], ignore_path)
    _atomic_json(inventory_path, raw, validator=load_inventory)
    _atomic_json(path, approved,
                 validator=lambda candidate: load_plan(candidate, 'approved'))
    return approved


def ingest_approved_batch(plan, approval_id, session_byte_limit,
                          session_request_limit, runtime_limit_seconds,
                          verifier, reader_factory=make_reader,
                          clock=time.monotonic):
    if (plan.get('status') != 'approved'
            or approval_id != plan.get('approval_id')
            or approval_id != _approval_digest(plan)
            or not 1 <= session_byte_limit <= MAX_APPLICATION_BYTES * MAX_SHARDS
            or not 1 <= session_request_limit <= MAX_SESSION_REQUESTS
            or runtime_limit_seconds <= 0
            or session_byte_limit > plan.get('limits', {}).get(
                'session_application_bytes', 0)
            or session_request_limit > plan.get('limits', {}).get(
                'session_http_requests', 0)
            or runtime_limit_seconds > plan.get('limits', {}).get(
                'wall_clock_seconds', 0)):
        raise BatchError('approval_or_limits_invalid')
    inventory = load_inventory()
    started = clock()
    used_bytes = used_requests = 0
    completed, skipped = [], []
    for entry in plan['shards']:
        if clock() - started >= runtime_limit_seconds:
            raise BatchError('runtime_limit_exceeded')
        shard = inventory.get(entry['key'])
        if (shard.path != entry['path'] or shard.split != entry['split']
                or shard.expected_file_size != entry['expected_file_size']
                or shard.expected_rows != entry['rows']
                or shard.expected_row_groups != entry['row_groups']
                or shard.approved_estimated_body_bytes
                != entry['estimated_application_body_bytes']):
            raise BatchError('inventory_plan_mismatch')
        estimate = entry['estimated_application_body_bytes']
        # planned_requests counts logical ranges. RangeFile.request_count also
        # counts redirect hops. Reserve the approved per-shard hard request
        # limit so redirects can never exceed the session authorization.
        requests = entry.get('approved_http_request_budget',
                             shard.maximum_http_requests)
        if requests != shard.maximum_http_requests:
            raise BatchError('inventory_plan_mismatch')
        if estimate > session_byte_limit - used_bytes:
            raise BatchError('session_byte_limit_exceeded')
        if requests > session_request_limit - used_requests:
            raise BatchError('session_request_limit_exceeded')
        output = local_output_path(shard)
        sidecar = output_writer.manifest_path(output)
        if output.exists() or sidecar.exists():
            try:
                document = output_writer.verify_completed_output(output)
                if document.get('source') != shard.source:
                    raise BatchError('existing_output_not_replaceable')
                skipped.append(shard.key)
                continue
            except output_writer.OutputWriterError:
                raise BatchError('existing_output_not_replaceable') from None
        output_writer._require_git_ignored(output, sidecar)
        if shutil.disk_usage(output_writer.WORKSPACE_ROOT).free < shard.expected_file_size:
            raise BatchError('insufficient_disk_space')
        reader = reader_factory(shard)
        reader.deadline, reader.clock = started + runtime_limit_seconds, clock
        try:
            ingest_reader(reader, shard, output,
                          session_byte_limit - used_bytes,
                          approved_estimate=estimate)
        except MultiShardError as error:
            raise BatchError(str(error)) from None
        finally:
            used_bytes += reader.body_bytes
            used_requests += reader.request_count
            reader.close()
        if used_bytes > session_byte_limit:
            raise BatchError('session_byte_limit_exceeded')
        if used_requests > session_request_limit:
            raise BatchError('session_request_limit_exceeded')
        if clock() - started >= runtime_limit_seconds:
            raise BatchError('runtime_limit_exceeded')
        completed.append(shard.key)
        verification = verifier()
        if not verification.get('success'):
            raise BatchError('aggregate_verification_failed')
    return {
        'success': True, 'completed_shards': completed,
        'skipped_verified_shards': skipped,
        'application_body_bytes': used_bytes,
        'http_request_count': used_requests,
        'remaining_plan_shards': len(plan['shards']) - len(completed) - len(skipped),
    }

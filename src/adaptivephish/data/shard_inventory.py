"""Strict inventory contract for revision-pinned PhreshPhish shards."""

from dataclasses import dataclass
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile

from adaptivephish.data.output_writer import WORKSPACE_ROOT


DATASET = 'phreshphish/phreshphish'
REVISION = 'eca603e0ecd4f698f8d4d226c088659e72681239'
SELECTED_COLUMNS = ('sha256', 'url', 'label', 'date')
INVENTORY_PATH = WORKSPACE_ROOT / 'configs' / 'phreshphish_inventory.json'
_SHARD_PATH = re.compile(r'data/(train|test)-([0-9]{3})\.parquet')


class InventoryError(Exception):
    """Sanitized inventory failure."""


@dataclass(frozen=True)
class ShardSpec:
    split: str
    path: str
    expected_file_size: int
    expected_rows: int | None
    expected_row_groups: int | None
    application_body_budget: int | None
    maximum_http_requests: int
    verification_status: str = 'footer_verified'
    approved_estimated_body_bytes: int | None = None

    @property
    def key(self):
        return PurePosixPath(self.path).stem

    @property
    def source(self):
        return {'dataset': DATASET, 'revision': REVISION,
                'split': self.split, 'shard': self.path}

    @property
    def url(self):
        return f'https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{self.path}'


@dataclass(frozen=True)
class ShardInventory:
    status: str
    expected_official_rows: int
    expected_official_shards: int
    shards: tuple

    def get(self, key):
        matches = [shard for shard in self.shards if shard.key == key]
        if len(matches) != 1:
            raise InventoryError('unapproved_shard')
        return matches[0]


def load_inventory(path=INVENTORY_PATH):
    try:
        document = json.loads(Path(path).read_text(encoding='utf-8'))
        if (document.get('schema_version') != 1
                or document.get('dataset') != DATASET
                or document.get('selected_columns') != list(SELECTED_COLUMNS)
                or document.get('expected_official_rows') != 666_315
                or document.get('expected_official_shards') != 77
                or document.get('inventory_status') not in {
                    'partial_verified', 'metadata_complete', 'complete_verified'
                }
                or document.get('provenance', {}).get('dataset_revision') != REVISION
                or not isinstance(document.get('shards'), list)):
            raise ValueError
        shards = []
        seen = set()
        for item in document['shards']:
            legacy_fields = {
                'split', 'path', 'expected_file_size', 'expected_rows',
                'expected_row_groups', 'application_body_budget',
                'maximum_http_requests',
            }
            optional_fields = {'verification_status',
                               'approved_estimated_body_bytes'}
            if (not legacy_fields <= set(item)
                    or not (set(item) - legacy_fields) <= optional_fields):
                raise ValueError
            status = item.get('verification_status', 'footer_verified')
            approved_estimate = item.get('approved_estimated_body_bytes')
            match = _SHARD_PATH.fullmatch(item['path'])
            if (not match or match.group(1) != item['split']
                    or item['path'] in seen
                    or type(item['expected_file_size']) is not int
                    or item['expected_file_size'] < 12
                    or (item['expected_rows'] is not None
                        and (type(item['expected_rows']) is not int
                             or item['expected_rows'] < 1))
                    or (item['expected_row_groups'] is not None
                        and (type(item['expected_row_groups']) is not int
                             or item['expected_row_groups'] < 1))
                    or status not in {'metadata_discovered', 'footer_verified'}
                    or (status == 'metadata_discovered'
                        and (item['expected_rows'] is not None
                             or item['expected_row_groups'] is not None
                             or item['application_body_budget'] is not None))
                    or (status == 'footer_verified'
                        and (type(item['expected_rows']) is not int
                             or type(item['expected_row_groups']) is not int
                             or type(item['application_body_budget']) is not int))
                    or (item['application_body_budget'] is not None
                        and not 1 <= item['application_body_budget'] <= 50_000_000)
                    or (approved_estimate is not None
                        and (type(approved_estimate) is not int
                             or approved_estimate < 12
                             or (item['application_body_budget'] is not None
                                 and approved_estimate
                                 > item['application_body_budget'])))
                    or type(item['maximum_http_requests']) is not int
                    or not 3 <= item['maximum_http_requests'] <= 256):
                raise ValueError
            seen.add(item['path'])
            normalized = dict(item)
            normalized.setdefault('verification_status', status)
            normalized.setdefault('approved_estimated_body_bytes',
                                  approved_estimate)
            shards.append(ShardSpec(**normalized))
        if not shards:
            raise ValueError
        if document['inventory_status'] == 'complete_verified':
            if (len(shards) != document['expected_official_shards']
                    or any(shard.expected_rows is None for shard in shards)
                    or sum(shard.expected_rows for shard in shards)
                    != document['expected_official_rows']):
                raise ValueError
        return ShardInventory(
            document['inventory_status'], document['expected_official_rows'],
            document['expected_official_shards'], tuple(shards)
        )
    except Exception:
        raise InventoryError('invalid_shard_inventory') from None


def merge_discovered_inventory(discovery, path=INVENTORY_PATH,
                               replace=os.replace):
    """Atomically merge one validated live discovery into the inventory."""
    try:
        if (not isinstance(discovery, dict)
                or discovery.get('success') is not True
                or discovery.get('dataset') != DATASET
                or discovery.get('revision') != REVISION
                or discovery.get('shard_count') != 77
                or discovery.get('split_counts') != {'train': 56, 'test': 21}
                or not isinstance(discovery.get('shards'), list)):
            raise ValueError
        discovered = {}
        for item in discovery['shards']:
            if (not isinstance(item, dict)
                    or set(item) != {'path', 'size', 'split'}
                    or not _SHARD_PATH.fullmatch(item['path'])
                    or _SHARD_PATH.fullmatch(item['path']).group(1) != item['split']
                    or type(item['size']) is not int or item['size'] < 12
                    or item['path'] in discovered):
                raise ValueError
            discovered[item['path']] = item
        expected_paths = {
            *(f'data/train-{index:03}.parquet' for index in range(56)),
            *(f'data/test-{index:03}.parquet' for index in range(21)),
        }
        if set(discovered) != expected_paths:
            raise ValueError

        destination = Path(path)
        current = json.loads(destination.read_text(encoding='utf-8'))
        load_inventory(destination)
        existing = {item['path']: item for item in current['shards']}
        merged = []
        for shard_path in sorted(discovered):
            remote = discovered[shard_path]
            if shard_path in existing:
                saved = dict(existing[shard_path])
                if saved['expected_file_size'] != remote['size']:
                    raise InventoryError('inventory_merge_conflict')
                saved.setdefault('verification_status', 'footer_verified')
                merged.append(saved)
            else:
                merged.append({
                    'split': remote['split'],
                    'path': shard_path,
                    'expected_file_size': remote['size'],
                    'expected_rows': None,
                    'expected_row_groups': None,
                    'application_body_budget': None,
                    'maximum_http_requests': 32,
                    'verification_status': 'metadata_discovered',
                    'approved_estimated_body_bytes': None,
                })
        all_footer_verified = all(
            item['verification_status'] == 'footer_verified'
            for item in merged
        )
        fully_verified = (all_footer_verified
                          and all(type(item['expected_rows']) is int
                                  for item in merged)
                          and sum(item['expected_rows'] for item in merged)
                          == 666_315)
        document = {
            'dataset': DATASET,
            'expected_official_rows': 666_315,
            'expected_official_shards': 77,
            'inventory_status': ('complete_verified' if fully_verified
                                 else 'metadata_complete'),
            'provenance': {
                'dataset_revision': REVISION,
                'evidence': ('Official revision metadata; footer-derived fields '
                             'are preserved only where independently verified'),
                'generated_by_live_inventory': True,
            },
            'schema_version': 1,
            'selected_columns': list(SELECTED_COLUMNS),
            'shards': merged,
        }
        rendered = json.dumps(document, indent=2, sort_keys=True) + '\n'
        # Validate the exact candidate before publication.
        temporary = None
        try:
            handle, temporary = tempfile.mkstemp(
                prefix='.phreshphish-inventory-', suffix='.json.tmp',
                dir=destination.parent,
            )
            with os.fdopen(handle, 'w', encoding='utf-8', newline='\n') as stream:
                stream.write(rendered)
                stream.flush()
                os.fsync(stream.fileno())
            load_inventory(temporary)
            if destination.read_text(encoding='utf-8') == rendered:
                Path(temporary).unlink()
                temporary = None
                return document
            replace(temporary, destination)
            temporary = None
            return document
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)
    except InventoryError:
        raise
    except Exception:
        raise InventoryError('inventory_persistence_failed') from None

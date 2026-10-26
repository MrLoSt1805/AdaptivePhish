"""Verified loading and deterministic leakage-resistant random splitting."""

from dataclasses import dataclass
import random

import pyarrow.parquet as pq

from adaptivephish.data import output_writer
from adaptivephish.data.source_config import load_source_config


RANDOM_SEED = 42
SPLIT_RATIOS = (0.70, 0.15, 0.15)
DATASET_PATH = (
    output_writer.WORKSPACE_ROOT / 'data' / 'raw' / 'phreshphish'
    / 'train-001-url-metadata.parquet'
)


class DataPreparationError(Exception):
    """Fixed, sanitized preparation failure."""


@dataclass(frozen=True)
class Partition:
    urls: tuple
    labels: tuple
    dates: tuple


@dataclass(frozen=True)
class PreparedData:
    train: Partition
    validation: Partition
    test: Partition
    seed: int = RANDOM_SEED
    ratios: tuple = SPLIT_RATIOS


def _partition(rows):
    return Partition(
        tuple(row['url'] for row in rows),
        tuple(0 if row['label'] == 'benign' else 1 for row in rows),
        tuple(row['date'] for row in rows),
    )


def split_rows(rows, seed=RANDOM_SEED, ratios=SPLIT_RATIOS):
    """Group exact URLs, then make deterministic approximately stratified splits."""
    if (len(ratios) != 3 or any(value <= 0 for value in ratios)
            or abs(sum(ratios) - 1.0) > 1e-9):
        raise DataPreparationError('invalid_split_configuration')
    grouped = {}
    for row in rows:
        try:
            url, label = row['url'], row['label']
        except (KeyError, TypeError):
            raise DataPreparationError('invalid_dataset_rows') from None
        if not isinstance(url, str) or label not in ('benign', 'phish'):
            raise DataPreparationError('invalid_dataset_rows')
        group = grouped.setdefault(url, [])
        if group and group[0]['label'] != label:
            raise DataPreparationError('conflicting_url_labels')
        group.append(row)

    buckets = [[], [], []]
    for label_index, label in enumerate(('benign', 'phish')):
        groups = [group for group in grouped.values() if group[0]['label'] == label]
        random.Random(seed + label_index).shuffle(groups)
        total = sum(len(group) for group in groups)
        targets = (total * ratios[0], total * ratios[1], total * ratios[2])
        counts = [0, 0, 0]
        for group in groups:
            choices = [
                (counts[index] / max(targets[index], 1), index)
                for index in range(3)
            ]
            destination = min(choices)[1]
            buckets[destination].extend(group)
            counts[destination] += len(group)
    for index, bucket in enumerate(buckets):
        random.Random(seed + 100 + index).shuffle(bucket)
    if any(not bucket for bucket in buckets):
        raise DataPreparationError('empty_split')
    return PreparedData(*(_partition(bucket) for bucket in buckets), seed, ratios)


def load_verified_data():
    """Verify Phase 2 output and return split data without predictive metadata leakage."""
    try:
        config = load_source_config()
        manifest = output_writer.verify_completed_output(DATASET_PATH)
        expected_source = {
            'dataset': config.dataset, 'revision': config.revision,
            'split': config.split, 'shard': config.shard,
        }
        if (manifest.get('source') != expected_source
                or manifest.get('row_count') != config.expected_rows
                or manifest.get('selected_columns') != list(config.selected_columns)):
            raise DataPreparationError('source_verification_failed')
        table = pq.read_table(DATASET_PATH, columns=['url', 'label', 'date'])
        return split_rows(table.to_pylist())
    except DataPreparationError:
        raise
    except Exception:
        raise DataPreparationError('source_verification_failed') from None


def load_verified_expanded_rows(inventory):
    """Future multi-shard loader; never changes the existing Phase 3 default."""
    if inventory.status != 'complete_verified':
        raise DataPreparationError('expanded_inventory_incomplete')
    from adaptivephish.data.multi_shard import local_output_path
    partitions = {'train': [], 'test': []}
    try:
        for shard in inventory.shards:
            path = local_output_path(shard)
            document = output_writer.verify_completed_output(path)
            if document.get('source') != shard.source:
                raise DataPreparationError('source_verification_failed')
            rows = pq.read_table(path, columns=['url', 'label', 'date']).to_pylist()
            partitions[shard.split].extend(rows)
        return partitions
    except DataPreparationError:
        raise
    except Exception:
        raise DataPreparationError('source_verification_failed') from None

"""Fail-closed multi-row-group preflight and bounded shard ingestion."""

from dataclasses import dataclass
from pathlib import Path

from adaptivephish.data import remote_parquet
from adaptivephish.data.output_writer import (
    APPROVED_DIRECTORY, OutputWriterError, manifest_path,
    verify_completed_output, write_validated_parquet,
)
from adaptivephish.data.shard_inventory import SELECTED_COLUMNS, ShardSpec
from adaptivephish.data.validation import normalize_batches


METADATA_REQUESTS = 3
BATCH_SIZE = 1_024


class MultiShardError(Exception):
    """Sanitized multi-shard failure."""


@dataclass(frozen=True)
class ShardPlan:
    rows: int
    row_groups: int
    footer_bytes: int
    selected_bytes: int
    estimated_body_bytes: int
    selected_ranges: tuple
    planned_requests: int


def local_output_path(shard):
    return APPROVED_DIRECTORY / f'{shard.key}-url-metadata.parquet'


def make_reader(shard, transport=remote_parquet.https_response, budget=None):
    effective_budget = shard.application_body_budget if budget is None else budget
    if effective_budget is None:
        raise MultiShardError('footer_preflight_required')
    return remote_parquet.RangeFile(
        url=shard.url, size=shard.expected_file_size,
        budget=effective_budget, transport=transport,
        request_limit=shard.maximum_http_requests,
    )


def _column_interval(column, footer_start):
    if column.file_path or column.total_compressed_size <= 0:
        raise MultiShardError('invalid_parquet_metadata')
    offsets = [column.data_page_offset]
    if column.has_dictionary_page:
        offsets.append(column.dictionary_page_offset)
    if any(offset is None or offset < 4 for offset in offsets):
        raise MultiShardError('invalid_parquet_metadata')
    start = min(offsets)
    end = start + column.total_compressed_size
    if not 4 <= start < end <= footer_start:
        raise MultiShardError('invalid_parquet_metadata')
    return start, end


def preflight_reader(reader, shard, session_remaining):
    """Fetch only header/trailer/footer and authorize no column data yet."""
    try:
        metadata, footer_start = remote_parquet.inspect_footer(reader)
        if (shard.expected_rows is not None
                and metadata.num_rows != shard.expected_rows):
            raise MultiShardError('source_dimensions_mismatch')
        if (shard.expected_row_groups is not None
                and metadata.num_row_groups != shard.expected_row_groups):
            raise MultiShardError('source_dimensions_mismatch')
        schema_names = metadata.schema.names
        if not all(name in schema_names for name in SELECTED_COLUMNS):
            raise MultiShardError('source_schema_mismatch')
        selected, forbidden, all_ranges = [], [], []
        for group_index in range(metadata.num_row_groups):
            group = metadata.row_group(group_index)
            for column_index in range(group.num_columns):
                column = group.column(column_index)
                interval = _column_interval(column, footer_start)
                all_ranges.append(interval)
                (selected if column.path_in_schema in SELECTED_COLUMNS
                 else forbidden).append(interval)
        ordered = sorted(all_ranges)
        if any(left[1] > right[0] for left, right in zip(ordered, ordered[1:])):
            raise MultiShardError('overlapping_column_chunks')
        if any(a < d and c < b for a, b in selected for c, d in forbidden):
            raise MultiShardError('forbidden_column_overlap')
        if len(selected) != metadata.num_row_groups * len(SELECTED_COLUMNS):
            raise MultiShardError('source_schema_mismatch')
        selected_bytes = sum(end - start for start, end in selected)
        footer_bytes = reader.size - 8 - footer_start
        estimated = 12 + footer_bytes + selected_bytes
        planned_requests = METADATA_REQUESTS + len(selected)
        approved_budget = (shard.application_body_budget
                           if shard.application_body_budget is not None
                           else reader.budget)
        if (estimated > approved_budget
                or estimated > session_remaining):
            raise MultiShardError('budget_precheck')
        if planned_requests > shard.maximum_http_requests:
            raise MultiShardError('request_limit_precheck')
        return metadata, ShardPlan(
            metadata.num_rows, metadata.num_row_groups, footer_bytes,
            selected_bytes, estimated, tuple(selected), planned_requests,
        )
    except MultiShardError:
        raise
    except remote_parquet.SafetyError as error:
        raise MultiShardError(remote_parquet._failure_category(error.category)) from None
    except Exception:
        raise MultiShardError('metadata_preflight_failed') from None


def ingest_reader(reader, shard, output_path, session_remaining,
                  approved_estimate=None):
    """Ingest one explicitly approved inventory shard; skip verified output."""
    if (shard.approved_estimated_body_bytes is not None
            and approved_estimate != shard.approved_estimated_body_bytes):
        reader.close()
        raise MultiShardError('preflight_approval_mismatch')
    output = Path(output_path)
    sidecar = manifest_path(output)
    if output.exists() or sidecar.exists():
        try:
            document = verify_completed_output(output)
            if document.get('source') == shard.source:
                reader.close()
                return {'status': 'verified_existing', 'manifest': document}
        except OutputWriterError:
            pass
        reader.close()
        raise MultiShardError('existing_output_not_replaceable')
    try:
        metadata, plan = preflight_reader(reader, shard, session_remaining)
        if (approved_estimate != plan.estimated_body_bytes
                or (shard.approved_estimated_body_bytes is not None
                    and plan.estimated_body_bytes
                    != shard.approved_estimated_body_bytes)):
            raise MultiShardError('preflight_approval_mismatch')
        reader.allowed.extend(plan.selected_ranges)
        for interval in plan.selected_ranges:
            reader.fetch(*interval)
        parquet = remote_parquet.pq.ParquetFile(
            reader, metadata=metadata, pre_buffer=False, buffer_size=0,
            arrow_extensions_enabled=False,
        )
        batches = parquet.iter_batches(
            batch_size=BATCH_SIZE,
            row_groups=list(range(metadata.num_row_groups)),
            columns=list(SELECTED_COLUMNS), use_threads=False,
        )
        try:
            normalized = normalize_batches(batches)
        finally:
            batches.close()
            parquet.close()
        if not reader.accounting_complete:
            raise MultiShardError('transport_unmetered')
        document = write_validated_parquet(
            normalized, output, source=shard.source, expected_rows=plan.rows
        )
        if verify_completed_output(output) != document:
            raise MultiShardError('completion_verification_failed')
        return {'status': 'ingested', 'manifest': document,
                'application_body_bytes': reader.body_bytes}
    except MultiShardError:
        raise
    except OutputWriterError as error:
        raise MultiShardError(error.category) from None
    except Exception:
        raise MultiShardError('shard_ingestion_failed') from None
    finally:
        reader.close()

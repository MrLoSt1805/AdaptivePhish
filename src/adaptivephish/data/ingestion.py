"""Coordinate approved remote reads and fail-closed local publication."""

from adaptivephish.data import remote_parquet
from adaptivephish.data.output_writer import (
    OutputWriterError,
    verify_completed_output,
    write_validated_parquet,
)
from adaptivephish.data.source_config import SourceConfigError, load_source_config
from adaptivephish.data.validation import ValidationError, normalize_batches


BATCH_SIZE = 1_024
EXPECTED_REQUEST_LIMIT = 32


class IngestionError(Exception):
    """A fixed, sanitized ingestion failure without source record values."""

    def __init__(self, category):
        self.category = category
        super().__init__(category)


def _approved_url(config):
    return (
        'https://huggingface.co/datasets/'
        f'{config.dataset}/resolve/{config.revision}/{config.shard}'
    )


def _load_approved_contract():
    try:
        config = load_source_config()
    except SourceConfigError:
        raise IngestionError('source_configuration_failed') from None
    if (remote_parquet.URL != _approved_url(config)
            or remote_parquet.SIZE != config.expected_file_size
            or remote_parquet.COLUMNS != config.selected_columns
            or config.application_body_budget > remote_parquet.BUDGET
            or config.maximum_http_requests != EXPECTED_REQUEST_LIMIT):
        raise IngestionError('source_contract_mismatch')
    return config


def _read_selected_batches(reader, expected_row_groups, expected_rows):
    """Read approved chunks through an already constructed RangeFile."""
    metadata, footer_start = remote_parquet.inspect_footer(reader)
    if (metadata.num_row_groups != expected_row_groups
            or metadata.num_rows != expected_rows):
        raise remote_parquet.SafetyError('Unexpected source dimensions')
    selected = remote_parquet.plan_first_group(reader, metadata, footer_start)
    for name in remote_parquet.COLUMNS:
        reader.fetch(*selected[name])

    parquet = remote_parquet.pq.ParquetFile(
        reader,
        metadata=metadata,
        pre_buffer=False,
        buffer_size=0,
        arrow_extensions_enabled=False,
    )
    batches = parquet.iter_batches(
        batch_size=BATCH_SIZE,
        row_groups=[0],
        columns=list(remote_parquet.COLUMNS),
        use_threads=False,
    )
    try:
        materialized = list(batches)
    finally:
        batches.close()
        parquet.close()
    if sum(batch.num_rows for batch in materialized) != expected_rows:
        raise remote_parquet.SafetyError('Unexpected source dimensions')
    return materialized


def _publish(reader, output_path, expected_row_groups, expected_rows,
             approved_shard):
    try:
        batches = normalize_batches(_read_selected_batches(
            reader, expected_row_groups, expected_rows
        ))
        if not reader.accounting_complete:
            raise remote_parquet.SafetyError('transport_unmetered')
        document = write_validated_parquet(
            batches, output_path, approved_shard=approved_shard
        )
        verified = verify_completed_output(output_path)
        if verified != document:
            raise IngestionError('completion_verification_failed')
        return verified
    except IngestionError:
        raise
    except remote_parquet.SafetyError as error:
        category = remote_parquet._failure_category(error.category)
        raise IngestionError(category) from None
    except OutputWriterError as error:
        raise IngestionError(error.category) from None
    except ValidationError as error:
        raise IngestionError(error.category) from None
    except Exception:
        raise IngestionError('ingestion_failed') from None
    finally:
        reader.close()


def ingest_approved_shard(output_path):
    """Ingest only the immutable approved shard using its production limits."""
    config = _load_approved_contract()
    reader = remote_parquet.RangeFile(
        url=remote_parquet.URL,
        size=config.expected_file_size,
        budget=config.application_body_budget,
    )
    return _publish(
        reader,
        output_path,
        expected_row_groups=config.expected_row_groups,
        expected_rows=config.expected_rows,
        approved_shard=True,
    )


def _ingest_with_offline_reader(reader, output_path, expected_rows,
                                expected_row_groups=1):
    """Test-only path for a pinned-URL RangeFile with a local fake transport.

    It cannot select another remote source: the reader must retain the exact
    approved URL, and the real HTTPS transport is explicitly forbidden.
    Small row counts are allowed only through this private offline function.
    """
    if (type(reader) is not remote_parquet.RangeFile
            or reader.url != remote_parquet.URL
            or reader.transport is remote_parquet.https_response
            or type(expected_rows) is not int or expected_rows < 1
            or type(expected_row_groups) is not int or expected_row_groups < 1):
        try:
            reader.close()
        except Exception:
            pass
        raise IngestionError('offline_test_source_rejected')
    return _publish(
        reader,
        output_path,
        expected_row_groups=expected_row_groups,
        expected_rows=expected_rows,
        approved_shard=False,
    )

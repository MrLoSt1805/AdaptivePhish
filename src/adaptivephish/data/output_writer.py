"""Safely publish validated URL metadata as local Parquet plus a manifest."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from adaptivephish.data.source_config import load_source_config
from adaptivephish.data.validation import COLUMNS, ValidationError, validate_batches


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
APPROVED_DIRECTORY = WORKSPACE_ROOT / 'data' / 'raw' / 'phreshphish'
EXPECTED_SCHEMA = pa.schema([
    ('sha256', pa.string()),
    ('url', pa.string()),
    ('label', pa.string()),
    ('date', pa.date32()),
])


class OutputWriterError(Exception):
    """A fixed, sanitized output failure without paths or record values."""

    def __init__(self, category):
        self.category = category
        super().__init__(category)


def manifest_path(output_path):
    """Return the sidecar path without exposing it through error messages."""
    output = Path(output_path)
    return output.with_suffix('.manifest.json')


def _approved_path(output_path, create_directory=False):
    if create_directory:
        APPROVED_DIRECTORY.mkdir(parents=True, exist_ok=True)
    root = APPROVED_DIRECTORY.resolve()
    candidate = Path(output_path)
    if not candidate.is_absolute():
        candidate = WORKSPACE_ROOT / candidate
    try:
        resolved = candidate.resolve()
    except OSError:
        raise OutputWriterError('unapproved_output_path') from None
    if resolved.parent != root or resolved.suffix != '.parquet' or not resolved.stem:
        raise OutputWriterError('unapproved_output_path')
    return resolved


def _require_git_ignored(*paths):
    for path in paths:
        try:
            result = subprocess.run(
                ['git', 'check-ignore', '--quiet', '--', str(path)],
                cwd=WORKSPACE_ROOT,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            raise OutputWriterError('git_ignore_check_failed') from None
        if result.returncode != 0:
            raise OutputWriterError('output_not_git_ignored')


def _checksum(path):
    digest = hashlib.sha256()
    try:
        with Path(path).open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
    except OSError:
        raise OutputWriterError('output_verification_failed') from None
    return digest.hexdigest()


def _verify_parquet(path, expected_rows):
    try:
        metadata = pq.read_metadata(path)
        schema = metadata.schema.to_arrow_schema()
    except Exception:
        raise OutputWriterError('output_verification_failed') from None
    if metadata.num_rows != expected_rows or schema != EXPECTED_SCHEMA:
        raise OutputWriterError('output_verification_failed')


def _manifest(config, summary, checksum):
    return {
        'completion_status': 'complete',
        'source': {
            'dataset': config.dataset,
            'revision': config.revision,
            'split': config.split,
            'shard': config.shard,
        },
        'selected_columns': list(config.selected_columns),
        'row_count': summary.row_count,
        'label_counts': summary.label_counts,
        'date_range': {
            'earliest': summary.earliest_date,
            'latest': summary.latest_date,
        },
        'duplicate_counts': {
            'sha256': summary.duplicate_sha256_count,
            'url': summary.duplicate_url_count,
            'row': summary.duplicate_row_count,
            'conflicting_sha256': summary.conflicting_sha256_count,
        },
        'output_sha256': checksum,
    }


def verify_completed_output(output_path):
    """Fail closed unless the manifest and Parquet file agree completely."""
    output = _approved_path(output_path)
    sidecar = manifest_path(output)
    try:
        document = json.loads(sidecar.read_text(encoding='utf-8'))
        if (not isinstance(document, dict)
                or document.get('completion_status') != 'complete'
                or type(document.get('row_count')) is not int
                or not isinstance(document.get('output_sha256'), str)):
            raise ValueError
        _verify_parquet(output, document['row_count'])
        if _checksum(output) != document['output_sha256']:
            raise ValueError
    except OutputWriterError:
        raise
    except Exception:
        raise OutputWriterError('incomplete_output') from None
    return document


def write_validated_parquet(batches, output_path, approved_shard=False):
    """Validate all batches, then publish Parquet and its manifest.

    Parquet and JSON cannot be replaced as one filesystem transaction. After
    both temporary files are verified, the old manifest is removed, Parquet
    is replaced, and the new manifest is published last. Thus no old manifest
    can certify new Parquet bytes, and a partial publication fails closed.
    """
    output = _approved_path(output_path, create_directory=True)
    sidecar = manifest_path(output)
    _require_git_ignored(output, sidecar)
    config = load_source_config()

    try:
        materialized = list(batches)
        summary = validate_batches(materialized)
    except ValidationError:
        raise OutputWriterError('validation_failed') from None
    except Exception:
        raise OutputWriterError('validation_failed') from None
    if approved_shard and summary.row_count != config.expected_rows:
        raise OutputWriterError('approved_row_count_mismatch')

    parquet_temp = manifest_temp = None
    try:
        parquet_handle, parquet_temp = tempfile.mkstemp(
            prefix='.adaptivephish-', suffix='.parquet.tmp', dir=output.parent
        )
        os.close(parquet_handle)
        writer = None
        try:
            writer = pq.ParquetWriter(parquet_temp, EXPECTED_SCHEMA)
            for record_batch in materialized:
                writer.write_batch(record_batch)
        finally:
            if writer is not None:
                writer.close()

        _verify_parquet(parquet_temp, summary.row_count)
        checksum = _checksum(parquet_temp)
        document = _manifest(config, summary, checksum)

        manifest_handle, manifest_temp = tempfile.mkstemp(
            prefix='.adaptivephish-', suffix='.json.tmp', dir=output.parent
        )
        with os.fdopen(manifest_handle, 'w', encoding='utf-8', newline='\n') as stream:
            json.dump(document, stream, indent=2, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())

        # Verify both temporary artifacts before publication begins. Until
        # this point, an existing complete output remains untouched.
        try:
            if json.loads(Path(manifest_temp).read_text(encoding='utf-8')) != document:
                raise ValueError
        except (OSError, ValueError, json.JSONDecodeError):
            raise OutputWriterError('output_verification_failed') from None

        # Publication boundary: remove the completion marker first. If either
        # replacement now fails, verify_completed_output() must reject the
        # remaining files as incomplete.
        sidecar.unlink(missing_ok=True)
        os.replace(parquet_temp, output)
        parquet_temp = None
        os.replace(manifest_temp, sidecar)
        manifest_temp = None
        verify_completed_output(output)
        return document
    except OutputWriterError:
        raise
    except Exception:
        raise OutputWriterError('output_write_failed') from None
    finally:
        for temporary in (parquet_temp, manifest_temp):
            if temporary is not None:
                try:
                    Path(temporary).unlink(missing_ok=True)
                except OSError:
                    pass

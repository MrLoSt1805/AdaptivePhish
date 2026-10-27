"""Reproducible temporal evaluation over verified local shard outputs."""

import os
from pathlib import Path
import tempfile

import pyarrow.parquet as pq

from adaptivephish.data.output_writer import (
    APPROVED_DIRECTORY, WORKSPACE_ROOT, verify_completed_output,
)


REPORT_PATH = WORKSPACE_ROOT / 'reports' / 'results' / 'temporal_evaluation_v2.json'
FIGURE_PATH = WORKSPACE_ROOT / 'reports' / 'figures' / 'temporal_f1_comparison_v2.png'
TRACKING_PATH = WORKSPACE_ROOT / 'mlruns'


class TemporalPipelineError(Exception):
    """Sanitized temporal-pipeline failure."""


def _atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.temporal-', suffix='.tmp',
                                             dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8', newline='\n') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def load_verified_local_rows():
    """Load only completed URL-metadata outputs; never scan arbitrary Parquet."""
    rows, sources = [], []
    try:
        from adaptivephish.data.shard_inventory import load_inventory
        inventory = load_inventory()
        for path in sorted(APPROVED_DIRECTORY.glob('train-*-url-metadata.parquet')):
            document = verify_completed_output(path)
            source = document.get('source', {})
            spec = inventory.get(path.name.replace('-url-metadata.parquet', ''))
            if (source != spec.source or source.get('split') != 'train'
                    or document.get('row_count') != spec.expected_rows
                    or document.get('selected_columns')
                    != ['sha256', 'url', 'label', 'date']):
                raise TemporalPipelineError('source_verification_failed')
            rows.extend(pq.read_table(path, columns=['url', 'label', 'date']).to_pylist())
            sources.append(source.get('shard'))
        if not rows:
            raise TemporalPipelineError('verified_dataset_missing')
        return rows, sources
    except TemporalPipelineError:
        raise
    except Exception:
        raise TemporalPipelineError('source_verification_failed') from None


def run_temporal_evaluation(overwrite=False):
    from adaptivephish.pipelines.research import run_research
    return run_research(overwrite=overwrite)

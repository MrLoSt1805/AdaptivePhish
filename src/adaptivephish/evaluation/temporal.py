"""Deterministic chronological evaluation helpers."""

from dataclasses import dataclass
import datetime


class TemporalEvaluationError(Exception):
    """Sanitized temporal-evaluation failure."""


@dataclass(frozen=True)
class TemporalPartition:
    train_indices: tuple
    test_indices: tuple
    cutoff: str


def chronological_partition(dates, train_fraction=0.8):
    """Split by whole dates so the same calendar day never crosses the cutoff."""
    if not 0 < train_fraction < 1 or not dates:
        raise TemporalEvaluationError('invalid_temporal_configuration')
    if any(type(value) is not datetime.date for value in dates):
        raise TemporalEvaluationError('invalid_temporal_date')
    unique_dates = sorted(set(dates))
    if len(unique_dates) < 2:
        raise TemporalEvaluationError('insufficient_temporal_coverage')
    cutoff_index = max(1, min(len(unique_dates) - 1,
                              int(len(unique_dates) * train_fraction)))
    cutoff = unique_dates[cutoff_index]
    train = tuple(index for index, value in enumerate(dates) if value < cutoff)
    test = tuple(index for index, value in enumerate(dates) if value >= cutoff)
    if not train or not test:
        raise TemporalEvaluationError('insufficient_temporal_coverage')
    return TemporalPartition(train, test, cutoff.isoformat())

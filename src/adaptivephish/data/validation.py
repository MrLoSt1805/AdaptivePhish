"""Offline validation for approved AdaptivePhish Arrow record batches."""

from dataclasses import asdict, dataclass, field
import datetime
import re

import pyarrow as pa


COLUMNS = ('sha256', 'url', 'label', 'date')
TYPES = (pa.string(), pa.string(), pa.string(), pa.date32())
LABELS = ('benign', 'phish')
SHA256_PATTERN = re.compile(r'^[0-9A-Fa-f]{64}$')
SCHEME_PATTERN = re.compile(r'^[A-Za-z][A-Za-z0-9+.-]*:')


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    """Sanitized aggregate results; no source row values are included."""

    valid: bool
    row_count: int
    label_counts: dict[str, int]
    earliest_date: str | None
    latest_date: str | None
    missing_fields: dict[str, int]
    invalid_values: dict[str, int]
    url_missing_scheme_count: int
    duplicate_sha256_count: int
    duplicate_url_count: int
    duplicate_row_count: int
    conflicting_sha256_count: int

    def to_dict(self):
        """Return an independent dictionary suitable for sanitized reports."""
        return asdict(self)


class ValidationError(Exception):
    """Fixed failure category plus a sanitized aggregate summary."""

    def __init__(self, category, summary):
        self.category = category
        self.summary = summary
        super().__init__(category)


@dataclass(slots=True)
class _State:
    row_count: int = 0
    label_counts: dict[str, int] = field(
        default_factory=lambda: {label: 0 for label in LABELS}
    )
    missing_fields: dict[str, int] = field(
        default_factory=lambda: {name: 0 for name in COLUMNS}
    )
    invalid_values: dict[str, int] = field(
        default_factory=lambda: {name: 0 for name in COLUMNS}
    )
    dates: list[datetime.date] = field(default_factory=list)
    url_missing_scheme_count: int = 0
    duplicate_sha256_count: int = 0
    duplicate_url_count: int = 0
    duplicate_row_count: int = 0
    sha_records: dict[str, tuple] = field(default_factory=dict)
    urls: set[str] = field(default_factory=set)
    rows: set[tuple] = field(default_factory=set)
    conflicts: set[str] = field(default_factory=set)

    def summary(self, valid):
        return ValidationSummary(
            valid=valid,
            row_count=self.row_count,
            label_counts=dict(self.label_counts),
            earliest_date=min(self.dates).isoformat() if self.dates else None,
            latest_date=max(self.dates).isoformat() if self.dates else None,
            missing_fields=dict(self.missing_fields),
            invalid_values=dict(self.invalid_values),
            url_missing_scheme_count=self.url_missing_scheme_count,
            duplicate_sha256_count=self.duplicate_sha256_count,
            duplicate_url_count=self.duplicate_url_count,
            duplicate_row_count=self.duplicate_row_count,
            conflicting_sha256_count=len(self.conflicts),
        )


def _check_schema(batch, state):
    if not isinstance(batch, pa.RecordBatch):
        raise ValidationError('invalid_batch', state.summary(False))
    if tuple(batch.schema.names) != COLUMNS:
        raise ValidationError('invalid_column_schema', state.summary(False))
    if tuple(batch.schema.types) != TYPES:
        raise ValidationError('invalid_arrow_types', state.summary(False))


def _value(batch, column_index, row_index, state, name):
    scalar = batch.column(column_index)[row_index]
    if not scalar.is_valid:
        state.missing_fields[name] += 1
        return None
    try:
        return scalar.as_py()
    except Exception:
        # Arrow conversion errors may contain source details. Never propagate
        # their text, and count the field as invalid instead.
        state.invalid_values[name] += 1
        return None


def _add_batch(batch, state):
    _check_schema(batch, state)
    for row_index in range(batch.num_rows):
        state.row_count += 1
        values = tuple(
            _value(batch, column_index, row_index, state, name)
            for column_index, name in enumerate(COLUMNS)
        )
        sha256, url, label, date = values

        if values in state.rows:
            state.duplicate_row_count += 1
        else:
            state.rows.add(values)

        valid_sha = isinstance(sha256, str) and SHA256_PATTERN.fullmatch(sha256)
        if sha256 is not None and not valid_sha:
            state.invalid_values['sha256'] += 1
        if valid_sha:
            if sha256 in state.sha_records:
                state.duplicate_sha256_count += 1
                if state.sha_records[sha256] != values[1:]:
                    state.conflicts.add(sha256)
            else:
                state.sha_records[sha256] = values[1:]

        valid_url = isinstance(url, str) and bool(url.strip())
        if url is not None and not valid_url:
            state.invalid_values['url'] += 1
        if valid_url:
            if url in state.urls:
                state.duplicate_url_count += 1
            else:
                state.urls.add(url)
            if SCHEME_PATTERN.match(url) is None:
                state.url_missing_scheme_count += 1

        if label is not None:
            if label in LABELS:
                state.label_counts[label] += 1
            else:
                state.invalid_values['label'] += 1

        if date is not None:
            if type(date) is datetime.date:
                state.dates.append(date)
            else:
                state.invalid_values['date'] += 1


def validate_batches(batches):
    """Validate batches and return aggregate results without changing rows.

    Duplicate records are counted and retained. Conflicting reuse of a valid
    publisher-provided SHA-256 identifier is rejected. The identifier format
    is checked, but its hash is not independently recomputed.
    """
    state = _State()
    try:
        for batch in batches:
            _add_batch(batch, state)
    except ValidationError:
        raise
    except Exception:
        raise ValidationError('batch_processing_error', state.summary(False)) from None

    summary = state.summary(False)
    if summary.conflicting_sha256_count:
        raise ValidationError('conflicting_identifier', summary)
    if any(summary.missing_fields.values()):
        raise ValidationError('missing_required_value', summary)
    if any(summary.invalid_values.values()):
        raise ValidationError('invalid_required_value', summary)
    return state.summary(True)

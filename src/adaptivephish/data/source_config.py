"""Load the immutable source contract for the first PhreshPhish shard."""

from dataclasses import dataclass
from pathlib import Path
import tomllib


APPROVED_VALUES = {
    'dataset': 'phreshphish/phreshphish',
    'revision': 'eca603e0ecd4f698f8d4d226c088659e72681239',
    'split': 'train',
    'shard': 'data/train-001.parquet',
    'expected_file_size': 322_018_201,
    'expected_row_groups': 1,
    'expected_rows': 5_780,
    'selected_columns': ('sha256', 'url', 'label', 'date'),
    'application_body_budget': 2_097_152,
    'maximum_http_requests': 32,
}
APPROVED_KEYS = frozenset(APPROVED_VALUES)
DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / 'configs' / 'phreshphish_source.toml'
)


class SourceConfigError(Exception):
    """A fixed, sanitized configuration failure with no source values."""

    def __init__(self, category):
        self.category = category
        super().__init__(category)


@dataclass(frozen=True, slots=True)
class SourceConfig:
    """Validated immutable settings for the single approved source shard."""

    dataset: str
    revision: str
    split: str
    shard: str
    expected_file_size: int
    expected_row_groups: int
    expected_rows: int
    selected_columns: tuple[str, ...]
    application_body_budget: int
    maximum_http_requests: int


def _parse(path):
    try:
        with Path(path).open('rb') as stream:
            document = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError, TypeError, ValueError):
        raise SourceConfigError('malformed_configuration') from None
    if not isinstance(document, dict) or set(document) != APPROVED_KEYS:
        raise SourceConfigError('invalid_configuration_shape')
    return document


def _validate(document):
    normalized = dict(document)
    columns = normalized.get('selected_columns')
    if not isinstance(columns, list) or any(type(item) is not str for item in columns):
        raise SourceConfigError('invalid_selected_columns')
    normalized['selected_columns'] = tuple(columns)

    for key, approved in APPROVED_VALUES.items():
        value = normalized.get(key)
        if type(value) is not type(approved) or value != approved:
            category = (
                'invalid_selected_columns'
                if key == 'selected_columns'
                else f'invalid_{key}'
            )
            raise SourceConfigError(category)
    return SourceConfig(**normalized)


def load_source_config(path=DEFAULT_CONFIG_PATH):
    """Load and validate the exact approved contract, failing closed."""
    return _validate(_parse(path))

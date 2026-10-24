"""Compatibility entry point for the reusable remote Parquet reader.

The implementation now lives in ``adaptivephish.data.remote_parquet``. This
module re-exports the established names so existing diagnostic scripts and
offline tests continue to use exactly the same safety behavior.
"""

from adaptivephish.data.remote_parquet import (
    BUDGET,
    COLUMNS,
    MAX_FOOTER,
    OVERHEAD,
    SIZE,
    URL,
    RangeFile,
    SafetyError,
    _failure_category,
    https_response,
    inspect_footer,
    pa,
    plan_first_group,
    pq,
    run_poc,
    safe_url,
    sample,
)


if __name__ == '__main__':
    # Review gate: no CLI flag can accidentally enable a real GET.
    print('Remote execution disabled pending implementation review; run offline unit tests.')

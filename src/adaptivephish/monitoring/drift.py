"""Aggregate-only feature drift measurements."""

import math

import numpy as np


class DriftError(Exception):
    """Sanitized drift calculation failure."""


def population_stability_index(reference, current, bins=10):
    """Return PSI using reference quantiles and stable finite smoothing."""
    try:
        reference = np.asarray(reference, dtype=float)
        current = np.asarray(current, dtype=float)
        if (reference.ndim != 1 or current.ndim != 1 or bins < 2
                or reference.size < bins or current.size == 0
                or not np.isfinite(reference).all()
                or not np.isfinite(current).all()):
            raise DriftError('invalid_drift_input')
        edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
        if edges.size < 2:
            value = edges[0]
            edges = np.array([-np.inf, value, np.nextafter(value, np.inf), np.inf])
        else:
            # Keep observed endpoints to expose shifts outside a narrow range.
            edges = np.unique(np.r_[-np.inf, edges, np.nextafter(edges[-1], np.inf), np.inf])
        ref_counts = np.histogram(reference, edges)[0]
        cur_counts = np.histogram(current, edges)[0]
        epsilon = 1e-6
        ref_share = np.maximum(ref_counts / reference.size, epsilon)
        cur_share = np.maximum(cur_counts / current.size, epsilon)
        value = float(np.sum((cur_share - ref_share) * np.log(cur_share / ref_share)))
        if not math.isfinite(value):
            raise DriftError('drift_calculation_failed')
        return value
    except DriftError:
        raise
    except Exception:
        raise DriftError('drift_calculation_failed') from None


def drift_summary(reference_matrix, current_matrix, feature_names,
                  warning_threshold=0.1, critical_threshold=0.25):
    """Calculate per-feature PSI and a deterministic aggregate decision."""
    reference = np.asarray(reference_matrix, dtype=float)
    current = np.asarray(current_matrix, dtype=float)
    if (reference.ndim != 2 or current.ndim != 2
            or reference.shape[1] != current.shape[1]
            or reference.shape[1] != len(feature_names)
            or not 0 <= warning_threshold < critical_threshold):
        raise DriftError('invalid_drift_input')
    scores = {
        name: population_stability_index(reference[:, index], current[:, index])
        for index, name in enumerate(feature_names)
    }
    maximum = max(scores.values(), default=0.0)
    status = ('critical' if maximum >= critical_threshold else
              'warning' if maximum >= warning_threshold else 'stable')
    return {'method': 'psi', 'status': status, 'maximum_psi': maximum,
            'feature_psi': scores, 'reference_rows': int(reference.shape[0]),
            'current_rows': int(current.shape[0])}

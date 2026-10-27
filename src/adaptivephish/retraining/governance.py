"""Fail-closed retraining and model-promotion policy."""

import math


class GovernanceError(Exception):
    """Sanitized governance failure."""


def retraining_decision(drift_status, labeled_rows, minimum_labeled_rows=500):
    if drift_status not in {'stable', 'warning', 'critical'}:
        raise GovernanceError('invalid_drift_status')
    if (type(labeled_rows) is not int or labeled_rows < 0
            or type(minimum_labeled_rows) is not int or minimum_labeled_rows < 1):
        raise GovernanceError('invalid_retraining_input')
    triggered = drift_status == 'critical' and labeled_rows >= minimum_labeled_rows
    return {'triggered': triggered,
            'reason': ('critical_drift_with_labels' if triggered else
                       'insufficient_labels' if drift_status == 'critical' else
                       'drift_below_threshold')}


def promotion_decision(champion, candidate, minimum_f1_gain=0.0,
                       maximum_fpr_increase=0.0, maximum_latency_seconds=0.01):
    """Promote only a non-regressing candidate with a required F1 gain."""
    required = {'f1', 'recall', 'roc_auc', 'false_positive_rate',
                'prediction_latency_seconds_per_row'}
    if not required.issubset(champion) or not required.issubset(candidate):
        raise GovernanceError('missing_governance_metric')
    try:
        for metrics in (champion, candidate):
            for key in required:
                value = metrics[key]
                if (isinstance(value, bool) or not isinstance(value, (int, float))
                        or not math.isfinite(value) or value < 0
                        or (key != 'prediction_latency_seconds_per_row' and value > 1)):
                    raise ValueError
        if any(not math.isfinite(v) or v < 0 for v in
               (minimum_f1_gain, maximum_fpr_increase, maximum_latency_seconds)):
            raise ValueError
        f1_gain = float(candidate['f1']) - float(champion['f1'])
        fpr_increase = (float(candidate['false_positive_rate'])
                        - float(champion['false_positive_rate']))
        auc_regressed = float(candidate['roc_auc']) < float(champion['roc_auc'])
    except Exception:
        raise GovernanceError('invalid_governance_metric') from None
    approved = (f1_gain >= minimum_f1_gain
                and fpr_increase <= maximum_fpr_increase
                and not auc_regressed
                and candidate['recall'] >= champion['recall']
                and candidate['prediction_latency_seconds_per_row'] <= maximum_latency_seconds)
    return {'approved': approved, 'f1_gain': f1_gain,
            'false_positive_rate_increase': fpr_increase,
            'auc_regressed': auc_regressed,
            'reason': 'promotion_approved' if approved else 'promotion_rejected'}

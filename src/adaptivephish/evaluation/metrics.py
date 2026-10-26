"""Model metrics with sanitized, aggregate-only output."""

from time import perf_counter


class EvaluationError(Exception):
    """Sanitized evaluation failure."""


def evaluate_classifier(model, features, labels):
    try:
        from sklearn.metrics import (
            accuracy_score, confusion_matrix, precision_score, recall_score,
            f1_score, roc_auc_score,
        )
        started = perf_counter()
        probabilities = model.predict_proba(features)[:, 1]
        predictions = model.predict(features)
        elapsed = perf_counter() - started
        tn, fp, fn, tp = confusion_matrix(labels, predictions, labels=[0, 1]).ravel()
        auc = roc_auc_score(labels, probabilities) if len(set(labels)) == 2 else None
        return {
            'accuracy': float(accuracy_score(labels, predictions)),
            'precision': float(precision_score(labels, predictions, zero_division=0)),
            'recall': float(recall_score(labels, predictions, zero_division=0)),
            'f1': float(f1_score(labels, predictions, zero_division=0)),
            'roc_auc': None if auc is None else float(auc),
            'false_positive_rate': float(fp / (fp + tn)) if fp + tn else 0.0,
            'confusion_matrix': {'tn': int(tn), 'fp': int(fp),
                                 'fn': int(fn), 'tp': int(tp)},
            'prediction_latency_seconds_total': float(elapsed),
            'prediction_latency_seconds_per_row': float(elapsed / max(len(labels), 1)),
            'prediction_count': len(labels),
        }
    except ImportError:
        raise EvaluationError('ml_dependencies_unavailable') from None
    except Exception:
        raise EvaluationError('evaluation_failed') from None

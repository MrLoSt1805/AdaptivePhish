"""Offline tests for temporal evaluation, drift and governance."""

import datetime
import unittest

from adaptivephish.evaluation.temporal import (
    TemporalEvaluationError, chronological_partition,
)
from adaptivephish.monitoring.drift import DriftError, drift_summary
from adaptivephish.retraining.governance import (
    GovernanceError, promotion_decision, retraining_decision,
)


class TemporalDriftGovernanceTests(unittest.TestCase):
    def test_temporal_partition_keeps_days_together(self):
        dates = [datetime.date(2025, 1, 2), datetime.date(2025, 1, 1),
                 datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)]
        result = chronological_partition(dates, 0.67)
        self.assertEqual(result.cutoff, '2025-01-03')
        self.assertEqual(result.train_indices, (0, 1, 2))
        self.assertEqual(result.test_indices, (3,))

    def test_temporal_partition_rejects_bad_inputs(self):
        for values in ([], [datetime.date(2025, 1, 1)], ['private']):
            with self.subTest(values=values), self.assertRaises(TemporalEvaluationError):
                chronological_partition(values)

    def test_drift_stable_and_critical(self):
        reference = [[float(index), float(index % 3)] for index in range(100)]
        stable = drift_summary(reference, reference, ['length', 'dots'])
        shifted = drift_summary(reference,
                                [[float(index + 1000), 20.0] for index in range(100)],
                                ['length', 'dots'])
        self.assertEqual(stable['status'], 'stable')
        self.assertEqual(shifted['status'], 'critical')
        self.assertNotIn('url', stable)

    def test_drift_rejects_invalid_shape(self):
        with self.assertRaises(DriftError):
            drift_summary([[1.0]], [[1.0, 2.0]], ['one'])

    def test_retraining_requires_drift_and_labels(self):
        self.assertFalse(retraining_decision('stable', 1000)['triggered'])
        self.assertFalse(retraining_decision('critical', 499)['triggered'])
        self.assertTrue(retraining_decision('critical', 500)['triggered'])

    def test_promotion_is_fail_closed(self):
        champion = {'f1': .8, 'roc_auc': .9, 'false_positive_rate': .1, 'recall': .8, 'prediction_latency_seconds_per_row': .001}
        candidate = {'f1': .82, 'roc_auc': .91, 'false_positive_rate': .09, 'recall': .8, 'prediction_latency_seconds_per_row': .001}
        self.assertTrue(promotion_decision(champion, candidate)['approved'])
        candidate['roc_auc'] = .89
        self.assertFalse(promotion_decision(champion, candidate)['approved'])
        with self.assertRaises(GovernanceError):
            promotion_decision({}, {})


if __name__ == '__main__':
    unittest.main()

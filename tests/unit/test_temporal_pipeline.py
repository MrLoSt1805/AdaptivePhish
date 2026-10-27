"""Focused offline safety tests for the temporal pipeline command."""

import io
import json
import unittest
from unittest.mock import patch

from scripts import run_temporal_evaluation as command


class TemporalPipelineCommandTests(unittest.TestCase):
    def test_flag_is_required(self):
        output = io.StringIO()
        with patch('sys.stdout', output), patch.object(
                command, 'run_temporal_evaluation') as run:
            self.assertEqual(command.main([]), 2)
        run.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())['failure_category'],
                         'run_flag_required')

    def test_output_is_aggregate_only(self):
        report = {'row_count': 10, 'cutoff_date': '2025-01-01',
                  'train_rows': 8, 'test_rows': 2,
                  'drift': {'status': 'stable'}}
        output = io.StringIO()
        with patch('sys.stdout', output), patch.object(
                command, 'run_temporal_evaluation', return_value=report):
            self.assertEqual(command.main(['--run']), 0)
        self.assertNotIn('url', output.getvalue().casefold())


if __name__ == '__main__':
    unittest.main()

"""One-command demo output remains aggregate and offline."""

import io
import json
import unittest
from unittest.mock import patch

from scripts import demo


class DemoTests(unittest.TestCase):
    def test_demo_never_prints_inputs(self):
        prediction = {'predicted_label': 'benign', 'model_version': 'phase3-v1'}
        output = io.StringIO()
        with patch('sys.stdout', output), patch.object(
                demo, 'predict_url', return_value=prediction):
            self.assertEqual(demo.main(), 0)
        result = json.loads(output.getvalue())
        self.assertTrue(result['success'])
        self.assertNotIn('.invalid', output.getvalue())


if __name__ == '__main__':
    unittest.main()

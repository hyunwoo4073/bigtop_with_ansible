"""Validate isolated publisher's exact-offset guard (Kafka CLI mocked)."""
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROLE = Path(__file__).resolve().parents[1] / 'roles/olist_kafka72/files'
sys.path.insert(0, str(ROLE))
spec = importlib.util.spec_from_file_location('step72_publish_test', ROLE/'step72_publish.py')
pub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pub)

class TestStep72Publisher(unittest.TestCase):
    def test_topic_is_step72(self):
        self.assertEqual(pub.TOPIC, 'bigtop_olist_step72_crash_v1')
    def test_initial_offset_only(self):
        with patch.object(pub,'kafka_end',return_value=9), patch.object(pub,'checked') as checked:
            with self.assertRaises(pub.GuardError):
                pub.publish('publish','/bin','broker:9092')
            checked.assert_not_called()
    def test_publishes_exactly_nine(self):
        with patch.object(pub,'kafka_end',side_effect=[0,9]),patch.object(pub,'checked',return_value='') as checked:
            pub.publish('publish','/bin','broker:9092')
            self.assertIn('--topic',checked.call_args.args)
            self.assertEqual(checked.call_args.kwargs['input_text'].count('\n'),9)
if __name__ == '__main__':
    unittest.main()

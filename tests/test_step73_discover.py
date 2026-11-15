import importlib.util
import pathlib
import unittest

P = pathlib.Path(__file__).resolve().parents[1] / 'roles' / 'olist_kafka73' / 'files' / 'step73_discover.py'
spec = importlib.util.spec_from_file_location('step73_discover', str(P))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class TestProbe(unittest.TestCase):
    def test_rules(self):
        rules = {'data': {'groups': [{'rules': [
            {'name': 'BigtopOlistStep68StreamingLag', 'health': 'ok', 'state': 'inactive'},
            {'name': 'other', 'health': 'ok'},
        ]}]}}
        self.assertEqual(list(module.summarize_rules(rules)), ['BigtopOlistStep68StreamingLag'])

    def test_receiver_redaction(self):
        receivers = [{'name': 'ops', 'webhook_configs': [{'url': 'http://secret.local/key'}]}]
        self.assertEqual(module.summarize_receivers(receivers), ['ops'])

    def test_no_external_host_in_manager_summary(self):
        data = {'data': {'activeAlertmanagers': [{'url': 'http://secret.local:9093'}]}}
        self.assertEqual(module.summarize_alertmanagers(data), {'active': 1, 'dropped': 0})


if __name__ == '__main__':
    unittest.main()

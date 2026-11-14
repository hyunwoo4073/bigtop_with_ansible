"""Step70 offline tests. Run with installed Web Console Python environment."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'roles' / 'web_console' / 'files'))
import kafka_ops70 as ops

class Step70Tests(unittest.TestCase):
    def test_unknown_is_not_zero(self):
        self.assertIsNone(ops._metrics_view([])['lag_total'])

    def test_lag(self):
        s = [{'metric': {'__name__': 'bigtop_olist_step68_offset_lag', 'topic': ops.CDC_TOPIC, 'partition': '0'}, 'value': [1, '0']}]
        self.assertEqual(ops._metrics_view(s)['lag_total'], 0)

    def test_tcp_unreachable(self):
        with patch.object(ops, '_broker_addresses', return_value=[('invalid', 9092)]), patch.object(ops.socket, 'create_connection', side_effect=OSError()):
            self.assertFalse(ops._broker_tcp()[0]['tcp_reachable'])

    def test_group_lag_parsing(self):
        sample = 'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\ngroup-one mytopic 0 12 15 3 - - -\ngroup-one mytopic 1 8 10 2 - - -\n'
        with patch.object(ops, '_command', return_value=sample) as command:
            d = ops.kafka_group_lag('group-one')
        self.assertEqual(d['total_lag'], 5)
        self.assertEqual(command.call_args.args[1], ['--describe', '--group', 'group-one'])

    def test_invalid_group_is_rejected(self):
        with self.assertRaises(ops.HTTPException):
            ops.kafka_group_lag('a;rm -rf')

    def test_topics_list(self):
        with patch.object(ops, '_command', return_value='a\nb\n'):
            self.assertEqual(ops.kafka_topics()['items'], ['a', 'b'])

    def test_overview_no_kafka_cli(self):
        with patch.object(ops, '_prometheus_samples', return_value=([], 'offline')), patch.object(ops, '_broker_tcp', return_value=[]), patch.object(ops, '_command', side_effect=AssertionError('no CLI')):
            d = ops.kafka_overview()
        self.assertTrue(d['read_only'])
        self.assertIsNone(d['streaming']['lag_total'])
        self.assertEqual(d['prometheus_error'], 'offline')

if __name__ == '__main__':
    unittest.main()

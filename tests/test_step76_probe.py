import importlib.util
import json
import pathlib
import sys
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
FILE = ROOT / 'roles/olist_metadata76/files/step76_probe.py'
spec = importlib.util.spec_from_file_location('step76_probe', FILE)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class Step76ProbeTest(unittest.TestCase):
    def test_pg_injection_prevented(self):
        with self.assertRaises(ValueError):
            probe.pg('postgres;DROP DATABASE')

    def test_mysql_injection_prevented(self):
        with self.assertRaises(ValueError):
            probe.mysql('bigtop-openmetadata', 'a;DROP')

    def test_hdfs_root_guard(self):
        with self.assertRaises(ValueError):
            probe.hdfs('/user/hadoop')
        with self.assertRaises(ValueError):
            probe.hdfs('/data/olist/../etc')

    def test_docker_id_guard(self):
        with patch.object(probe, 'run', return_value='id1\nid2'):
            with self.assertRaises(RuntimeError):
                probe.docker_id('bigtop-openmetadata', 'mysql')

    def test_pg_read_only_queries(self):
        outputs = ['iceberg_rest56', '10', '12345', 'pg_dump (PostgreSQL) 16.0']
        with patch.object(probe, 'run', side_effect=outputs) as run_mock, patch.object(
                probe, 'disk_info', return_value={'free_mib': 3000}):
            result = probe.pg('iceberg_rest56')
            self.assertTrue(result['ready_for_backup'])
            self.assertEqual(result['user_table_count'], 10)
            self.assertEqual(run_mock.call_count, 4)

    def test_mysql_no_password_exposure(self):
        with patch.object(probe, 'docker_id', return_value='abc'), patch.object(
                probe, 'docker_summary', return_value={
                    'status': 'running', 'health': 'healthy',
                    'mounts': [{'destination': '/var/lib/mysql', 'type': 'volume', 'volume': 'db'}]
                }), patch.object(probe, 'disk_info', return_value={'free_mib': 2000}), patch.object(
                probe, 'run', side_effect=['123', 'mysqldump 8.0', 'openmetadata_db']):
            result = probe.mysql('bigtop-openmetadata', 'openmetadata_db')
            self.assertEqual(result['user_table_count'], 123)
            self.assertNotIn('password', json.dumps(result).lower())

    def test_hdfs_missing_is_non_destructive(self):
        with patch.object(probe, 'run', side_effect=RuntimeError('not found')):
            result = probe.hdfs('/data/olist/iceberg-rest')
            self.assertFalse(result['exists'])


if __name__ == '__main__':
    unittest.main()

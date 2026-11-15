"""Offline safety checks for Step74 isolated watchdog recovery proof."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'roles/olist_kafka74/files/step74_proof.py'
SPEC = importlib.util.spec_from_file_location('step74_test_module', SRC)
proof = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proof)


class ReadOnlyAndFaultGuards(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.env = [patch.object(proof, 'ROOT', self.root),
                    patch.object(proof, 'LOCK', self.root / 'control.lock'),
                    patch.object(proof, 'EVIDENCE', self.root / 'step74-proof.json')]
        for p in self.env:
            p.start()

    def tearDown(self):
        for p in reversed(self.env):
            p.stop()
        self.tmp.cleanup()

    @staticmethod
    def healthy():
        app = dict(app_id='application_1_2', name=proof.APP_NAME,
                   type='SPARK', owner=proof.OWNER, queue='batch', state='RUNNING')
        return dict(app=app, active_applications=[app], desired='running',
                    timer_active='active', timer_enabled='enabled',
                    checkpoint_present=True, attempts_in_last_hour=0,
                    total_attempts=2, ready_for_fault=True)

    def test_parse_yarn_list_requires_header(self):
        with patch.object(proof, 'command') as call:
            call.return_value.stdout = 'unexpected blank output'
            with self.assertRaises(proof.GuardError):
                proof.yarn_apps()

    def test_parse_yarn_list_exact_columns(self):
        out = ('Total number of applications (application-types: []):2\n'
               'Application-Id Application-Name Application-Type User Queue State\n'
               'application_1_2 olist-iceberg-kafka65-continuous SPARK hadoop batch RUNNING\n'
               'application_1_3 other SPARK hadoop batch ACCEPTED\n')
        with patch.object(proof, 'command') as call:
            call.return_value.stdout = out
            apps = proof.yarn_apps()
        self.assertEqual(len(apps), 2)
        self.assertEqual(proof.exact_step65(apps)['app_id'], 'application_1_2')

    def test_fault_needs_explicit_optin(self):
        with patch.object(proof, 'base_state', side_effect=AssertionError('must not check')):
            with self.assertRaises(proof.GuardError):
                proof.fault('application_1_2', 'NO')

    def test_fault_rejects_wrong_id(self):
        with patch.object(proof, 'base_state', return_value=self.healthy()), \
                patch.object(proof, 'command', side_effect=AssertionError('must not kill')):
            with self.assertRaises(proof.GuardError):
                proof.fault('application_1_99', 'YES')

    def test_fault_rejects_disarmed_state(self):
        data = self.healthy()
        data['ready_for_fault'] = False
        with patch.object(proof, 'base_state', return_value=data), \
                patch.object(proof, 'command', side_effect=AssertionError('must not kill')):
            with self.assertRaises(proof.GuardError):
                proof.fault('application_1_2', 'YES')

    def test_single_exact_app_kill_and_evidence(self):
        with patch.object(proof, 'base_state', return_value=self.healthy()), \
                patch.object(proof, 'command') as shell:
            proof.fault('application_1_2', 'YES')
            shell.assert_called_once_with('yarn', 'application', '-kill', 'application_1_2', timeout=90)
        self.assertEqual(json.loads(proof.EVIDENCE.read_text())['phase'], 'fault_sent')
        self.assertEqual(proof.EVIDENCE.stat().st_mode & 0o777, 0o600)
        with patch.object(proof, 'base_state', return_value=self.healthy()), \
                patch.object(proof, 'command', side_effect=AssertionError('must not kill')):
            with self.assertRaises(proof.GuardError):
                proof.fault('application_1_2', 'YES')

    def test_verify_detects_new_running_app_only(self):
        proof.EVIDENCE.write_text(json.dumps({'phase': 'fault_sent',
                         'expected_app_id': 'application_1_2', 'original_attempts': 2}))
        data = self.healthy()
        data['total_attempts'] = 3
        data['app'] = dict(data['app'], app_id='application_1_3')
        data['active_applications'] = [data['app']]
        with patch.object(proof, 'base_state', return_value=data):
            passed, _, _ = proof.verify_once()
            self.assertTrue(passed)
        data['total_attempts'] = 4
        with patch.object(proof, 'base_state', return_value=data):
            with self.assertRaises(proof.GuardError):
                proof.verify_once()

    def test_no_unsafe_operation_in_inspect(self):
        with patch.object(proof, 'base_state', return_value=self.healthy()), \
                patch.object(proof, 'command', side_effect=AssertionError('must not mutate')):
            proof.inspect()
        self.assertFalse(proof.EVIDENCE.exists())


class Step74Packaging(unittest.TestCase):
    def test_yaml_role_and_playbook(self):
        for f in list((ROOT / 'roles/olist_kafka74').rglob('*.yml')) + \
                 [ROOT / 'playbooks/74-watchdog-recovery-proof.yml']:
            self.assertIsNotNone(yaml.safe_load(f.read_text()))
        txt = (ROOT / 'roles/olist_kafka74/tasks/main.yml').read_text()
        self.assertIn('olist_kafka74_confirm_fault', txt)
        self.assertNotIn('state: absent', txt)

    def test_no_destructive_reset_in_step74_sources(self):
        for text in [SRC.read_text(),
                     (ROOT / 'roles/olist_kafka74/tasks/main.yml').read_text()]:
            for unsafe in ['hdfs dfs -rm', 'DROP TABLE', 'TRUNCATE', 'KafkaTopics.delete',
                           'systemctl restart', 'rm -rf']:
                self.assertNotIn(unsafe, text)


if __name__ == '__main__':
    unittest.main()

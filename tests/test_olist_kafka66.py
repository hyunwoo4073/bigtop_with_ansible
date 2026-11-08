"""Step66 watchdog contract, cold-start safety, recovery policy, config checks."""
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
ROLE = ROOT / 'roles/olist_kafka66'
SRC = ROOT / 'roles/olist_kafka65/files/step65_yarn_control.py'
WRAP = ROLE / 'files/step66_watchdog.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ctl = load('step66_yarn_controller_unit', SRC)
watch = load('step66_watchdog_unit', WRAP)


class Step66RecoverySafety(unittest.TestCase):
    def setUp(self):
        self.tdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tdir.name)
        self.patchers = [
            patch.object(ctl, 'DESIRED_FILE', str(self.root / 'desired.json')),
            patch.object(ctl, 'RECOVERY_FILE', str(self.root / 'recovery.json')),
            patch.object(ctl, 'LOCK', str(self.root / 'control.lock')),
        ]
        for p in self.patchers:
            p.start()
        ctl.atomic_json(ctl.RECOVERY_FILE, {'attempts': [], 'total_attempts': 0,
                                            'missing_since': 0, 'last_attempt_at': 0})

    def tearDown(self):
        for p in reversed(self.patchers):
            p.stop()
        self.tdir.cleanup()

    def test_default_disarmed_never_contacts_yarn_or_submits(self):
        with patch.object(ctl, 'active_apps', side_effect=AssertionError('should not query')):
            value = ctl.do_recover('bootstrap', now=1000)
            self.assertEqual(value['state'], 'unarmed')
            self.assertEqual(value['desired_running'], 0)

    def test_running_app_does_not_restart(self):
        ctl.set_desired(True)
        with patch.object(ctl, 'active_apps', return_value=[('application_1_1', 'RUNNING')]) as apps:
            value = ctl.do_recover('bootstrap', now=1000)
            self.assertEqual(value['state'], 'running')
            self.assertEqual(value['running'], 1)
            apps.assert_called_once_with(strict=True)

    def test_accepted_app_never_creates_duplicate(self):
        ctl.set_desired(True)
        with patch.object(ctl, 'active_apps', return_value=[('application_1_1', 'ACCEPTED')]), \
             patch.object(ctl, 'execute', side_effect=AssertionError('must not submit')):
            value = ctl.do_recover('bootstrap', now=1000)
            self.assertEqual(value['state'], 'accepted')
            self.assertEqual(value['active'], 1)

    def test_missing_grace_then_one_recovery(self):
        ctl.set_desired(True)
        with patch.object(ctl, 'active_apps', return_value=[]):
            self.assertEqual(ctl.do_recover('bootstrap', now=1000)['state'], 'missing_grace')
            self.assertEqual(ctl.do_recover('bootstrap', now=1119)['state'], 'missing_grace')
        with patch.object(ctl, 'active_apps', side_effect=[[], [('application_1_2', 'RUNNING')]]), \
             patch.object(ctl, 'spark_command', return_value=['spark-submit']), \
             patch.object(ctl, 'execute', return_value='submitted') as submit:
            report = ctl.do_recover('bootstrap', now=1120)
            self.assertEqual(report['restarted'], 1)
            self.assertEqual(report['total_attempts'], 1)
            submit.assert_called_once_with('spark-submit', timeout=240)
        self.assertEqual(ctl.recovery_state()['missing_since'], 0)

    def test_hourly_restart_cap(self):
        ctl.set_desired(True)
        ctl.atomic_json(ctl.RECOVERY_FILE, {'attempts': [900, 920, 940],
                                            'total_attempts': 3,
                                            'missing_since': 800,
                                            'last_attempt_at': 940})
        with patch.object(ctl, 'active_apps', return_value=[]), \
             patch.object(ctl, 'execute', side_effect=AssertionError('rate limited')):
            report = ctl.do_recover('bootstrap', now=1100)
            self.assertEqual(report['state'], 'rate_limited')
            self.assertEqual(report['blocked'], 1)
            self.assertEqual(report['total_attempts'], 3)

    def test_yarn_error_blocks_submission(self):
        ctl.set_desired(True)
        with patch.object(ctl, 'active_apps', side_effect=ctl.StateError('YARN unavailable')), \
             patch.object(ctl, 'execute', side_effect=AssertionError('never submit')):
            with self.assertRaises(ctl.StateError):
                ctl.do_recover('bootstrap', now=2000)

    def test_missing_output_fails_closed_only_on_strict(self):
        with patch.object(ctl, 'execute', return_value='garbage'):
            with self.assertRaises(ctl.StateError):
                ctl.active_apps(strict=True)
            self.assertEqual(ctl.active_apps(), [])

    def test_multiple_active_apps_fails_closed(self):
        text = ('Total number of applications (application-types: []):2\n'
                'application_1_1 olist-iceberg-kafka65-continuous SPARK hadoop batch RUNNING UNDEFINED 50% url\n'
                'application_1_2 olist-iceberg-kafka65-continuous SPARK hadoop batch ACCEPTED UNDEFINED 0% url')
        with patch.object(ctl, 'execute', return_value=text):
            with self.assertRaises(ctl.StateError):
                ctl.active_apps(strict=True)

    def test_manual_stop_disarms_even_when_yarn_is_down(self):
        ctl.set_desired(True)
        with patch.object(ctl, 'active_apps', side_effect=ctl.StateError('RM down')):
            with self.assertRaises(ctl.StateError):
                ctl.run('stop', 'bootstrap')
        self.assertFalse(ctl.desired_running())

    def test_manual_disarm_does_not_query_yarn_or_kill(self):
        ctl.set_desired(True)
        with patch.object(ctl, 'active_apps', side_effect=AssertionError('must not query')):
            ctl.run('disarm', 'bootstrap')
        self.assertFalse(ctl.desired_running())

    def test_arm_requires_running_status(self):
        with patch.object(ctl, 'active_apps', return_value=[('application_1_1', 'ACCEPTED')]):
            with self.assertRaises(ctl.StateError):
                ctl.run('arm', 'bootstrap')
        self.assertFalse(ctl.desired_running())

    def test_recover_attempt_counted_before_submission_error(self):
        ctl.set_desired(True)
        ctl.atomic_json(ctl.RECOVERY_FILE, {'attempts': [], 'total_attempts': 0,
                                            'missing_since': 100,
                                            'last_attempt_at': 0})
        with patch.object(ctl, 'active_apps', return_value=[]), \
             patch.object(ctl, 'spark_command', return_value=['spark-submit']), \
             patch.object(ctl, 'execute', side_effect=ctl.StateError('app failed')):
            with self.assertRaises(ctl.StateError):
                ctl.do_recover('bootstrap', now=500)
        self.assertEqual(ctl.recovery_state()['total_attempts'], 1)

    def test_metrics_atomic_and_no_secrets(self):
        path = self.root / 'watchdog.prom'
        watch.write_atomic(str(path), watch.textfile({
            'watchdog_success': 1, 'desired_running': 1, 'running': 1,
            'active': 1, 'restarted': 0, 'blocked': 0,
            'attempts_window': 0, 'total_attempts': 0}, 1800))
        body = path.read_text()
        self.assertIn('bigtop_olist_step66_watchdog_success 1', body)
        self.assertIn('bigtop_olist_step66_last_run_timestamp_seconds 1800', body)
        self.assertEqual(path.stat().st_mode & 0o777, 0o644)


class Step66Artifacts(unittest.TestCase):
    def test_playbook_and_role_modes(self):
        play = yaml.safe_load((ROOT / 'playbooks/66-kafka-streaming-watchdog.yml').read_text())
        self.assertEqual(play[0]['hosts'], 'master')
        defaults = yaml.safe_load((ROLE / 'defaults/main.yml').read_text())
        self.assertEqual(defaults['olist_kafka66_mode'], 'plan')
        self.assertEqual(defaults['olist_kafka66_user'], 'hadoop')
        self.assertIn('bigtop-olist-step66-watchdog', defaults['olist_kafka66_service'])
        role = (ROLE / 'tasks/main.yml').read_text()
        for mode in ('preflight', 'apply', 'enable', 'disable', 'verify'):
            self.assertIn(mode, role)

    def test_timer_is_opt_in_and_unit_root_wrapper_only(self):
        timer = (ROLE / 'templates/bigtop-olist-step66-watchdog.timer.j2').read_text()
        unit = (ROLE / 'templates/bigtop-olist-step66-watchdog.service.j2').read_text()
        self.assertIn('OnUnitInactiveSec=60s', timer)
        self.assertIn('Type=oneshot', unit)
        self.assertIn('ExecStart=/usr/bin/python3', unit)
        self.assertIn('runuser', WRAP.read_text())
        install = (ROLE / 'tasks/install.yml').read_text()
        self.assertIn('enabled: false', install)

    def test_no_restart_of_brokers_or_destructive_iceberg(self):
        content = '\n'.join(p.read_text() for p in ROLE.rglob('*') if p.is_file() and p.suffix in ('.py', '.yml', '.j2'))
        for forbidden in ('DROP TABLE', 'TRUNCATE', 'systemctl restart', 'update-alternatives', 'hdfs dfs -rm'):
            self.assertNotIn(forbidden, content)

    def test_valid_yaml(self):
        for file in list(ROLE.rglob('*.yml')) + [ROOT / 'playbooks/66-kafka-streaming-watchdog.yml']:
            with self.subTest(file=file):
                self.assertIsNotNone(yaml.safe_load(file.read_text()))

    def test_grafana_json_after_jinja_uid_substitution(self):
        text = (ROLE / 'templates/olist-step66-streaming-watchdog.json.j2').read_text()
        rendered = text.replace('{{ olist_kafka66_datasource_uid }}', 'prometheus')
        data = json.loads(rendered)
        self.assertEqual(data['uid'], 'bigtop-olist-step66')
        self.assertGreaterEqual(len(data['panels']), 8)
        self.assertTrue(any('bigtop_olist_step66_running' in p['targets'][0]['expr'] for p in data['panels']))

    def test_no_vendor_files_in_package(self):
        for p in list(ROLE.rglob('*')) + [SRC]:
            if p.is_file():
                self.assertNotIn(p.suffix, ('.jar', '.zip', '.tar', '.tgz'))


if __name__ == '__main__':
    unittest.main()

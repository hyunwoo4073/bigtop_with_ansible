"""Static checks for the playbook-36 PostgreSQL backup alert rules."""
import pathlib
import unittest
import yaml
from jinja2 import Template

ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULTS = yaml.safe_load((ROOT / 'roles/web_console_pg_backup_alerts/defaults/main.yml').read_text())
TEMPLATE = (ROOT / 'roles/web_console_pg_backup_alerts/templates/web-console-pg-backup.yml.j2').read_text()


class BackupAlertRuleTests(unittest.TestCase):
    def setUp(self):
        values = {'web_console_pg_backup_alerts_backup_age_seconds': 93600,
                  'web_console_pg_backup_alerts_observer_age_seconds': 1800}
        self.doc = yaml.safe_load(Template(TEMPLATE).render(**values))
        self.rules = self.doc['groups'][0]['rules']

    def test_seven_distinct_rules(self):
        names = [r['alert'] for r in self.rules]
        self.assertEqual(len(names), 7)
        self.assertEqual(len(set(names)), 7)

    def test_all_have_severity_and_for(self):
        for r in self.rules:
            self.assertIn(r['labels']['severity'], ('warning', 'critical'))
            self.assertRegex(r['for'], r'^\d+m$')

    def test_exact_collector_metrics(self):
        expressions = '\n'.join(r['expr'] for r in self.rules)
        for metric in ('archive_count', 'latest_archive_valid',
                       'latest_archive_timestamp_seconds', 'timer_active',
                       'collector_success', 'last_check_timestamp_seconds'):
            self.assertIn('bigtop_web_console_pg_backup_' + metric, expressions)

    def test_missing_metrics_alerted_independently(self):
        rule = next(r for r in self.rules if r['alert'] == 'BigtopPGBackupMetricsMissing')
        self.assertTrue(rule['expr'].startswith('absent('))

    def test_configurable_thresholds_rendered(self):
        text = '\n'.join(r['expr'] for r in self.rules)
        self.assertIn('93600', text)
        self.assertIn('1800', text)


if __name__ == '__main__':
    unittest.main()

"""Unit-level checks for the step 49 overlay (no cluster required)."""
import json
import re
import unittest
from pathlib import Path
from jinja2 import Environment, StrictUndefined
import yaml

ROOT = Path(__file__).resolve().parents[1]
ROLE = ROOT / 'roles/data_pipeline_olist_observability'
DASHBOARD = ROLE / 'templates/olist-pipeline-overview.json.j2'
TASKS = ROLE / 'tasks/main.yml'
PLAY = ROOT / 'playbooks/49-olist-observability.yml'


def dashboard(version='v1'):
    rendered = Environment(undefined=StrictUndefined).from_string(DASHBOARD.read_text()).render(
        data_pipeline_olist_observability_warehouse_version=version,
        grafana_datasource_uid='prometheus',
        data_pipeline_olist_observability_openmetadata_url='http://metadata:8585',
        data_pipeline_olist_observability_console_url='http://worker3:1337',
    )
    return json.loads(rendered)

class OlistObservabilityTests(unittest.TestCase):
    def test_package_paths(self):
        for p in [PLAY, TASKS, DASHBOARD, ROLE/'defaults/main.yml',
                  ROOT/'docs/49-olist-apply.md', ROOT/'docs/49-olist-observability.md']:
            self.assertTrue(p.is_file(), str(p))

    def test_valid_yaml(self):
        for p in (PLAY,TASKS,ROLE/'defaults/main.yml'):
            self.assertTrue(yaml.safe_load(p.read_text()))

    def test_target_only_ops(self):
        play=yaml.safe_load(PLAY.read_text())
        self.assertEqual(play[0]['hosts'],'ops')

    def test_json_dashboard(self):
        doc=dashboard()
        self.assertEqual(doc['uid'],'bigtop-olist-pipeline')
        self.assertGreaterEqual(len(doc['panels']),12)

    def test_no_invented_prometheus_metrics(self):
        doc=dashboard()
        names=set()
        for p in doc['panels']:
            for t in p['targets']:
                names.update(re.findall(r'olist_(?:pipeline|quality)_[a-z_]+',t['expr']))
        expected={'olist_pipeline_last_attempt_success','olist_pipeline_running',
          'olist_pipeline_etl_success','olist_pipeline_quality_success',
          'olist_pipeline_last_attempt_duration_seconds','olist_pipeline_last_success_timestamp_seconds',
          'olist_quality_run_success','olist_quality_checks_failed','olist_quality_checks_total',
          'olist_quality_quarantine_rate','olist_quality_fact_rows','olist_quality_orphan_order_items',
          'olist_quality_last_run_timestamp_seconds','olist_pipeline_last_attempt_timestamp_seconds'}
        self.assertEqual(names,expected)

    def test_version_filter_in_all_targets(self):
        for p in dashboard('v2')['panels']:
            for t in p['targets']:
                self.assertIn('warehouse_version="v2"',t['expr'])

    def test_unique_panel_ids(self):
        ids=[p['id'] for p in dashboard()['panels']]
        self.assertEqual(len(ids),len(set(ids)))

    def test_prometheus_datasource_ref(self):
        for p in dashboard()['panels']:
            self.assertEqual(p['datasource']['uid'],'prometheus')

    def test_link_is_navigation_not_ingestion(self):
        doc=dashboard()
        self.assertTrue(any('OpenMetadata' in l['title'] for l in doc['links']))
        self.assertFalse(any('metadata' in x.lower() for x in [p['title'] for p in doc['panels']]))

    def test_no_faked_missing_series(self):
        text=DASHBOARD.read_text()
        self.assertNotIn('or vector(0)',text)

    def test_no_restarts_or_deletes(self):
        text=TASKS.read_text()
        self.assertNotIn('state: restarted',text)
        self.assertNotIn('state: absent',text)
        self.assertNotIn('hdfs dfs -rm',text)

    def test_requires_existing_grafana_provider(self):
        self.assertIn('Refuse to replace absent Grafana infrastructure',TASKS.read_text())
        self.assertIn('/etc/grafana/provisioning/dashboards/platform.yml',TASKS.read_text())

    def test_no_hardcoded_secret_or_artifacts(self):
        paths=[PLAY,TASKS,DASHBOARD,ROLE/'defaults/main.yml']
        self.assertFalse(any('password=' in p.read_text().lower() for p in paths))
        self.assertFalse(any('token=' in p.read_text().lower() for p in paths))

    def test_dashboard_title(self):
        self.assertIn('Data Quality',dashboard()['title'])

if __name__ == '__main__':
    unittest.main()

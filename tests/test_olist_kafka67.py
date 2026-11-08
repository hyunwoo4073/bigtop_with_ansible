"""Focused read-only 67 monitoring regression checks (no VM services required)."""
from __future__ import annotations
import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock

import yaml
from jinja2 import Template

ROOT = pathlib.Path(__file__).resolve().parents[1]
ROLE = ROOT / 'roles/olist_kafka67'
SPEC = importlib.util.spec_from_file_location('step67_progress', ROLE / 'files/step67_progress.py')
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


class Step67Tests(unittest.TestCase):
    def test_yaml(self):
        paths = list(ROOT.glob('playbooks/*.yml')) + list(ROLE.rglob('*.yml'))
        self.assertTrue(paths)
        for p in paths:
            self.assertTrue(yaml.safe_load(p.read_text()), p)

    def test_offsets_commits_not_latest_uncommitted(self):
        sample = 'Found 3 items\n-rw-r--r-- 2 hadoop hadoop 8 2026-11-07 10:00 /cp/commits/8\n-rw-r--r-- 2 hadoop hadoop 8 2026-11-07 10:00 /cp/commits/10\n-rw-r--r-- 2 hadoop hadoop 8 2026-11-07 10:00 /cp/commits/.10.crc\n'
        self.assertEqual(mod.parse_commit_ids(sample), 10)
        with self.assertRaises(mod.CollectionError):
            mod.parse_commit_ids('Found 0 items')

    def test_parse_kafka_checkpoint_offset(self):
        content = 'v1\n{"batchWatermarkMs":0}\n{"bigtop_olist_step65_cdc_v1":{"0":24}}\n'
        self.assertEqual(mod.parse_committed_offset(content, mod.DEFAULT_TOPIC), 24)
        with self.assertRaises(mod.CollectionError):
            mod.parse_committed_offset(content.replace('"0":24', '"1":24'), mod.DEFAULT_TOPIC)
        with self.assertRaises(mod.CollectionError):
            mod.parse_committed_offset('v1\n{}\n{}', mod.DEFAULT_TOPIC)

    def test_spark_committed_matches_commits_file(self):
        def mock_hdfs(_bin,*args):
            if '-ls' in args:
                return '-rw-r--r-- 2 hadoop hadoop 8 2026-11-07 10:00 /cp/commits/4\n'
            if '-cat' in args:
                self.assertTrue(args[-1].endswith('/offsets/4'))
                return 'v1\n{}\n{"bigtop_olist_step65_cdc_v1":{"0":24}}\n'
            if '-stat' in args:
                self.assertTrue(args[-1].endswith('/commits/4'))
                return '1794042862000\n'
            self.fail('Unexpected HDFS call')
        with mock.patch.object(mod,'hdfs_call',side_effect=mock_hdfs), \
             mock.patch.object(mod.time,'time',return_value=1794043000):
            self.assertEqual(mod.spark_committed(mod.DEFAULT_TOPIC,'hdfs://master:9000/cp','/usr/bin/hdfs'),(4,24,1794042862.0))

    def test_format_success_failure_no_fake_zero_offset(self):
        good=mod.format_metrics('kafka',mod.DEFAULT_TOPIC,100,{'offset':24})
        self.assertIn('kafka_end_offset{topic=',good)
        self.assertIn('collector_success{source="kafka"} 1',good)
        bad=mod.format_metrics('kafka',mod.DEFAULT_TOPIC,101,None)
        self.assertIn('collector_success{source="kafka"} 0',bad)
        self.assertNotIn('kafka_end_offset',bad)
        good2=mod.format_metrics('spark',mod.DEFAULT_TOPIC,100,{'offset':24,'batch':4,'mtime':123.4})
        self.assertIn('spark_committed_batch_id 4',good2)
        self.assertIn('spark_last_commit_timestamp_seconds 123.400',good2)

    def test_atomic_metrics_failure(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d)/'x.prom'
            p.write_text('stale=1')
            with mock.patch.object(mod,'kafka_latest',side_effect=mod.CollectionError('no broker')):
                self.assertEqual(mod.main(['--source','kafka','--metrics-file',str(p)]),1)
            self.assertIn('collector_success{source="kafka"} 0',p.read_text())
            self.assertNotIn('stale=1',p.read_text())

    def test_kafka_latest_exactly_one_partition(self):
        with mock.patch.object(mod.Path, 'is_file', return_value=True), \
             mock.patch.object(mod, 'call', return_value=mod.DEFAULT_TOPIC + ':0:24\n') as cmd:
            self.assertEqual(mod.kafka_latest(mod.DEFAULT_TOPIC, mod.DEFAULT_BOOTSTRAP), 24)
            self.assertEqual(cmd.call_args.kwargs['env']['JAVA_HOME'], mod.JAVA17)
            self.assertIn('-1', cmd.call_args.args[0])
        with mock.patch.object(mod.Path, 'is_file', return_value=True), \
             mock.patch.object(mod, 'call', return_value=mod.DEFAULT_TOPIC + ':0:24\n' + mod.DEFAULT_TOPIC + ':1:5\n'):
            with self.assertRaises(mod.CollectionError):
                mod.kafka_latest(mod.DEFAULT_TOPIC, mod.DEFAULT_BOOTSTRAP)

    def test_checkpoint_future_mtime_rejected(self):
        def mock_hdfs(_bin, *args):
            if '-ls' in args:
                return '-rw-r--r-- 2 hadoop hadoop 4 2026-11-07 10:00 /cp/commits/7\n'
            if '-cat' in args:
                return 'v1\n{}\n{"bigtop_olist_step65_cdc_v1":{"0":24}}\n'
            return '999999999999999\n'
        with mock.patch.object(mod, 'hdfs_call', side_effect=mock_hdfs):
            with self.assertRaises(mod.CollectionError):
                mod.spark_committed(mod.DEFAULT_TOPIC, mod.DEFAULT_CHECKPOINT, '/usr/bin/hdfs')

    def test_spark_failure_has_no_committed_offset_metric(self):
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / 'spark.prom'
            with mock.patch.object(mod, 'spark_committed', side_effect=mod.CollectionError('HDFS outage')):
                self.assertEqual(mod.main(['--source', 'spark', '--metrics-file', str(path)]), 1)
            self.assertIn('collector_success{source="spark"} 0', path.read_text())
            self.assertNotIn('spark_committed_offset', path.read_text())

    def test_dedicated_java_and_readonly_apis(self):
        source=(ROLE/'files/step67_progress.py').read_text()
        for phrase in ['kafka-get-offsets.sh','--time',"'-1'", "'-ls'", "'-cat'", "'-stat'", "'/commits'", "'/offsets/'", "'-u', 'hadoop'"]:
            self.assertIn(phrase,source)
        self.assertNotIn('spark-submit',source)
        self.assertNotIn('kafka-console-consumer',source)
        self.assertNotIn('yarn application -kill',source)

    def test_install_disabled_default(self):
        defaults=yaml.safe_load((ROLE/'defaults/main.yml').read_text())
        self.assertEqual(defaults['olist_kafka67_mode'],'plan')
        install=(ROLE/'tasks/install.yml').read_text()
        self.assertIn('state: stopped',install)
        self.assertIn('enabled: false',install)
        self.assertNotIn('daemon_reload: true\n  become: true\n  when: false',install)

    def test_no_deleting_platform_resources(self):
        task='\n'.join(p.read_text() for p in (ROLE/'tasks').glob('*.yml'))
        for forbidden in ['kafka-topics.sh --delete','spark-submit','hdfs dfs -rm','state: absent','reformat', 'kafka_kraft64']:
            self.assertNotIn(forbidden,task)
        self.assertIn('hosts: master,worker3',(ROOT/'playbooks/67-kafka-spark-progress.yml').read_text())

    def test_grafana_render_json(self):
        template=(ROLE/'templates/olist-step67-progress.json.j2').read_text()
        rendered=Template(template).render(olist_kafka67_datasource_uid='prometheus')
        obj=json.loads(rendered)
        self.assertEqual(obj['uid'],'bigtop-olist-step67')
        self.assertGreaterEqual(len(obj['panels']),8)
        expr='\n'.join(x['targets'][0]['expr'] for x in obj['panels'])
        self.assertIn('on (topic, partition)',expr)
        self.assertIn('spark_committed_offset',expr)
        self.assertIn('kafka_end_offset',expr)
        self.assertIn('spark_last_commit_timestamp_seconds',expr)

    def test_systemd_timer_not_automatically_on(self):
        template=(ROLE/'templates/bigtop-olist-step67.timer.j2').read_text()
        self.assertIn('OnUnitActiveSec=60s',template)
        self.assertNotIn('OnCalendar=',template)

    def test_no_binaries(self):
        self.assertFalse(any(p.suffix in ('.jar','.zip','.tar','.tgz','.pyc') for p in ROLE.rglob('*') if p.is_file() and '__pycache__' not in p.parts))


if __name__=='__main__':
    unittest.main()

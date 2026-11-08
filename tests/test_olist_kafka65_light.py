"""Unit tests for Step65 CLI-only Kafka and Trino/HDFS operation."""
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

import yaml

ROOT = Path(__file__).resolve().parents[1]
FILES = ROOT / 'roles/olist_kafka65/files'
sys.path.insert(0, str(FILES))


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, file)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


app = load('olist_kafka65', FILES / 'olist_kafka65.py')
light = load('step65_light_tests', FILES / 'step65_light.py')


def fixture():
    rows = [('sample_%04d' % i, 1, '10.00', '2.00', '12.00', 300, False)
            for i in range(512)]
    rows += [('deleted_%03d' % i, 1, '10.00', '2.00', '12.00', 300, True)
             for i in range(8)]
    return rows


class Step65Light(unittest.TestCase):
    def test_event_fixture_unchanged(self):
        lines = light.produce_lines(fixture()).splitlines()
        self.assertEqual(len(lines), 12)
        self.assertEqual(len(app.newest([json.loads(x.split('|', 1)[1]) for x in lines])), 4)

    def test_offset_line(self):
        self.assertEqual(light.parse_offset_line(light.TOPIC + ':0:24'), 24)
        for value in ['garbage', light.TOPIC + ':1:24', light.TOPIC + ':0:-2',
                      light.TOPIC + ':0:12\n' + light.TOPIC + ':1:0']:
            with self.assertRaises(light.LightError):
                light.parse_offset_line(value)

    def test_kafka_multi_part_rejected(self):
        output = light.TOPIC + ':0:12\n' + light.TOPIC + ':1:0'
        with patch.object(light, 'kafka_run', return_value=output):
            with self.assertRaises(light.LightError):
                light.kafka_offset('host:9092', -1)

    def test_producer_proof(self):
        seed = {'source_snapshot': '123'}
        proof = light.publish_proof(seed)
        self.assertTrue(light.proof_valid(proof, seed))
        self.assertFalse(light.proof_valid({**proof, 'messages': 13}, seed))

    def test_checkpoint_commits_only(self):
        def fake_hdfs(*args):
            if '-ls' in args:
                return (b'Found 2 items\n-rw-r--r-- a b 1 date 0 ' +
                        app.CHECKPOINT.encode() + b'/commits/0\n' +
                        b'-rw-r--r-- a b 1 date 1 ' + app.CHECKPOINT.encode() + b'/commits/1\n')
            return b'v1\n{}\n{"bigtop_olist_step65_cdc_v1":{"0":12}}\n'
        with patch.object(app, 'hdfs', side_effect=fake_hdfs):
            self.assertEqual(light.checkpoint_end(), 12)

    def test_legacy_yarn_accepted_refused(self):
        sample = ('application_10_0003 olist-kafka65-produce SPARK hadoop batch ACCEPTED x\n')
        with patch.object(light.subprocess, 'run', return_value=Mock(returncode=0, stdout=sample)):
            with self.assertRaisesRegex(light.LightError, 'Legacy Spark'):
                light.check_legacy_yarn_job()

    def test_legacy_yarn_other_jobs_not_killed(self):
        sample = 'application_10_0003 olist-iceberg-kafka65-continuous SPARK hadoop batch RUNNING x\n'
        with patch.object(light.subprocess, 'run', return_value=Mock(returncode=0, stdout=sample)):
            light.check_legacy_yarn_job()

    def test_reconcile_only_exact_message_count(self):
        with patch.object(light, 'kafka_offset', side_effect=[0, 11]):
            with self.assertRaisesRegex(light.LightError, 'exactly 12'):
                light.reconcile('host:9092', fixture(), {'source_snapshot':'42'})

    def test_reconcile_compares_values(self):
        payload = '\n'.join(line.split('|', 1)[1]
                            for line in light.produce_lines(fixture()).splitlines()) + '\n'
        with patch.object(light, 'kafka_offset', side_effect=[0, 12]), \
             patch.object(light, 'kafka_run', return_value=payload):
            proof = light.reconcile('host:9092', fixture(), {'source_snapshot': '42'})
            self.assertEqual(proof['messages'], 12)

    def test_replay_requires_immutable_apply_and_single_pass(self):
        seed = {'status': 'PASS', 'schema': 'bigtop.olist.kafka-stream-seed.v1',
                'table': app.TARGET, 'rows': 520, 'source_snapshot': '42',
                'source_digest': 'abc'}
        proof = light.publish_proof(seed)
        applied = {'status': 'PASS', 'schema': light.APPLY_SCHEMA,
                   'main_snapshot': '777', 'digest_sha256': 'def'}
        env = {'seed': seed, 'produce': proof, 'apply': applied}
        with patch.object(light, 'validate_trino'), \
             patch.object(light, 'check_source', return_value=fixture()), \
             patch.object(light, 'trino_snapshot', return_value='777'), \
             patch.object(light, 'trino_rows', return_value=[]), \
             patch.object(light.core, 'digest', return_value='def'), \
             patch.object(light, 'kafka_offset', side_effect=[24, 0]):
            with self.assertRaisesRegex(light.LightError, 'permitted only once'):
                light.publisher('replay', 'host:9092', light.TRINO, env)

    def test_verify_writes_apply_proof_without_spark(self):
        source = fixture()
        expected = app.expected_after(source)
        seed = {'status': 'PASS', 'schema': 'bigtop.olist.kafka-stream-seed.v1',
                'table': app.TARGET, 'rows': 520, 'source_snapshot': '42',
                'source_digest': app.digest(source)}
        existing = light.publish_proof(seed)
        def proofs(path):
            return seed if path == app.SEED_PROOF else existing
        with patch.object(light, 'validate_trino'), \
             patch.object(light, 'check_source', return_value=source), \
             patch.object(light, 'trino_rows', return_value=expected), \
             patch.object(light, 'trino_snapshot', return_value='900'), \
             patch.object(light, 'checkpoint_end', return_value=12), \
             patch.object(light.core, 'read_proof', side_effect=proofs), \
             patch.object(light.core, 'exists', return_value=False), \
             patch.object(light.core, 'write_proof') as write:
            light.verifier(light.TRINO, light.TOPIC + ':0:12')
            write.assert_called_once()
            self.assertEqual(write.call_args.args[0], app.APPLY_PROOF)
            self.assertEqual(write.call_args.args[1]['rows'], 521)

    def test_verify_rejects_uncommitted_kafka_events(self):
        source = fixture()
        expected = app.expected_after(source)
        seed = {'status': 'PASS', 'schema': 'bigtop.olist.kafka-stream-seed.v1',
                'table': app.TARGET, 'rows': 520, 'source_snapshot': '42',
                'source_digest': app.digest(source)}
        existing = light.publish_proof(seed)
        def proofs(path):
            return seed if path == app.SEED_PROOF else existing
        with patch.object(light, 'validate_trino'), \
             patch.object(light, 'check_source', return_value=source), \
             patch.object(light, 'trino_rows', return_value=expected), \
             patch.object(light, 'checkpoint_end', return_value=11), \
             patch.object(light.core, 'read_proof', side_effect=proofs), \
             patch.object(light.core, 'write_proof') as write:
            with self.assertRaisesRegex(light.LightError, 'has not committed'):
                light.verifier(light.TRINO, light.TOPIC + ':0:12')
            write.assert_not_called()

    def test_cli_never_runs_spark(self):
        content = (FILES / 'step65_light.py').read_text()
        self.assertNotIn('spark-submit', content)
        self.assertNotIn('SparkSession.builder', content)
        self.assertIn('core.CHECKPOINT', content)
        self.assertIn('core.APPLY_PROOF', content)

    def test_no_reseed_or_delete(self):
        content = (ROOT / 'roles/olist_kafka65/tasks/light_install.yml').read_text().lower()
        self.assertNotIn('seed', content)
        self.assertNotIn('ansible.builtin.service', content)
        self.assertNotIn('rm -r', content)

    def test_playbook_stages_once_no_yarn_submit_in_light(self):
        text = (ROOT / 'roles/olist_kafka65/tasks/light_run.yml').read_text()
        self.assertNotIn('spark-submit', text)
        self.assertIn('STEP65', text)
        self.assertIn('delegate_to: worker3', text)

    def test_anisble_yaml_and_modes(self):
        for name in ['main.yml','run.yml','light_install.yml','light_run.yml']:
            self.assertIsInstance(yaml.safe_load((ROOT / 'roles/olist_kafka65/tasks' / name).read_text()), list)
        self.assertIn('upgrade', (ROOT / 'roles/olist_kafka65/tasks/main.yml').read_text())

    def test_kafka42_no_deprecated_flags(self):
        content = (FILES / 'step65_light.py').read_text()
        self.assertIn('--reader-property', content)
        self.assertIn('--command-property', content)
        self.assertNotIn('--producer-property', content)


if __name__ == '__main__':
    unittest.main()

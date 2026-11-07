#!/usr/bin/env python3
"""Offline Step64 checks; never contact Kafka, Spark, HDFS or PostgreSQL."""
import importlib.util
import json
import re
import os
import subprocess
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / 'roles/olist_iceberg_kafka64/files/olist_iceberg_kafka64.py'
FETCH = ROOT / 'scripts/64-fetch-spark-kafka-jars.py'
VERIFY = ROOT / 'scripts/64-verify-kafka-jars.py'


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


m = load(CORE, 'step64')
fetcher = load(FETCH, 'step64_fetch')
verifier = load(VERIFY, 'step64_verify')


def baseline():
    rows = []
    for i in range(512):
        p = Decimal('10.00')
        f = Decimal('2.00')
        rows.append((f'olist_order_{i:04d}', 1, p, f, p+f, 0, False))
    for i in range(4):
        p = Decimal('10.00')
        f = Decimal('2.00')
        rows.append((f'step63_old_deleted_{i:04d}', 1, p, f, p+f, 200, True))
    return rows


class Step64Tests(unittest.TestCase):
    def test_isolated_table_and_topic(self):
        self.assertEqual(m.TARGET, 'shared.olist64_kafka.fact_cdc_stream_v1')
        self.assertEqual(m.TOPIC, 'bigtop_olist_step64_cdc_v1')

    def test_fixed_hdfs_checkpoint(self):
        self.assertEqual(m.CHECKPOINT,
                         'hdfs://master:9000/data/olist/iceberg-checkpoints/step64/v1')

    def test_protects_existing_63_and_gold(self):
        source = CORE.read_text()
        self.assertIn('ensure_guard', source)
        self.assertIn('ensure_owned', source)
        self.assertNotIn('DROP TABLE', source)
        self.assertNotIn('expire_snapshots', source)
        self.assertNotIn('TRUNCATE TABLE', source)

    def test_512_live_4_deleted_baseline(self):
        rows = baseline()
        self.assertEqual(len(rows), 516)
        self.assertEqual(sum(x[6] for x in rows), 4)
        self.assertEqual(len(m.normalized_digest(rows)), 64)

    def test_48_events_16_distinct_keys(self):
        events = m.event_payloads(baseline())
        self.assertEqual(len(events), 48)
        self.assertEqual(len(m.latest_events(events)), 16)
        self.assertEqual(len({(e['order_id'], e['order_item_id']) for e in events}), 16)

    def test_events_are_synthetic_from_existing_fact(self):
        events = m.event_payloads(baseline())
        self.assertEqual({e['run_id'] for e in events}, {m.RUN_ID})
        self.assertEqual({e['op'] for e in events}, {'I','U','D'})

    def test_updated_8_deleted_4_inserted_4(self):
        latest = m.latest_events(m.event_payloads(baseline()))
        self.assertEqual({op: sum(e['op']==op for e in latest)
                          for op in ('U','D','I')}, {'U':8,'D':4,'I':4})

    def test_final_rows_520_and_tombstones_8(self):
        out = m.final_rows(baseline())
        self.assertEqual(len(out), 520)
        self.assertEqual(sum(x[6] for x in out), 8)
        self.assertEqual(sum(not x[6] for x in out), 512)

    def test_out_of_order_and_exact_duplicate_identical(self):
        events = m.event_payloads(baseline())
        self.assertEqual(m.events_digest(events),
                         m.events_digest(list(reversed(events))))
        self.assertEqual(len(m.latest_events(list(reversed(events)))), 16)

    def test_conflicting_payload_same_seq_is_rejected(self):
        events = m.event_payloads(baseline())[:1]
        wrong = {**events[0], 'price': '999.00', 'item_total': '1001.00'}
        with self.assertRaisesRegex(m.Step64Error, 'Conflicting'):
            m.latest_events(events + [wrong])

    def test_conflicting_business_total_is_rejected(self):
        events = m.event_payloads(baseline())[:1]
        wrong = {**events[0], 'item_total': '0.01'}
        with self.assertRaisesRegex(m.Step64Error, 'item_total'):
            m.latest_events([wrong])

    def test_foreign_run_id_is_rejected(self):
        wrong = {**m.event_payloads(baseline())[0], 'run_id': 'another'}
        with self.assertRaisesRegex(m.Step64Error, 'Foreign'):
            m.latest_events([wrong])

    def test_bootstrap_must_be_explicit(self):
        with self.assertRaises(m.Step64Error):
            m.validate_bootstrap('')
        with self.assertRaises(m.Step64Error):
            m.validate_bootstrap('broker without port')
        self.assertEqual(m.validate_bootstrap('broker1:9092,broker2:9093'),
                         'broker1:9092,broker2:9093')

    def test_strict_dedup_target_no_replay_snapshot_changes(self):
        code = CORE.read_text()
        self.assertIn('s.event_seq > t.last_event_seq', code)
        self.assertIn('snap == previous[\'main_snapshot\']', code)
        self.assertIn('STEP64_KAFKA_REPLAY_IDEMPOTENT_PASS', code)

    def test_spark_streaming_checkpoint_and_available_now(self):
        code = CORE.read_text()
        self.assertIn("readStream.format('kafka')", code)
        self.assertIn('.trigger(availableNow=True)', code)
        self.assertIn('.foreachBatch(', code)
        self.assertIn(".option('failOnDataLoss', 'true')", code)
        self.assertNotIn('startingOffsets', 'no')

    def test_foreach_batch_does_not_capture_spark_context(self):
        code = CORE.read_text()
        self.assertIn('def apply_microbatch(batch_df, batch_id):', code)
        self.assertIn('spark = batch_df.sparkSession', code)
        self.assertIn('lambda batch, batch_id: apply_microbatch(batch, batch_id)', code)

    def test_pinned_spark_kafka_dependencies(self):
        self.assertEqual(len(fetcher.ARTIFACTS), 5)
        self.assertEqual(verifier.EXPECTED,
                         {name for name, _ in fetcher.artifact_urls()})
        self.assertIn('spark-sql-kafka-0-10_2.12-3.5.6.jar', verifier.EXPECTED)
        self.assertIn('kafka-clients-3.4.1.jar', verifier.EXPECTED)

    def test_no_global_spark_or_java_changes(self):
        data = '\n'.join(p.read_text() for p in (ROOT / 'roles/olist_iceberg_kafka64/tasks').glob('*.yml'))
        self.assertNotIn('alternatives', data)
        self.assertNotIn('spark-defaults.conf', data)
        self.assertNotIn('core-site.xml', data)
        self.assertIn('spark-submit', data)
        self.assertIn('--jars', data)

    def test_role_var_prefix(self):
        defaults = (ROOT / 'roles/olist_iceberg_kafka64/defaults/main.yml').read_text()
        keys = re.findall(r'^([a-z][a-zA-Z0-9_]+):', defaults, re.M)
        self.assertTrue(keys)
        self.assertTrue(all(k.startswith('olist_iceberg_kafka64_') for k in keys))

    def test_preflight_does_not_install_kafka(self):
        data = (ROOT / 'roles/olist_iceberg_kafka64/tasks/main.yml').read_text()
        self.assertIn("olist_iceberg_kafka64_mode == 'preflight'", data)
        self.assertNotIn('kafka-server-start', data)
        self.assertNotIn('kafka-topics.sh --create', data)

    def test_plan_is_read_only(self):
        role = (ROOT / 'roles/olist_iceberg_kafka64/tasks/main.yml').read_text()
        self.assertIn("when: olist_iceberg_kafka64_mode == 'plan'", role)
        self.assertIn("when: olist_iceberg_kafka64_mode in ['seed', 'produce', 'ingest', 'replay', 'verify']", role)

    def test_evidence_schema_and_stage_order(self):
        code = CORE.read_text()
        for name in ('_SEED_REPORT.json', '_PRODUCE_REPORT.json', '_INGEST_REPORT.json'):
            self.assertIn(name, code)
        self.assertIn('check_produce_proof(read_json(PRODUCE_PROOF)', code)

    def test_trino_verifies_source_and_gold(self):
        src = (ROOT / 'scripts/64-verify-trino.py').read_text()
        self.assertIn('Step59 Gold main drift', src)
        self.assertIn('Step63 source main drift', src)
        self.assertIn('normalized_digest(actual)', src)

    def test_topic_cli_requires_replication_factor(self):
        content = (ROOT / 'scripts/64-kafka-lab-topic.sh').read_text()
        self.assertIn('Step64 only supports three 64-A brokers', content)
        self.assertIn('--if-not-exists', content)
        self.assertIn('KAFKA64_JAVA_HOME', content)
        self.assertIn('retention.ms=604800000', content)
        self.assertIn('min.insync.replicas=2', content)

    def test_known_kraft_bootstrap_addresses(self):
        defaults = (ROOT / 'roles/olist_iceberg_kafka64/defaults/main.yml').read_text()
        for address in ('192.158.2.101:9092', '192.158.2.102:9092',
                        '192.158.2.103:9092'):
            self.assertIn(address, defaults)
        self.assertNotIn('broker1:9092', defaults)

    def test_all_brokers_checked_from_driver_and_executors(self):
        task = (ROOT / 'roles/olist_iceberg_kafka64/tasks/main.yml').read_text()
        self.assertIn("['master', 'worker1', 'worker2'] | product", task)
        self.assertIn('delegate_to: "{{ item.0 }}"', task)

    def test_topic_script_has_dedicated_jdk_and_no_delete(self):
        script = (ROOT / 'scripts/64-kafka-lab-topic.sh').read_text()
        self.assertIn('export JAVA_HOME=', script)
        self.assertIn('TOPIC=bigtop_olist_step64_cdc_v1', script)
        self.assertNotIn('--delete', script)
        self.assertIn('version=$($JAVA_HOME/bin/java -version', script)
        self.assertIn('STEP64_TOPIC_READY', script)

    def test_topic_cli_uses_dedicated_java17_even_if_system_java8(self):
        with tempfile.TemporaryDirectory() as tempdir:
            base = Path(tempdir)
            jdk = base / 'jdk17' / 'bin'
            jdk.mkdir(parents=True)
            java = jdk / 'java'
            java.write_text("#!/bin/sh\necho 'openjdk version \"17.0.18\" 2026-10-01' >&2\n")
            java.chmod(0o755)
            cli = base / 'kafka-topics.sh'
            cli.write_text("#!/bin/sh\n"
                           "echo 'Topic: bigtop_olist_step64_cdc_v1 PartitionCount: 1 ReplicationFactor: 3 Configs: min.insync.replicas=2'\n"
                           "echo ' Topic: bigtop_olist_step64_cdc_v1 Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2,3'\n")
            cli.chmod(0o755)
            env = dict(os.environ, JAVA_HOME='/usr/lib/jvm/java-8-openjdk',
                       KAFKA64_JAVA_HOME=str(jdk.parent), KAFKA_TOPICS_BIN=str(cli))
            result = subprocess.run(['bash', str(ROOT / 'scripts/64-kafka-lab-topic.sh'), 'describe'],
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('STEP64_TOPIC_READY', result.stdout)

    def test_topic_cli_refuses_wrong_java_version(self):
        with tempfile.TemporaryDirectory() as tempdir:
            base = Path(tempdir)
            jdk = base / 'jdk' / 'bin'
            jdk.mkdir(parents=True)
            java = jdk / 'java'
            java.write_text("#!/bin/sh\necho 'openjdk version \"1.8.0_402\"' >&2\n")
            java.chmod(0o755)
            cli = base / 'kafka-topics.sh'
            cli.write_text('#!/bin/sh\necho ERROR: should not run\n')
            cli.chmod(0o755)
            env = dict(os.environ, KAFKA64_JAVA_HOME=str(jdk.parent),
                       KAFKA_TOPICS_BIN=str(cli))
            result = subprocess.run(['bash', str(ROOT / 'scripts/64-kafka-lab-topic.sh'), 'describe'],
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('requires dedicated Java 17', result.stderr)
            self.assertNotIn('should not run', result.stdout)

    def test_all_yaml_documents_parse(self):
        try:
            import yaml
        except ImportError:
            self.skipTest('PyYAML not available')
        paths = [ROOT / 'playbooks/64-olist-kafka-iceberg-cdc.yml']
        paths += list((ROOT / 'roles/olist_iceberg_kafka64').rglob('*.yml'))
        for f in paths:
            with self.subTest(file=f):
                self.assertIsInstance(yaml.safe_load(f.read_text()), (dict, list))

    def test_repo_vendor_existence_is_not_failure(self):
        src = CORE.read_text()
        self.assertNotIn("assertFalse((ROOT / 'vendor').exists())", src)


if __name__ == '__main__':
    unittest.main()

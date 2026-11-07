"""Step65 isolated CDC + lifecycle regression; no network/Spark required."""
import importlib.util
import sys
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'roles/olist_kafka65/files'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


app = load('olist_kafka65_unit', SRC / 'olist_kafka65.py')
yarn = load('step65_yarn_unit', SRC / 'step65_yarn_control.py')


def fixture_rows():
    result = [('sample_%04d' % i, 1, '10.00', '2.00', '12.00', 300, False)
              for i in range(512)]
    result += [('deleted_%03d' % i, 1, '10.00', '2.00', '12.00', 300, True)
               for i in range(8)]
    return result


class Step65Tests(unittest.TestCase):
    def test_input_counts(self):
        self.assertEqual(len(fixture_rows()), 520)
        self.assertEqual(len(app.events_for(fixture_rows())), 12)

    def test_four_unique_keys(self):
        events = app.events_for(fixture_rows())
        self.assertEqual(len(app.newest(events)), 4)

    def test_final_state(self):
        after = app.expected_after(fixture_rows())
        self.assertEqual(len(after), 521)
        self.assertEqual(sum(r[6] for r in after), 9)
        self.assertEqual(sum(not r[6] for r in after), 512)

    def test_event_replay_is_idempotent_digest(self):
        base = fixture_rows()
        after = app.expected_after(base)
        events = app.newest(app.events_for(base))
        state = {(r[0], r[1]): r for r in after}
        for e in events:
            key, seq, _ = app.validate_event(e)
            self.assertGreaterEqual(state[key][5], seq)
        self.assertEqual(app.digest(after), app.digest(list(state.values())))

    def test_conflicting_duplicate_sequence_rejected(self):
        e = app.events_for(fixture_rows())[0]
        changed = {**e, 'price': '99.00', 'item_total': '101.00'}
        with self.assertRaises(app.GuardError):
            app.newest([e, changed])

    def test_foreign_run_rejected(self):
        e = {**app.events_for(fixture_rows())[0], 'run_id': 'other'}
        with self.assertRaises(app.GuardError):
            app.validate_event(e)

    def test_wrong_amount_rejected(self):
        e = {**app.events_for(fixture_rows())[0], 'item_total': '0.00'}
        with self.assertRaises(app.GuardError):
            app.validate_event(e)

    def test_multiple_active_yarn_apps_fails_closed(self):
        sample = ("application_111_001 olist-iceberg-kafka65-continuous SPARK hadoop batch RUNNING UNDEFINED 0% x\n"
                  "application_111_002 olist-iceberg-kafka65-continuous SPARK hadoop batch RUNNING UNDEFINED 0% x")
        original = yarn.execute
        try:
            yarn.execute = lambda *args, **kwargs: sample
            with self.assertRaises(yarn.StateError):
                yarn.active_apps()
        finally:
            yarn.execute = original

    def test_yarn_exact_name_not_substring(self):
        sample = ("application_111_001 olist-iceberg-kafka65-continuous-other SPARK hadoop batch RUNNING UNDEFINED 0% x\n"
                  "application_111_002 olist-iceberg-kafka65-continuous SPARK other batch RUNNING UNDEFINED 0% x")
        original = yarn.execute
        try:
            yarn.execute = lambda *args, **kwargs: sample
            self.assertEqual(yarn.active_apps(), [])
        finally:
            yarn.execute = original

    def test_yarn_only_one_exact_app(self):
        sample = 'application_111_001 olist-iceberg-kafka65-continuous SPARK hadoop batch RUNNING UNDEFINED 0% x'
        original = yarn.execute
        try:
            yarn.execute = lambda *args, **kwargs: sample
            self.assertEqual(yarn.active_apps(), [('application_111_001', 'RUNNING')])
        finally:
            yarn.execute = original

    def test_stream_trigger_and_checkpoint(self):
        text = (SRC / 'olist_kafka65.py').read_text()
        self.assertIn("trigger(processingTime='10 seconds')", text)
        self.assertIn(".option('failOnDataLoss', 'true')", text)
        self.assertIn(".option('maxOffsetsPerTrigger', '500')", text)
        self.assertIn('checkpointLocation', text)
        self.assertIn('/step65/v1', text)

    def test_checkpoint_committed_offset_verified(self):
        text = (SRC / 'olist_kafka65.py').read_text()
        self.assertIn("CHECKPOINT + '/commits'", text)
        self.assertIn("CHECKPOINT + '/offsets/'", text)
        self.assertIn('committed_end >= kafka_end', text)

    def test_no_destructive_data_mutation(self):
        text = (SRC / 'olist_kafka65.py').read_text().upper()
        for banned in ('DROP TABLE', 'DELETE FROM', 'TRUNCATE', 'EXPIR_SNAPSHOTS'):
            self.assertNotIn(banned, text)

    def test_no_global_java_change_or_service_restart(self):
        content = '\n'.join(p.read_text() for p in
                            (ROOT / 'roles/olist_kafka65/tasks').glob('*.yml'))
        self.assertNotIn('update-alternatives', content)
        self.assertNotIn('systemctl restart', content)

    def test_single_yarn_application_name(self):
        self.assertEqual(app.APP_NAME, yarn.NAME)
        self.assertEqual(yarn.OWNER, 'hadoop')

    def test_yarn_submit_cluster_nonblocking(self):
        text = (SRC / 'step65_yarn_control.py').read_text()
        self.assertIn('spark.yarn.submit.waitAppCompletion=false', text)
        self.assertIn("'--deploy-mode', 'cluster'", text)

    def test_exact_id_kill_only(self):
        text = (SRC / 'step65_yarn_control.py').read_text()
        self.assertIn("execute('yarn', 'application', '-kill', target)", text)
        self.assertNotIn('application -kill $(grep', text)

    def test_topic_jdk17(self):
        text = (ROOT / 'scripts/65-kafka-topic.sh').read_text()
        self.assertIn('export JAVA_HOME=/opt/bigtop-kafka-kraft64/jdk17', text)
        self.assertIn('min.insync.replicas=2', text)
        self.assertIn('bigtop_olist_step65_cdc_v1', text)

    def test_yaml_all_parse(self):
        for path in list(ROOT.glob('playbooks/*.yml')) + list(ROOT.glob('roles/**/*.yml')):
            with self.subTest(path=path):
                self.assertIsNotNone(yaml.safe_load(path.read_text()))

    def test_explicit_apply_only(self):
        text = (ROOT / 'roles/olist_kafka65/tasks/main.yml').read_text()
        self.assertIn('olist_kafka65_mode == \'apply\'', text)
        self.assertIn('olist_kafka65_mode == \'plan\'', text)

    def test_playbook_isolated_master(self):
        play = yaml.safe_load((ROOT / 'playbooks/65-kafka-streaming-operations.yml').read_text())[0]
        self.assertEqual(play['hosts'], 'master')

    def test_binaries_not_in_step65_bundle(self):
        step65_files = [p for d in ['roles/olist_kafka65', 'scripts', 'playbooks', 'tests', 'docs']
                        for p in (ROOT / d).rglob('*') if p.is_file()]
        self.assertFalse(any(p.suffix in ('.jar', '.tar', '.tgz', '.zip')
                             for p in step65_files))


    def test_trino_verifier_pins_catalog_and_snapshot(self):
        text = (ROOT / 'scripts/65-verify-trino.py').read_text()
        self.assertIn('iceberg_rest.olist65_stream.fact_cdc_running_v1', text)
        self.assertIn('snapshot(url, TARGET)', text)
        self.assertIn('STEP65_SPARK_KAFKA_ICEBERG_TRINO_PASS', text)

    def test_step65_evidence_export_paths_are_local(self):
        text = (ROOT / 'roles/olist_kafka65/tasks/run.yml').read_text()
        self.assertIn('delegate_to: localhost', text)
        self.assertIn('65-kafka-cdc-', text)
        self.assertIn("olist_kafka65_mode == 'verify'", text)


if __name__ == '__main__':
    unittest.main()

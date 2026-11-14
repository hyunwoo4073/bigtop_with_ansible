"""No Spark required: unit tests for Step72 fixture and recovery guardrails."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / 'roles/olist_kafka72/files/step72_cdc.py'
spec = importlib.util.spec_from_file_location('step72_cdc_test', SCRIPT)
core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(core)


class TestStep72PureLogic(unittest.TestCase):
    def test_isolated_assets(self):
        self.assertIn('step72', core.TOPIC)
        self.assertIn('olist72', core.TARGET)
        self.assertIn('step72', core.CHECKPOINT)
        self.assertIn('step72', core.FAULT_PROOF)
        self.assertNotIn('step65', core.CHECKPOINT)

    def test_fixture_is_nine_events(self):
        self.assertEqual(len(core.fixture_events()), 9)

    def test_expected_three_row_state(self):
        rows = core.expected()
        self.assertEqual(len(rows), 3)
        self.assertEqual(sum(not r[5] for r in rows), 2)
        self.assertEqual(sum(r[5] for r in rows), 1)

    def test_event_order_and_duplicates_do_not_change_result(self):
        records = core.fixture_events()
        self.assertEqual(core.normalized(core.fold(records).values()),
                         core.normalized(core.fold(list(reversed(records))).values()))

    def test_fold_rejects_conflicting_sequence(self):
        event = core.fixture_events()[0]
        bad = dict(event, price='100.00', item_total='102.00')
        with self.assertRaises(core.GuardError):
            core.fold([event, bad])

    def test_digest_stable(self):
        self.assertEqual(core.digest(core.expected()), core.digest(list(reversed(core.expected()))))

    def test_fault_proof_requires_correct_batch(self):
        wrong = json.dumps({'phase':'after_iceberg_merge_before_spark_batch_commit',
                            'topic':core.TOPIC,'table':core.TARGET,'checkpoint':core.CHECKPOINT,
                            'batch_id':1})
        with mock.patch.object(core, 'hdfs_exists', return_value=True), mock.patch.object(core, 'hdfs', return_value=wrong):
            with self.assertRaises(core.GuardError):
                core.fault_proof()

    def test_fault_commit_guard(self):
        proof = {'batch_id': 0}
        def exists(path):
            return path.endswith('/commits/0')
        with mock.patch.object(core,'hdfs_exists',side_effect=exists):
            with self.assertRaises(core.GuardError):
                core.assert_uncommitted_fault_batch(proof)

    def test_fault_offsets_guard(self):
        proof = {'batch_id': 0}
        with mock.patch.object(core,'hdfs_exists',return_value=False):
            with self.assertRaises(core.GuardError):
                core.assert_uncommitted_fault_batch(proof)

    def test_replay_noop_predicate(self):
        rows = core.expected()
        saved = {r[0]:r for r in rows}
        latest = core.fold(core.fixture_events())
        pending = [e for name,e in sorted(latest.items())
                   if name not in saved or e[1] > saved[name][1]]
        self.assertEqual(pending,[])


if __name__ == '__main__':
    unittest.main()

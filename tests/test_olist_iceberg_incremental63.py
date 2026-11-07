"""Step 63 offline tests. No HDFS/Spark/Trino install required."""
import importlib.util
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import unittest
from unittest import mock
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPARK = ROOT / 'roles/olist_iceberg_incremental63/files/olist_iceberg_incremental63.py'
TRINO = ROOT / 'scripts/63-verify-trino.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


s = load('step63_spark', SPARK)
t = load('step63_trino', TRINO)


def baseline(n=s.ROWS):
    return [(f'olist-order-{i:04d}', 1, Decimal('10.00'), Decimal('2.00'),
             Decimal('12.00'), 0, False) for i in range(n)]


def seed_proof():
    return {'schema': 'bigtop.olist.iceberg-cdc-seed.v1', 'status': 'PASS',
            'table': s.TARGET, 'rows': 512,
            'digest_sha256': 'a' * 64, 'main_snapshot': '10',
            'source_snapshot': '20', 'gold_snapshot': '30'}


def apply_proof():
    return {'schema': 'bigtop.olist.iceberg-cdc-apply.v1', 'status': 'PASS',
            'table': s.TARGET, 'rows': 516, 'live_rows': 512,
            'deleted_rows': 4, 'updates': 8, 'inserts': 4,
            'unique_events': 16, 'raw_events': 48,
            'digest_sha256': 'b' * 64, 'main_snapshot': '11',
            'seed_snapshot': '10', 'source_snapshot': '20',
            'gold_snapshot': '30', 'duplicate_replay_skipped': True,
            'late_events_skipped': True}


class Step63Tests(unittest.TestCase):
    def test_isolated_table(self):
        self.assertEqual(s.TARGET, 'shared.olist63_cdc.fact_change_lab')
        self.assertNotEqual(s.TARGET, s.SOURCE)
        self.assertNotEqual(s.TARGET, s.PROTECTED)

    def test_sample_constant(self):
        self.assertEqual(s.ROWS, 512)
        self.assertEqual(s.UPDATES + s.DELETES + s.INSERTS, 16)

    def test_raw_events_duplicates(self):
        e = s.make_events(baseline())
        self.assertEqual(len(e), 48)
        self.assertEqual(len(s.unique_latest_events(e)), 16)

    def test_latest_events_are_highest_seq(self):
        e = s.unique_latest_events(s.make_events(baseline()))
        self.assertTrue(all(x['event_seq'] == 200 for x in e))

    def test_reject_event_conflict(self):
        e = s.make_events(baseline())
        wrong = {**e[0], 'price': Decimal('999.00')}
        with self.assertRaises(s.Step63Error):
            s.unique_latest_events(e + [wrong])

    def test_reject_illegal_operation(self):
        e = [{**s.make_events(baseline())[0], 'op': 'DROP'}]
        with self.assertRaises(s.Step63Error):
            s.unique_latest_events(e)

    def test_expected_live_and_tombstones(self):
        expected = s.desired_rows(baseline())
        self.assertEqual(len(expected), 516)
        self.assertEqual(sum(not x[6] for x in expected), 512)
        self.assertEqual(sum(bool(x[6]) for x in expected), 4)
        self.assertEqual(sum(x[5] == 200 for x in expected), 16)

    def test_exact_update_and_tombstone(self):
        result = {tuple(x[:2]): x for x in s.desired_rows(baseline())}
        self.assertEqual(result[('olist-order-0000', 1)][2], '11.00')
        self.assertEqual(result[('olist-order-0008', 1)][6], True)

    def test_canonical_digest_order_independent(self):
        rows = baseline()
        self.assertEqual(s.normalized_digest(rows), s.normalized_digest(list(reversed(rows))))

    def test_canonical_digest_duplicate_key_rejected(self):
        rows = baseline()
        with self.assertRaises(s.Step63Error):
            s.normalized_digest(rows + [rows[0]])

    def test_canonical_digest_uses_decimals(self):
        rows = [('x', 1, Decimal('1.0'), Decimal('0.1'), Decimal('1.10'), 0, False)]
        rows2 = [('x', 1, '1.00', '0.10', '1.10', 0, False)]
        self.assertEqual(s.normalized_digest(rows), s.normalized_digest(rows2))

    def test_not_overwrite_proof(self):
        with mock.patch.object(s, 'hdfs_exists', return_value=True), \
             self.assertRaises(s.Step63Error):
            s.write_immutable_proof(s.APPLY_PROOF, {})

    def test_reject_protected_proof_path(self):
        with self.assertRaises(s.Step63Error):
            s.write_immutable_proof(s.PUBLISH59, {})

    def test_proof_validators(self):
        self.assertEqual(s.validate_proof(seed_proof(), 'seed')['rows'], 512)
        self.assertEqual(s.validate_proof(apply_proof(), 'apply')['rows'], 516)

    def test_proof_invalid_count(self):
        p = apply_proof()
        p['deleted_rows'] = 5
        with self.assertRaises(s.Step63Error):
            s.validate_proof(p, 'apply')

    def test_proof_no_publish_safety(self):
        p = apply_proof()
        p['table'] = s.PROTECTED
        with self.assertRaises(s.Step63Error):
            s.validate_proof(p, 'apply')

    def test_trino_proof_pair(self):
        self.assertEqual(t.validate_proofs(seed_proof(), apply_proof()).TARGET, s.TARGET)

    def test_trino_rejects_snapshot_mismatch(self):
        a = apply_proof()
        a['source_snapshot'] = '99'
        with self.assertRaises(ValueError):
            t.validate_proofs(seed_proof(), a)

    def test_trino_cross_engine_full_digest(self):
        actual = s.desired_rows(baseline())
        a = apply_proof()
        a['digest_sha256'] = s.normalized_digest(actual)
        with mock.patch.object(t, 'fetch', return_value={'state': 'ACTIVE', 'nodeVersion': {'version': '484'}}), \
             mock.patch.object(t, 'one_ref', side_effect=['30', '20', '11']), \
             mock.patch.object(t, 'query', return_value=actual):
            t.verify('http://127.0.0.1:8080', seed_proof(), a)

    def test_trino_rejects_wrong_data(self):
        actual = s.desired_rows(baseline())
        a = apply_proof()
        a['digest_sha256'] = hashlib.sha256(b'wrong').hexdigest()
        with mock.patch.object(t, 'fetch', return_value={'state': 'ACTIVE', 'nodeVersion': {'version': '484'}}), \
             mock.patch.object(t, 'one_ref', side_effect=['30', '20', '11']), \
             mock.patch.object(t, 'query', return_value=actual), \
             self.assertRaises(ValueError):
            t.verify('http://127.0.0.1:8080', seed_proof(), a)

    def test_yaml_valid(self):
        for file in [ROOT/'playbooks/63-olist-iceberg-incremental.yml',
                     *(ROOT/'roles/olist_iceberg_incremental63').rglob('*.yml')]:
            with self.subTest(file=file):
                self.assertIsInstance(yaml.safe_load(file.read_text()), (dict, list))

    def test_role_prefix(self):
        obj = yaml.safe_load((ROOT/'roles/olist_iceberg_incremental63/defaults/main.yml').read_text())
        self.assertTrue(all(k.startswith('olist_iceberg_incremental63_') for k in obj))

    def test_safe_plan_default(self):
        data = yaml.safe_load((ROOT/'roles/olist_iceberg_incremental63/defaults/main.yml').read_text())
        self.assertEqual(data['olist_iceberg_incremental63_mode'], 'plan')

    def test_no_global_java_or_warehouse(self):
        code = (ROOT/'roles/olist_iceberg_incremental63/tasks/run.yml').read_text()
        self.assertNotIn('spark.sql.catalog.shared.warehouse', code)
        self.assertNotIn('alternatives', code)
        self.assertNotIn('expire_snapshots', code)

    def test_no_real_source_mutation(self):
        code = SPARK.read_text()
        self.assertNotIn('MERGE INTO {SOURCE}', code)
        self.assertNotIn('MERGE INTO {PROTECTED}', code)
        self.assertIn('MERGE INTO {TARGET}', code)

    def test_no_vendor_or_proofs_bundled(self):
        self.assertFalse((ROOT/'roles/olist_iceberg_incremental63/vendor').exists())
        self.assertFalse((ROOT/'artifacts').exists())


if __name__ == '__main__':
    unittest.main()

"""Offline Step62 tests; do not need Spark, HDFS, Trino or Ansible live cluster."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest import mock
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPARK = ROOT / 'roles/olist_iceberg_maintenance62/files/olist_iceberg_maintenance62.py'
VERIFY = ROOT / 'scripts/62-verify-trino.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

s = load('step62_spark', SPARK)
v = load('step62_verify', VERIFY)


def sample_proof():
    return {'schema': 'bigtop.olist.iceberg-maintenance-lab.v1',
            'status': 'PASS', 'table': s.TABLE, 'rows': 2048,
            'before_files': 8, 'after_files': 1,
            'compacted_snapshot': '100', 'main_snapshot': '100',
            'fault_snapshot': '101', 'protected_gold_snapshot': '999'}


class Step62Tests(unittest.TestCase):
    def test_constants_protect_published_gold(self):
        self.assertEqual(s.PROTECTED, 'shared.olist59_v3.gold_daily_category_sales')
        self.assertEqual(s.TABLE, 'shared.olist62_ops.maintenance_smoke')
        self.assertNotEqual(s.TABLE, s.PROTECTED)

    def test_mutable_target_must_be_exact(self):
        s.require_isolated_table(s.TABLE)
        for target in (s.PROTECTED, 'shared.olist58_v3.gold_daily_category_sales',
                       'shared.olist62_ops.other', 'lab.olist62_ops.maintenance_smoke'):
            with self.assertRaises(s.GuardError):
                s.require_isolated_table(target)

    def test_proof_validated(self):
        self.assertEqual(s.validate_lab_proof(sample_proof())['before_files'], 8)

    def test_no_file_reduction_is_failure(self):
        p = sample_proof()
        p['after_files'] = 8
        with self.assertRaises(s.GuardError):
            s.validate_lab_proof(p)

    def test_fault_cannot_be_current_snapshot(self):
        p = sample_proof()
        p['fault_snapshot'] = p['main_snapshot']
        with self.assertRaises(s.GuardError):
            s.validate_lab_proof(p)

    def test_unknown_source_is_rejected(self):
        p = sample_proof()
        p['table'] = s.PROTECTED
        with self.assertRaises(s.GuardError):
            s.validate_lab_proof(p)

    def test_atomic_proof_refuses_production_path(self):
        with self.assertRaises(s.GuardError):
            s.atomic_proof('/data/olist/published/v3/_APPROVAL.json', {})

    def test_atomic_proof_never_overwrites(self):
        with mock.patch.object(s, 'exists', return_value=True), self.assertRaises(s.GuardError):
            s.atomic_proof(s.LAB_PROOF, {})

    def test_wap_validation_success(self):
        import hashlib
        stage = {'status': 'PASS', 'target_table': s.PROTECTED,
                 'quality_checks_total': 29, 'quality_checks_failed': 0,
                 'checks': [{'status': 'PASS'} for _ in range(29)],
                 'audit_snapshot_id': '999', 'expected_gold_count': 55877}
        raw = json.dumps(stage).encode()
        publish = {'schema': 'bigtop.olist.iceberg-wap-publish.v1',
                   'status': 'PASS', 'target_table': s.PROTECTED,
                   'stage_report_sha256': hashlib.sha256(raw).hexdigest(),
                   'main_snapshot_id': '999', 'published_rows': 55877}
        class DF:
            def count(self): return 55877
        class Spark:
            def table(self, name):
                assert name == s.PROTECTED
                return DF()
        with mock.patch.object(s, 'read_json', side_effect=[(stage, raw),(publish, b'd')]), \
             mock.patch.object(s, 'refs', return_value={'main':'999','audit_v3':'999'}):
            self.assertEqual(s.check_release(Spark())['rows'], 55877)

    def test_wap_rejects_main_drift(self):
        import hashlib
        stage = {'status': 'PASS', 'target_table': s.PROTECTED,
                 'quality_checks_total': 29, 'quality_checks_failed': 0,
                 'checks': [{'status': 'PASS'} for _ in range(29)],
                 'audit_snapshot_id': '999', 'expected_gold_count': 55877}
        raw = json.dumps(stage).encode()
        pub = {'schema': 'bigtop.olist.iceberg-wap-publish.v1',
               'status': 'PASS', 'target_table': s.PROTECTED,
               'stage_report_sha256': hashlib.sha256(raw).hexdigest(),
               'main_snapshot_id':'999','published_rows':55877}
        with mock.patch.object(s, 'read_json', side_effect=[(stage, raw),(pub,b'p')]), \
             mock.patch.object(s,'refs',return_value={'main':'444','audit_v3':'999'}), \
             self.assertRaises(s.GuardError):
            s.check_release(object())

    def test_trino_proof_schema(self):
        self.assertEqual(v.validate_lab(sample_proof())['rows'], 2048)

    def test_trino_rejects_unvalidated_lab(self):
        p=sample_proof();p['status']='FAIL'
        with self.assertRaises(ValueError): v.validate_lab(p)

    def test_trino_row_parity(self):
        p = sample_proof()
        summary = [[2048, 0, 2047, sum(i * 13 % 10000 for i in range(2048))]]
        with mock.patch.object(v,'query',side_effect=[summary,[[1]],[[100]]]):
            v.verify_lab('http://127.0.0.1:8080',p)

    def test_trino_detects_moved_snapshot(self):
        p = sample_proof()
        summary = [[2048, 0, 2047, sum(i * 13 % 10000 for i in range(2048))]]
        with mock.patch.object(v,'query',side_effect=[summary,[[1]],[[99]]]), \
             self.assertRaises(ValueError):
            v.verify_lab('http://127.0.0.1:8080',p)

    def test_ansible_role_variable_prefix(self):
        role='olist_iceberg_maintenance62'
        defaults=yaml.safe_load((ROOT/f'roles/{role}/defaults/main.yml').read_text())
        self.assertTrue(all(k.startswith(role+'_') for k in defaults))

    def test_unsafe_modes_not_default(self):
        d=yaml.safe_load((ROOT/'roles/olist_iceberg_maintenance62/defaults/main.yml').read_text())
        self.assertEqual(d['olist_iceberg_maintenance62_mode'],'plan')
        self.assertFalse(d['olist_iceberg_maintenance62_confirm_expire_sandbox'])

    def test_production_maintenance_never_in_script(self):
        src=SPARK.read_text()
        self.assertNotIn("rewrite_data_files('olist59_v3",src)
        self.assertNotIn("expire_snapshots('olist59_v3",src)
        self.assertNotIn('remove_orphan_files(',src)
        self.assertIn("require_isolated_table(TABLE)",src)

    def test_all_yaml_parses(self):
        files = [ROOT/'playbooks/62-olist-iceberg-maintenance.yml',
                 *list((ROOT/'roles/olist_iceberg_maintenance62').rglob('*.yml'))]
        for file in files:
            with self.subTest(file=file):
                self.assertIsInstance(yaml.safe_load(file.read_text()), (dict, list))

    def test_no_external_vendor_files_bundled(self):
        self.assertFalse((ROOT/'roles/olist_iceberg_maintenance62/vendor').exists())

    def test_proofs_not_bundled(self):
        self.assertFalse((ROOT/'artifacts').exists())

    def test_only_expected_code_and_docs(self):
        self.assertTrue(SPARK.exists())
        self.assertTrue(VERIFY.exists())
        # Existing Step53/56/57 vendor files are allowed in the repository.
        self.assertFalse((ROOT/'roles/olist_iceberg_maintenance62/vendor').exists())


if __name__ == '__main__':
    unittest.main()

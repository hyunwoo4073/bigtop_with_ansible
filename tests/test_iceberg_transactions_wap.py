"""Step54 self-contained, no Spark/Hadoop requirements: python3 -m unittest ..."""
import argparse
import importlib.util
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = ROOT / 'roles/iceberg_transactions_lab/files/iceberg_transactions_wap.py'
SPEC = importlib.util.spec_from_file_location('iceberg_transactions_wap', APP)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def args(**changes):
    p = {'catalog': 'lab', 'mode': 'stage', 'proof_root': MOD.PROOF_ROOT}
    p.update(changes)
    return argparse.Namespace(**p)


class TestGuard(unittest.TestCase):
    def test_stage(self): MOD.guard_args(args())
    def test_publish(self): MOD.guard_args(args(mode='publish'))
    def test_refuse_wrong_catalog(self):
        with self.assertRaises(ValueError): MOD.guard_args(args(catalog='prod'))
    def test_refuse_sql_injection_catalog(self):
        with self.assertRaises(ValueError): MOD.guard_args(args(catalog='lab; DROP TABLE'))
    def test_refuse_wrong_mode(self):
        with self.assertRaises(ValueError): MOD.guard_args(args(mode='reset'))
    def test_refuse_outside_proof(self):
        with self.assertRaises(ValueError): MOD.guard_args(args(proof_root='/data/olist/published/v3'))
    def test_isolated_tables(self):
        self.assertTrue(MOD.MUTATIONS.endswith('_step54'))
        self.assertTrue(MOD.WAP.endswith('_step54'))
        self.assertEqual(MOD.BRANCH, 'audit_step54')


class TestQualityGate(unittest.TestCase):
    def test_clean_candidate(self): MOD.check_quality(MOD.BASELINE, MOD.CANDIDATE)
    def test_reject_missing_candidate(self):
        with self.assertRaises(RuntimeError): MOD.check_quality(MOD.BASELINE, MOD.BASELINE)
    def test_reject_changed_main(self):
        with self.assertRaises(RuntimeError): MOD.check_quality(MOD.CANDIDATE, MOD.CANDIDATE)
    def test_reject_wrong_amount(self):
        with self.assertRaises(RuntimeError): MOD.check_quality(MOD.BASELINE, [(1, 'published', 100), (2, 'candidate', 201)])
    def test_reject_zero_amount(self):
        with self.assertRaises(RuntimeError): MOD.check_quality(MOD.BASELINE, [(1, 'published', 100), (2, 'candidate', 0)])
    def test_reject_duplicate(self):
        with self.assertRaises(RuntimeError): MOD.check_quality(MOD.BASELINE, [(1,'published',100),(1,'candidate',200)])
    def test_rows_equal(self): MOD.assert_rows([1,2], [1,2], 'rows')
    def test_rows_unequal(self):
        with self.assertRaises(RuntimeError): MOD.assert_rows([1], [2], 'rows')


class TestPackaging(unittest.TestCase):
    def test_all_files(self):
        for f in ('playbooks/54-iceberg-transactions-wap.yml',
                  'roles/iceberg_transactions_lab/defaults/main.yml',
                  'roles/iceberg_transactions_lab/tasks/main.yml',
                  'docs/54-iceberg-apply.md',
                  'docs/54-iceberg-architecture.md'):
            self.assertTrue((ROOT / f).is_file(), f)
    def test_explicit_publish(self):
        src = (ROOT/'roles/iceberg_transactions_lab/defaults/main.yml').read_text()
        self.assertIn('iceberg_transactions_lab_mode: deploy_only', src)
        role = (ROOT/'roles/iceberg_transactions_lab/tasks/main.yml').read_text()
        self.assertIn("iceberg_transactions_lab_mode != 'deploy_only'", role)
        self.assertIn('spark-submit', role)
        self.assertIn('--jars', role)
    def test_existing_jar_no_global_spark_modification(self):
        text = (ROOT/'roles/iceberg_transactions_lab/tasks/main.yml').read_text()
        self.assertIn('iceberg-spark-runtime-3.5_2.12-1.10.1.jar', text)
        self.assertNotIn('/usr/lib/spark/jars/', text)
        self.assertNotIn('DROP TABLE', text)
    def test_guard_target_in_app(self):
        code = APP.read_text()
        self.assertIn('guard_args(args)', code)
        self.assertIn('lab', code)
        self.assertNotIn('data/olist/published/v3/_APPROVAL.json', code)
    def test_spark_sql_features(self):
        code = APP.read_text()
        for operation in ('DELETE FROM', 'UPDATE ', 'MERGE INTO', 'ADD COLUMNS',
                          "option('snapshot-id'", 'CREATE BRANCH IF NOT EXISTS',
                          'branch_', 'fast_forward'):
            self.assertIn(operation, code)
    def test_audit_branch_read_before_publish(self):
        code = APP.read_text()
        publish = code[code.index('def publish('):code.index('def write_proof(')]
        self.assertLess(publish.index('check_quality(current, candidate)'), publish.index('fast_forward'))
        self.assertIn("stage_proof.get('audit_snapshot_id')", publish)
        self.assertIn("stage_proof.get('main_snapshot_id')", publish)
        self.assertIn("result = 'ALREADY_PUBLISHED'", publish)
    def test_document_hdfs_readonly_source(self):
        docs=(ROOT/'docs/54-iceberg-apply.md').read_text()
        self.assertIn('Step53', docs)
        self.assertIn('mode=stage', docs)
        self.assertIn('mode=publish', docs)


if __name__ == '__main__':
    unittest.main()

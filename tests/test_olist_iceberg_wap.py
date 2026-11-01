"""No Spark, HDFS, cluster, Java or Trino needed for these unit safety tests."""
import hashlib
import importlib.util
import json
import pathlib
import re
import unittest
from unittest.mock import patch

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = ROOT / 'roles/olist_iceberg_wap/files/olist_iceberg_wap59.py'
VERIFY = ROOT / 'scripts/59-verify-trino.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


app = load('step59_wap_app', APP)
trino = load('step59_trino_verify', VERIFY)


def evidence():
    return {'manifest_sha256': 'a' * 64, 'approval_sha256': 'b' * 64,
            'quality_report_sha256': 'c' * 64, 'step58_proof_sha256': 'd' * 64,
            'expected_gold_count': 55877}


def stage():
    return {'schema': 'bigtop.olist.iceberg-wap-stage.v1', 'status': 'PASS',
            'source_namespace': 'olist58_v3', 'target_table': app.TARGET,
            'branch': app.BRANCH, 'quality_checks_total': 29,
            'quality_checks_failed': 0,
            'checks': [{'id': str(i), 'status': 'PASS'} for i in range(29)],
            'expected_gold_count': 55877,
            'manifest_sha256': 'a' * 64, 'approval_sha256': 'b' * 64,
            'quality_report_sha256': 'c' * 64, 'step58_proof_sha256': 'd' * 64,
            'source_snapshot_ids': {table: str(i + 10) for i, (_, table) in enumerate(app.SOURCE_TABLES)},
            'main_snapshot_before': '10', 'audit_snapshot_id': '111'}


class Step59Tests(unittest.TestCase):
    def test_58_sources_exact_inventory(self):
        self.assertEqual(len(app.SOURCE_TABLES), 7)
        self.assertEqual(len({name for _, name in app.SOURCE_TABLES}), 7)
        self.assertIn(('mart/daily_category_sales', app.GOLD), app.SOURCE_TABLES)

    def test_release_namespace_is_isolated(self):
        self.assertNotEqual(app.SOURCE_NS, app.TARGET_NS)
        self.assertTrue(app.TARGET.startswith('shared.olist59_v3.'))
        self.assertEqual(app.BRANCH_TABLE, app.TARGET + '.branch_audit_v3')

    def test_valid_stage(self):
        app.validate_stage_report(stage(), evidence())
        self.assertEqual(trino.validate_proofs(stage()), 55877)

    def test_failed_stage_blocked(self):
        s = stage()
        s['status'] = 'FAIL'
        with self.assertRaises(app.GateError):
            app.validate_stage_report(s, evidence())

    def test_missing_quality_rule_blocked(self):
        s = stage()
        s['checks'].pop()
        with self.assertRaises(app.GateError):
            app.validate_stage_report(s, evidence())

    def test_changed_snapshot_blocked(self):
        s = stage()
        s['audit_snapshot_id'] = s['main_snapshot_before']
        with self.assertRaises(app.GateError):
            app.validate_stage_report(s, evidence())

    def test_source_snapshot_proof_required(self):
        s = stage()
        s['source_snapshot_ids'].pop(app.GOLD)
        with self.assertRaises(app.GateError):
            app.validate_stage_report(s, evidence())

    def test_wrong_source_hash_blocked(self):
        s = stage()
        s['manifest_sha256'] = '0' * 64
        with self.assertRaises(app.GateError):
            app.validate_stage_report(s, evidence())

    def test_proof_publish_hash_binding(self):
        s = stage()
        raw = json.dumps(s).encode()
        p = {'schema': 'bigtop.olist.iceberg-wap-publish.v1', 'status': 'PASS',
             'target_table': app.TARGET,
             'main_snapshot_id': s['audit_snapshot_id'],
             'stage_report_sha256': hashlib.sha256(raw).hexdigest(),
             'published_rows': 55877}
        trino.validate_proofs(s, p, raw)
        p['stage_report_sha256'] = 'x' * 64
        with self.assertRaises(ValueError):
            trino.validate_proofs(s, p, raw)

    def test_nonpassing_trino_stage_rejected(self):
        s = stage()
        s['checks'][0]['status'] = 'FAIL'
        with self.assertRaises(ValueError):
            trino.validate_proofs(s)

    def test_no_overwrite_in_code(self):
        source = APP.read_text()
        self.assertNotIn('.createOrReplace()', source)
        self.assertNotIn('.mode("overwrite")', source)
        self.assertNotIn('hdfs dfs -rm', source)
        self.assertIn('fast_forward', source)
        self.assertIn('remainder.writeTo(BRANCH_TABLE).append()', source)

    def test_spark_does_not_set_gravitino_wrong_warehouse(self):
        tasks = (ROOT / 'roles/olist_iceberg_wap/tasks/apply.yml').read_text()
        self.assertIn('spark.sql.catalog.shared.type=rest', tasks)
        self.assertIn('spark.sql.catalog.shared.uri=', tasks)
        self.assertNotIn('spark.sql.catalog.shared.warehouse=',
                         '\n'.join(x for x in tasks.splitlines() if not x.lstrip().startswith('#')))

    def test_plan_default_and_explicit_publish(self):
        defaults = yaml.safe_load((ROOT / 'roles/olist_iceberg_wap/defaults/main.yml').read_text())
        self.assertEqual(defaults['olist_iceberg_wap_mode'], 'plan')
        self.assertTrue(all(k.startswith('olist_iceberg_wap_') for k in defaults))
        tasks = (ROOT / 'roles/olist_iceberg_wap/tasks/main.yml').read_text()
        self.assertIn("olist_iceberg_wap_mode in ['stage', 'publish']", tasks)

    def test_hdfs_immutable_proof(self):
        source = APP.read_text()
        self.assertIn('require(not exists(path)', source)
        self.assertIn("hdfs('-mv', staging, path)", source)
        self.assertNotIn("'-put', '-f'", source)

    def test_all_yaml_valid(self):
        for path in (ROOT / 'playbooks/59-olist-iceberg-gold-wap.yml',
                     ROOT / 'roles/olist_iceberg_wap/defaults/main.yml',
                     ROOT / 'roles/olist_iceberg_wap/tasks/main.yml',
                     ROOT / 'roles/olist_iceberg_wap/tasks/apply.yml'):
            self.assertIsNotNone(yaml.safe_load(path.read_text()))

    def test_exact_checks_are_inherited(self):
        code = APP.read_text()
        for name in ('fact_required_columns_not_null', 'fact_amounts_reconciled',
                     'orphan_count_matches_manifest', 'mart_missing_or_changed_rows',
                     'mart_extra_or_changed_rows', 'quarantine_rate_threshold'):
            self.assertIn(name, code)
        self.assertIn('len(legacy.checks) == CHECK_COUNT', code)

    def test_stage_has_no_fast_forward_call(self):
        code = APP.read_text()
        stage_code = code[code.index('def stage('):code.index('def publish(')]
        self.assertNotIn('.system.fast_forward', stage_code)

    def test_stage_does_not_modify_old_58_source(self):
        code = APP.read_text()
        self.assertNotIn('writeTo(SRC_GOLD)', code)
        self.assertNotIn('INSERT INTO shared.olist58_v3', code)

    def test_failed_hdfs_exist_not_masked(self):
        from subprocess import CompletedProcess
        with patch.object(app.subprocess, 'run', return_value=CompletedProcess([], 1, b'', b'Permission denied')):
            with self.assertRaises(app.GateError):
                app.exists('/private')

    def test_hdfs_no_file(self):
        from subprocess import CompletedProcess
        with patch.object(app.subprocess, 'run', return_value=CompletedProcess([], 1, b'', b'No such file')):
            self.assertFalse(app.exists('/not-there'))


if __name__ == '__main__':
    unittest.main()

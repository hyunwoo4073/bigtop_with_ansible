"""Pure standard library tests; no JVM, Hadoop, Trino or PySpark required."""
import hashlib
import importlib.util
import json
import pathlib
import unittest
from unittest.mock import Mock

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = ROOT / 'roles/olist_iceberg_materialization/files/olist_iceberg58.py'
TRINO = ROOT / 'scripts/58-verify-trino.py'


def module_at(name, path):
    import sys
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


etl = module_at('olist_iceberg58', APP)
trino = module_at('step58_verify_trino', TRINO)


def fixture():
    manifest = {'pipeline': 'olist_spark_batch_etl', 'raw_version': 'v1',
                'tables': list(etl.TABLES),
                'metrics': {'etl_version': 'v3', 'output_rows': {t: 42 for t in etl.TABLES}}}
    mbytes = json.dumps(manifest).encode()
    report = {'status': 'PASS', 'warehouse_version': 'v3',
              'etl_manifest_sha256': hashlib.sha256(mbytes).hexdigest(),
              'checks': [{'id': str(i), 'status': 'PASS'} for i in range(29)]}
    rbytes = json.dumps(report).encode()
    approval = {'schema_version': 1, 'kind': 'olist_quality_gated_approval',
                'warehouse_version': 'v3',
                'etl_manifest_sha256': hashlib.sha256(mbytes).hexdigest(),
                'quality_report_sha256': hashlib.sha256(rbytes).hexdigest()}
    return mbytes, json.dumps(approval).encode(), rbytes


class Step58Tests(unittest.TestCase):
    def test_manifest_approved(self):
        ev = etl.validate_provenance(*fixture())
        self.assertEqual(ev.expected_counts['mart/daily_category_sales'], 42)
        self.assertEqual(len(etl.property_dict(ev)), 4)

    def test_wrong_approval_rejected(self):
        m, a, r = fixture()
        approval = json.loads(a)
        approval['etl_manifest_sha256'] = '0' * 64
        with self.assertRaises(etl.MaterializationError):
            etl.validate_provenance(m, json.dumps(approval).encode(), r)

    def test_failed_quality_rejected(self):
        m, a, r = fixture()
        report = json.loads(r)
        report['status'] = 'FAIL'
        with self.assertRaises(etl.MaterializationError):
            etl.validate_provenance(m, a, json.dumps(report).encode())

    def test_quality_report_integrity_rejected(self):
        m, a, r = fixture()
        report = json.loads(r)
        report['checks'].pop()
        with self.assertRaises(etl.MaterializationError):
            etl.validate_provenance(m, a, json.dumps(report).encode())

    def test_invalid_count_bool_rejected(self):
        m, a, r = fixture()
        manifest = json.loads(m)
        manifest['metrics']['output_rows']['curated/orders'] = True
        with self.assertRaises(etl.MaterializationError):
            etl.validate_provenance(json.dumps(manifest).encode(), a, r)

    def test_seven_table_mapping_with_gold_and_quarantine(self):
        self.assertEqual(len(etl.MAPPING), 7)
        self.assertIn(('quarantine/rows', 'quality_quarantine_rows'), etl.MAPPING)
        self.assertIn(('mart/daily_category_sales', 'gold_daily_category_sales'), etl.MAPPING)
        self.assertEqual(etl.catalog_table('gold_daily_category_sales'),
                         'shared.olist58_v3.gold_daily_category_sales')

    def test_target_name_allowlist(self):
        with self.assertRaises(etl.MaterializationError):
            etl.catalog_table('olist56.catalog_smoke')

    def test_parity_rejects_unequal_rows(self):
        a, b = Mock(), Mock()
        a.exceptAll.return_value.limit.return_value.count.return_value = 1
        with self.assertRaises(etl.MaterializationError):
            etl.exact_multiset_parity(a, b, 'a')

    def test_parity_both_directions(self):
        a, b = Mock(), Mock()
        a.exceptAll.return_value.limit.return_value.count.return_value = 0
        b.exceptAll.return_value.limit.return_value.count.return_value = 0
        etl.exact_multiset_parity(a, b, 'a')
        a.exceptAll.assert_called_once_with(b)
        b.exceptAll.assert_called_once_with(a)

    def test_proof_trino_table_count(self):
        proof = etl.expected_proof(etl.validate_provenance(*fixture()))
        self.assertEqual(trino.validate_proof(proof)['gold_daily_category_sales'], 42)

    def test_trino_rejects_unverified_parity(self):
        proof = etl.expected_proof(etl.validate_provenance(*fixture()))
        proof['parity'] = 'sample_and_count'
        with self.assertRaises(ValueError):
            trino.validate_proof(proof)

    def test_all_yaml_parse(self):
        for path in (ROOT / 'playbooks').glob('58-*.yml'):
            yaml.safe_load(path.read_text())
        for path in (ROOT / 'roles/olist_iceberg_materialization').rglob('*.yml'):
            yaml.safe_load(path.read_text())

    def test_new_role_has_role_prefix(self):
        defaults = yaml.safe_load((ROOT / 'roles/olist_iceberg_materialization/defaults/main.yml').read_text())
        self.assertTrue(all(name.startswith('olist_iceberg_materialization_') for name in defaults))

    def test_does_not_invoke_old_olist_roles(self):
        # This test runs inside an existing repository: old roles SHOULD exist.
        # The isolated Step 58 playbook must not call or alter them.
        playbook = (ROOT / 'playbooks/58-olist-iceberg-materialization.yml').read_text()
        self.assertIn('olist_iceberg_materialization', playbook)
        self.assertNotIn('data_pipeline_olist_etl', playbook)
        self.assertNotIn('iceberg_spark_lab', playbook)

    def test_gravitino_warehouse_is_server_side(self):
        apply_text = (ROOT / 'roles/olist_iceberg_materialization/tasks/apply.yml').read_text()
        self.assertIn('spark.sql.catalog.shared.uri=', apply_text)
        self.assertIn('spark.sql.catalog.shared.type=rest', apply_text)
        self.assertNotIn('spark.sql.catalog.shared.warehouse=',
                         '\n'.join(line for line in apply_text.splitlines()
                                   if not line.strip().startswith('#')))
        self.assertNotIn('spark.sql.catalog.shared.prefix=', apply_text)

    def test_apply_opt_in(self):
        tasks = (ROOT / 'roles/olist_iceberg_materialization/tasks/main.yml').read_text()
        self.assertIn('olist_iceberg_materialization_apply | bool', tasks)
        self.assertIn('apply.yml', tasks)

    def test_no_overwrite_or_unsafe_hdfs_delete(self):
        script = APP.read_text()
        self.assertIn('writer.create()', script)
        self.assertNotIn('.createOrReplace()', script)
        self.assertNotIn('.mode("overwrite")', script)
        self.assertNotIn('hdfs dfs -rm', script)


if __name__ == '__main__':
    unittest.main()

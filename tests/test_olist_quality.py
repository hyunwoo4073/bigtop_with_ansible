"""Quality report, manifest and deployment contracts; no Spark installation required."""
from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'roles/data_pipeline_olist_quality/files/olist_quality.py'
spec = importlib.util.spec_from_file_location('olist_quality', SOURCE)
quality = importlib.util.module_from_spec(spec)
spec.loader.exec_module(quality)


def good_manifest():
    return {
        'pipeline': 'olist_spark_batch_etl', 'pipeline_version': '46.1',
        'raw_manifest_sha256': 'a' * 64,
        'tables': list(quality.TABLES),
        'metrics': {
            'source_rows': {name: 10 for name in quality.SOURCE_NAMES},
            'output_rows': {name: 10 for name in quality.TABLES},
            'orphan_order_items': 0,
            'etl_version': 'v1',
        },
    }


class FakeHdfs(quality.Hdfs):
    def __init__(self, existing=()):
        self.paths = set(existing)
        self.ops = []

    def exists(self, path):
        return path in self.paths

    def command(self, *args):
        self.ops.append(args)
        return None


class OlistQualityTests(unittest.TestCase):
    def test_version_valid(self):
        self.assertEqual(quality.check_version('v1'), 'v1')

    def test_version_blocks_paths(self):
        for x in ('../v1', 'v1/x', '', 'a b', '.' * 40):
            with self.subTest(x=x), self.assertRaises(quality.QualityError):
                quality.check_version(x)

    def test_run_id_exact_uuidhex(self):
        self.assertEqual(quality.check_run_id('a' * 32), 'a' * 32)

    def test_run_id_blocks_injection(self):
        for x in ('../../etc', '', 'g' * 32, 'F' * 32, 'a' * 31):
            with self.subTest(x=x), self.assertRaises(quality.QualityError):
                quality.check_run_id(x)

    def test_only_expected_hdfs_roots(self):
        quality.check_roots('/data/olist/warehouse', '/data/olist/quality')
        with self.assertRaises(quality.QualityError):
            quality.check_roots('/data', '/data/olist/quality')

    def test_acceptable_thresholds(self):
        quality.check_thresholds(0.05, 0)
        quality.check_thresholds(1, 42)

    def test_bad_thresholds(self):
        for x, n in ((-0.1, 0), (1.1, 0), (0.1, -1)):
            with self.subTest(x=x, n=n), self.assertRaises(quality.QualityError):
                quality.check_thresholds(x, n)

    def test_manifest_valid(self):
        quality.validate_etl_manifest(good_manifest())

    def test_manifest_rejects_wrong_pipeline(self):
        m = good_manifest()
        m['pipeline'] = 'different'
        with self.assertRaises(quality.QualityError):
            quality.validate_etl_manifest(m)

    def test_manifest_rejects_missing_table(self):
        m = good_manifest()
        m['tables'].pop()
        with self.assertRaises(quality.QualityError):
            quality.validate_etl_manifest(m)

    def test_manifest_rejects_missing_source_metric(self):
        m = good_manifest()
        m['metrics']['source_rows'].pop('products')
        with self.assertRaises(quality.QualityError):
            quality.validate_etl_manifest(m)

    def test_manifest_rejects_missing_output_metric(self):
        m = good_manifest()
        m['metrics']['output_rows'].pop('quarantine/rows')
        with self.assertRaises(quality.QualityError):
            quality.validate_etl_manifest(m)

    def test_manifest_rejects_boolean_count(self):
        m = good_manifest()
        m['metrics']['output_rows']['curated/orders'] = True
        with self.assertRaises(quality.QualityError):
            quality.validate_etl_manifest(m)

    def test_manifest_rejects_invalid_orphan_count(self):
        m = good_manifest()
        m['metrics']['orphan_order_items'] = -1
        with self.assertRaises(quality.QualityError):
            quality.validate_etl_manifest(m)

    def test_rate_zero_denominator(self):
        self.assertEqual(quality.rate(0, 0), 0.0)
        self.assertAlmostEqual(quality.rate(2, 10), 0.2)

    def test_checkbook_pass_and_failure(self):
        book = quality.CheckBook()
        book.equal('rows', 20, 20)
        book.maximum('quarantine', 3, 5)
        self.assertEqual(book.status, 'PASS')
        book.maximum('orphans', 1, 0)
        self.assertEqual(book.failed, 1)
        self.assertEqual(book.status, 'FAIL')

    def test_report_carries_fingerprint_and_counts_no_records(self):
        book = quality.CheckBook()
        book.equal('count', 3, 3)
        report = quality.build_report('a' * 32, 'v1', 1_800_000_000, 'f' * 64,
                                      book, {'fact_rows': 3})
        self.assertEqual(report['status'], 'PASS')
        self.assertEqual(report['summary']['checks_total'], 1)
        self.assertEqual(report['etl_manifest_sha256'], 'f' * 64)
        self.assertNotIn('record_json', json.dumps(report))

    def test_report_error_is_not_pass(self):
        report = quality.build_report('a' * 32, 'v1', 1_800_000_000, 'f' * 64,
                                      quality.CheckBook(), {}, error='SparkError')
        self.assertEqual(report['status'], 'ERROR')

    def test_report_publishes_new_hdfs_run_not_overwrite(self):
        hdfs = FakeHdfs()
        dest = '/data/olist/quality/v1/runs/' + 'a' * 32
        stage = '/data/olist/quality/_staging/' + 'a' * 32
        hdfs.publish(dest, stage, {'status': 'PASS'})
        self.assertEqual(hdfs.ops[-1], ('-mv', stage, dest))
        self.assertFalse(any(c[0] in ('-rm', '-rmr', '-put', '-mv') and
                             '/data/olist/warehouse' in str(c) for c in hdfs.ops))

    def test_report_refuses_existing_run(self):
        dest = '/data/olist/quality/v1/runs/' + 'a' * 32
        hdfs = FakeHdfs([dest])
        with self.assertRaises(quality.QualityError):
            hdfs.publish(dest, '/data/olist/quality/_staging/a', {})

    def test_yarn_cluster_execution_and_result_ingestion(self):
        tasks = (ROOT / 'roles/data_pipeline_olist_quality/tasks/main.yml').read_text()
        self.assertIn('- cluster', tasks)
        self.assertIn('spark.yarn.submit.waitAppCompletion=true', tasks)
        self.assertIn('data_pipeline_olist_quality_report.status == \'PASS\'', tasks)
        self.assertIn('delegate_to:', tasks)
        self.assertNotIn('ansible_connection=local', tasks)

    def test_metrics_have_low_cardinality_series(self):
        template = (ROOT / 'roles/data_pipeline_olist_quality/templates/olist_quality.prom.j2').read_text()
        self.assertIn('olist_quality_run_success', template)
        self.assertIn('olist_quality_last_run_timestamp_seconds', template)
        self.assertNotIn('order_id', template)
        self.assertNotIn('customer_id', template)

    def test_python_source_compiles(self):
        compile(SOURCE.read_text(), str(SOURCE), 'exec')


if __name__ == '__main__':
    unittest.main()

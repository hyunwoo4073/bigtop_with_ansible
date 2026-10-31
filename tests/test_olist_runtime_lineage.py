"""Step 51 pure-Python tests. No Spark, HDFS or OpenMetadata needed."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
CLIENT_FILE = ROOT / 'roles/data_pipeline_olist_runtime_lineage/files/import_runtime_lineage.py'
spec = importlib.util.spec_from_file_location('import_runtime_lineage', CLIENT_FILE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


RAW = 'v1'
WH = 'v2'
HOST = 'master:9000'


def event_and_manifest():
    outputs = sorted(module.OUTPUTS)
    e = {
        'schema': 'bigtop.olist.spark-runtime-lineage.v1',
        'status': 'COMPLETE',
        'pipeline': 'olist_spark_batch_etl',
        'run_id': '3fe9c76b-632d-46b1-b47b-e1dd0a6b1e84',
        'started_at': '2026-10-31T01:02:03+00:00',
        'finished_at': '2026-10-31T01:03:03+00:00',
        'raw_version': RAW,
        'warehouse_version': WH,
        'writes': [{
            'output': name,
            'input_files': ['hdfs://master:9000/data/olist/raw/v1/orders/olist_orders_dataset.csv'],
            'write_path': 'hdfs:///data/olist/warehouse/_staging/v2-012345/' + name,
            'published_path': 'hdfs:///data/olist/warehouse/v2/' + name,
            'output_rows': 4,
        } for name in outputs]
    }
    m = {'pipeline': 'olist_spark_batch_etl', 'raw_version': RAW,
         'metrics': {'output_rows': {name: 4 for name in outputs}}}
    return e, m


class FakeOM:
    def __init__(self):
        self.writes = []
        self.items = {}
        self._graph = {}

    def container(self, fqn):
        obj = self.items.get(fqn)
        if obj is None:
            obj = {'id': fqn, 'fullyQualifiedName': fqn}
            self.items[fqn] = obj
        return obj

    def graph(self, fqn):
        return {'downstreamEdges': self._graph.get(fqn, [])}

    def call(self, method, route, payload):
        if method != 'PUT' or route != 'lineage':
            raise AssertionError('unexpected write')
        self.writes.append(payload)
        frm = payload['edge']['fromEntity']['id']
        to = payload['edge']['toEntity']['id']
        self._graph.setdefault(frm, []).append({'fromEntity': frm, 'toEntity': to})
        return {'ok': True}


class RuntimeLineageTests(unittest.TestCase):
    def test_normalize_hdfs_uri_with_or_without_authority(self):
        p = '/data/olist/raw/v1/orders/olist_orders_dataset.csv'
        self.assertEqual(module.hdfs_uri('hdfs://master:9000' + p, HOST), p)
        self.assertEqual(module.hdfs_uri('hdfs://' + p, HOST), p)

    def test_reject_wrong_namenode(self):
        with self.assertRaises(module.EvidenceError):
            module.hdfs_uri('hdfs://evil:9000/data/olist/raw/v1/x', HOST)

    def test_reject_path_traversal(self):
        with self.assertRaises(module.EvidenceError):
            module.hdfs_uri('hdfs:///data/olist/raw/v1/../unknown', HOST)

    def test_reject_unregistered_source(self):
        path = 'hdfs:///data/olist/raw/v1/reviews/olist_order_reviews_dataset.csv'
        with self.assertRaises(module.EvidenceError):
            module.source_for(path, RAW, HOST)

    def test_valid_complete_evidence(self):
        e, m = event_and_manifest()
        result = module.parse_evidence(e, m, RAW, WH, HOST)
        self.assertEqual(len(result), 7)
        self.assertEqual(result['curated/orders'], {'orders'})

    def test_reject_duplicate_write(self):
        e, m = event_and_manifest()
        e['writes'][-1]['output'] = e['writes'][0]['output']
        with self.assertRaises(module.EvidenceError):
            module.parse_evidence(e, m, RAW, WH, HOST)

    def test_reject_fabricated_published_output(self):
        e, m = event_and_manifest()
        e['writes'][0]['published_path'] = 'hdfs:///data/olist/warehouse/v1/curated/orders'
        with self.assertRaises(module.EvidenceError):
            module.parse_evidence(e, m, RAW, WH, HOST)

    def test_reject_mismatch_warehouse_version(self):
        e, m = event_and_manifest()
        e['warehouse_version'] = 'v1'
        with self.assertRaises(module.EvidenceError):
            module.parse_evidence(e, m, RAW, WH, HOST)

    def test_reject_wrong_row_count(self):
        e, m = event_and_manifest()
        e['writes'][0]['output_rows'] = 100
        with self.assertRaises(module.EvidenceError):
            module.parse_evidence(e, m, RAW, WH, HOST)

    def test_reject_output_missing_count(self):
        e, m = event_and_manifest()
        del m['metrics']['output_rows']['curated/orders']
        with self.assertRaises(module.EvidenceError):
            module.parse_evidence(e, m, RAW, WH, HOST)

    def test_reject_unfinished_execution(self):
        e, m = event_and_manifest()
        e['status'] = 'RUNNING'
        with self.assertRaises(module.EvidenceError):
            module.parse_evidence(e, m, RAW, WH, HOST)

    def test_plan_no_mutations(self):
        e, m = event_and_manifest()
        om = FakeOM()
        outputs = module.parse_evidence(e, m, RAW, WH, HOST)
        n = module.add_run_lineage(om, outputs, 'bigtop_hdfs_olist', RAW, WH, False)
        self.assertEqual(n, 0)
        self.assertEqual(om.writes, [])

    def test_apply_creates_actual_edges_once(self):
        e, m = event_and_manifest()
        om = FakeOM()
        outputs = module.parse_evidence(e, m, RAW, WH, HOST)
        self.assertEqual(module.add_run_lineage(om, outputs, 'bigtop_hdfs_olist', RAW, WH, True), 7)
        self.assertEqual(len(om.writes), 7)
        self.assertEqual(module.add_run_lineage(om, outputs, 'bigtop_hdfs_olist', RAW, WH, True), 0)

    def test_source_empty_not_inferred(self):
        e, m = event_and_manifest()
        e['writes'][0]['input_files'] = []
        outputs = module.parse_evidence(e, m, RAW, WH, HOST)
        self.assertEqual(len(outputs), 7)
        self.assertEqual(module.add_run_lineage(FakeOM(), outputs, 'bigtop_hdfs_olist', RAW, WH, False), 0)

    def test_46_supports_opt_in_without_affecting_immutable_skip(self):
        script = (ROOT / 'roles/data_pipeline_olist_etl/files/olist_etl.py').read_text()
        tasks = (ROOT / 'roles/data_pipeline_olist_etl/tasks/main.yml').read_text()
        self.assertIn('df.inputFiles()', script)
        self.assertIn('_RUNTIME_LINEAGE.json', script)
        self.assertIn('args.verify_existing:', script)
        self.assertIn('data_pipeline_olist_etl_capture_runtime_lineage | default(false)', tasks)
        self.assertIn("--no-runtime-lineage", tasks)


if __name__ == '__main__':
    unittest.main()

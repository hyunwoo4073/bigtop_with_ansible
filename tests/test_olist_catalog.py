"""Offline Step 50 tests: no HDFS, OpenMetadata, token, or Spark required."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'roles/data_pipeline_olist_catalog/files/catalog.py'
spec = importlib.util.spec_from_file_location('olist_catalog', SOURCE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def manifests():
    raw = {'dataset': 'olist', 'schema_version': 1,
           'files': {k: {'filename': v, 'rows': 10, 'bytes': 100,
                         'sha256': 'a' * 64}
                     for k, v in mod.RAW_FILENAMES.items()}}
    wh = {'pipeline': 'olist_spark_batch_etl', 'raw_version': 'v1',
          'tables': list(mod.OUTPUTS),
          'metrics': {'output_rows': {k: 7 for k in mod.OUTPUTS}}}
    return raw, wh


class FakeOM:
    def __init__(self):
        self.s = None
        self.containers = {}
        self.edges = set()
        self.writes = []

    def service(self, name):
        return self.s

    def container(self, fqn):
        return self.containers.get(fqn)

    def downstream(self, fqn):
        src = self.containers[fqn]['id']
        return {'downstreamEdges': [{'fromEntity': a, 'toEntity': b}
                                    for a, b in self.edges if a == src]}

    def call(self, method, path, body=None):
        self.writes.append((method, path))
        if path == 'services/storageServices':
            self.s = {'name': body['name'], 'serviceType': body['serviceType']}
            return self.s
        if path == 'containers':
            fqn = body['service'] + '.' + body['name']
            obj = dict(body, id=f'asset-{len(self.containers)+1}',
                       service={'name': body['service']})
            self.containers[fqn] = obj
            return obj
        if path == 'lineage':
            a, b = body['edge']['fromEntity']['id'], body['edge']['toEntity']['id']
            self.edges.add((a, b))
            return {}
        raise AssertionError((method, path))


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.raw, self.wh = manifests()
        self.metrics = mod.validated_manifests(self.raw, self.wh, 'v1', 'v1')
        self.assets = mod.define_assets('bigtop_hdfs_olist', self.raw, self.metrics,
                                        'v1', 'v1', 'master:9000')

    def test_nine_raw_and_seven_outputs(self):
        self.assertEqual(len(self.assets), 16)
        self.assertEqual(len(mod.LINEAGE), 13)
        self.assertEqual(len(set(tuple(map(tuple, edge)) for edge in mod.LINEAGE)), 13)

    def test_raw_path(self):
        self.assertEqual(self.assets[('raw','orders')]['fullPath'],
                         'hdfs://master:9000/data/olist/raw/v1/orders/olist_orders_dataset.csv')

    def test_mart_is_parquet(self):
        self.assertEqual(self.assets[('mart','daily_category_sales')]['fileFormats'], ['parquet'])

    def test_bad_raw_manifest_rejected(self):
        del self.raw['files']['geolocation']
        with self.assertRaises(mod.CatalogError):
            mod.validated_manifests(self.raw, self.wh, 'v1', 'v1')

    def test_wrong_warehouse_rejected(self):
        self.wh['raw_version'] = 'v9'
        with self.assertRaises(mod.CatalogError):
            mod.validated_manifests(self.raw, self.wh, 'v1', 'v1')

    def test_invalid_count_rejected(self):
        self.wh['metrics']['output_rows']['curated/orders'] = -1
        with self.assertRaises(mod.CatalogError):
            mod.validated_manifests(self.raw, self.wh, 'v1', 'v1')

    def test_plan_creates_nothing(self):
        cli = FakeOM()
        self.assertEqual(mod.run(cli, 'bigtop_hdfs_olist', self.assets, False), 0)
        self.assertFalse(cli.writes)

    def test_apply_then_idempotent(self):
        cli = FakeOM()
        self.assertEqual(mod.run(cli, 'bigtop_hdfs_olist', self.assets, True), 30)
        self.assertEqual(len(cli.containers), 16)
        self.assertEqual(len(cli.edges), 13)
        self.assertEqual(mod.run(cli, 'bigtop_hdfs_olist', self.assets, True), 0)
        self.assertEqual(len(cli.writes), 30)

    def test_conflicting_existing_service(self):
        cli = FakeOM()
        cli.s = {'name': 'bigtop_hdfs_olist', 'serviceType': 'S3'}
        with self.assertRaises(mod.CatalogError):
            mod.run(cli, 'bigtop_hdfs_olist', self.assets, True)
        self.assertEqual(cli.writes, [])

    def test_conflicting_existing_path(self):
        cli = FakeOM()
        cli.s = {'name': 'bigtop_hdfs_olist', 'serviceType': 'CustomStorage'}
        exp = self.assets[('raw','orders')]
        cli.containers['bigtop_hdfs_olist.' + exp['name']] = {
            'id':'bad','name':exp['name'],'service':{'name':'bigtop_hdfs_olist'},
            'fullPath':'hdfs://master:9000/bad',
        }
        with self.assertRaises(mod.CatalogError):
            mod.run(cli, 'bigtop_hdfs_olist', self.assets, True)
        self.assertEqual(cli.writes, [])

    def test_root_only_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp)/'token'
            token.write_text('test-token')
            token.chmod(0o644)
            with self.assertRaises(mod.CatalogError):
                mod.load_private_token(token)
            token.chmod(0o600)
            self.assertEqual(mod.load_private_token(token), 'test-token')

    def test_plan_default_no_write_flag(self):
        yml = (ROOT/'roles/data_pipeline_olist_catalog/defaults/main.yml').read_text()
        self.assertIn('data_pipeline_olist_catalog_apply: false', yml)

    def test_no_destructive_apis(self):
        src = SOURCE.read_text()
        self.assertNotIn("call('DELETE'", src)
        self.assertNotIn("call('PATCH'", src)
        self.assertNotIn("call('PUT', 'containers'", src)

if __name__ == '__main__':
    unittest.main()

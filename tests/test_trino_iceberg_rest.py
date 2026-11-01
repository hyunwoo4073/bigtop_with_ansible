"""Static and behavior unit tests for the Step 57 standalone Trino lab role."""
import importlib.util
import json
import pathlib
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
ROLE = ROOT / 'roles' / 'trino_iceberg_rest'


def load_py(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TrinoStep57Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fetch = load_py(ROOT / 'scripts/57-fetch-trino-assets.py', 'fetch57')
        cls.smoke = load_py(ROOT / 'scripts/57-verify-trino.py', 'smoke57')
        cls.verify = load_py(ROOT / 'scripts/57-verify-artifacts.py', 'verify57')

    def test_trino_digest_is_pinned(self):
        self.assertEqual(self.fetch.TRINO_SHA256, '4b2d05f2e52fcc156b2f137cabc886353e5dde782d12bf71e290ffc13463b81a')
        self.assertEqual(self.verify.TRINO_SHA256, self.fetch.TRINO_SHA256)

    def test_jdk_asset_uses_metadata_sha256(self):
        metadata = [{'version': {'semver': '25.0.2+9'}, 'binary': {'package': {'checksum': 'a' * 64, 'link': 'https://example.org/java.tar.gz'}}}]
        item = self.fetch.jdk_asset(metadata)
        self.assertEqual(item['sha256'], 'a' * 64)
        self.assertEqual(item['version'], '25.0.2+9')

    def test_jdk_asset_rejects_java25_dot0(self):
        with self.assertRaises(RuntimeError):
            self.fetch.jdk_asset([{'version': {'semver': '25.0.0+1'}, 'binary': {'package': {'checksum': 'a' * 64, 'link': 'https://example.org/java.tar.gz'}}}])

    def test_sql_is_read_only_and_qualified(self):
        self.assertEqual(len(self.smoke.QUERIES), 2)
        for _, sql, _ in self.smoke.QUERIES:
            self.assertTrue(sql.startswith('SELECT'))
            self.assertIn('iceberg_rest.olist56.catalog_smoke', sql)
        self.assertEqual(self.smoke.QUERIES[0][2], [[56, 'spark']])

    def test_trino_nexturi_host_is_checked(self):
        import inspect
        self.assertIn('urlsplit(next_url).netloc', inspect.getsource(self.smoke.execute_sql))

    def test_role_var_prefix(self):
        defaults = yaml.safe_load((ROLE / 'defaults/main.yml').read_text())
        self.assertTrue(all(key.startswith('trino_iceberg_rest_') for key in defaults))

    def test_safe_default(self):
        defaults = yaml.safe_load((ROLE / 'defaults/main.yml').read_text())
        self.assertFalse(defaults['trino_iceberg_rest_apply'])
        self.assertFalse(defaults['trino_iceberg_rest_smoke'])

    def test_one_vm_only(self):
        playbook = yaml.safe_load((ROOT / 'playbooks/57-trino-iceberg-rest.yml').read_text())
        self.assertEqual(playbook[0]['hosts'], 'metadata')
        self.assertEqual(playbook[1]['hosts'], 'localhost')

    def test_no_changes_to_spark_java(self):
        all_role_content = '\n'.join(p.read_text() for p in ROLE.rglob('*') if p.is_file())
        for blocked in ('update-alternatives', 'JAVA_HOME=/usr/lib/jvm', 'systemctl restart hadoop', 'spark-submit'):
            self.assertNotIn(blocked, all_role_content)
        self.assertIn('jdk25', all_role_content)

    def test_catalog_has_hdfs(self):
        catalog = (ROLE / 'templates/iceberg_rest.properties.j2').read_text()
        self.assertIn('fs.hadoop.enabled=true', catalog)
        self.assertIn('iceberg.catalog.type=rest', catalog)
        self.assertIn('hive.config.resources=', catalog)
        self.assertNotIn('iceberg.rest-catalog.prefix=', catalog)  # Step 56 uses default static catalog

    def test_trino_is_memory_capped(self):
        systemd = (ROLE / 'templates/bigtop-trino57.service.j2').read_text()
        self.assertIn('MemoryMax={{ trino_iceberg_rest_memory_max }}', systemd)
        jvm = (ROLE / 'templates/jvm.config.j2').read_text()
        self.assertIn('-Xmx{{ trino_iceberg_rest_heap }}', jvm)

    def test_existing_warehouse_not_modified(self):
        all_role_content = '\n'.join(p.read_text() for p in ROLE.rglob('*') if p.is_file())
        self.assertNotIn('hdfs dfs -rm', all_role_content)
        self.assertNotIn('hdfs dfs -chmod', all_role_content)
        self.assertNotIn('fast_forward', all_role_content)

    def test_yaml_all_parse(self):
        for file in [*ROLE.rglob('*.yml'), ROOT / 'playbooks/57-trino-iceberg-rest.yml']:
            with self.subTest(path=file):
                self.assertIsNotNone(yaml.safe_load(file.read_text()))


if __name__ == '__main__':
    unittest.main()

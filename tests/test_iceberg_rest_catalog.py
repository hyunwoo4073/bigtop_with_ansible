"""Static safety/unit tests for Step 56 (no network or VM required)."""
import importlib.util
import hashlib
import json
from unittest import mock
import pathlib
import tempfile
import unittest

try:
    import yaml
except ImportError:
    yaml = None

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_module(name, location):
    spec = importlib.util.spec_from_file_location(name, location)
    obj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj


pg = load_module('ensure_catalog_postgres', ROOT / 'roles/iceberg_rest_catalog/files/ensure_catalog_postgres.py')
verify = load_module('verify_artifacts', ROOT / 'scripts/56-verify-artifacts.py')
fetch = load_module('fetch_rest_assets', ROOT / 'scripts/56-fetch-iceberg-rest-assets.py')


class Step56Tests(unittest.TestCase):
    def test_pg_jdbc_checksum_is_explicitly_pinned(self):
        self.assertEqual(fetch.PG, 'postgresql-42.7.11.jar')
        self.assertEqual(fetch.PG_SHA256,
                         '1981b31d3993c58702783c1cddf10a34e48c1f413d70ff1cb6def0a143484647')

    def test_fetch_does_not_request_missing_maven_sha256_sidecar(self):
        # Network-free simulation: JDK release information is the only API call.
        jdk_asset = [{'binary': {'package': {
            'link': 'https://example.invalid/temurin17.tar.gz',
            'checksum': 'a' * 64,
        }}}]
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.object(fetch, 'HERE', pathlib.Path(d)), \
                mock.patch.object(fetch, 'fetch_file') as download, \
                mock.patch.object(fetch, 'read_url', return_value=json.dumps(jdk_asset).encode()) as read:
            fetch.main()
            self.assertEqual(read.call_count, 1)
            self.assertIn('api.adoptium.net', read.call_args.args[0])
            jdbc_calls = [call for call in download.call_args_list
                          if call.args[1].name == fetch.PG]
            self.assertEqual(len(jdbc_calls), 1)
            self.assertEqual(jdbc_calls[0].args[2], fetch.PG_SHA256)
            self.assertEqual((pathlib.Path(d) / (fetch.PG + '.sha256')).read_text(),
                             f'{fetch.PG_SHA256}  {fetch.PG}\n')

    def test_fetch_reuses_correct_archive_without_network(self):
        payload = b'preserve-already-downloaded-archive'
        digest = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as d:
            target = pathlib.Path(d) / 'present.tar.gz'
            target.write_bytes(payload)
            with mock.patch.object(fetch.urllib.request, 'urlopen',
                                   side_effect=AssertionError('network should not be called')):
                fetch.fetch_file('https://example.invalid/asset', target, digest)
            self.assertEqual(target.read_bytes(), payload)

    def test_fetch_rejects_corrupted_existing_file(self):
        with tempfile.TemporaryDirectory() as d:
            target = pathlib.Path(d) / 'corrupt.jar'
            target.write_bytes(b'invalid')
            with self.assertRaisesRegex(RuntimeError, 'checksum differs'):
                fetch.fetch_file('https://example.invalid/asset', target, 'a' * 64)

    def test_valid_db_identifiers(self):
        self.assertEqual(pg.ident('iceberg_rest56'), 'iceberg_rest56')

    def test_reject_sql_injection_identifiers(self):
        for name in ('postgres;DROP', 'UPPER', 'postgres-db', '', 'a' * 40):
            with self.subTest(name=name), self.assertRaises(ValueError):
                pg.ident(name)

    def test_secret_file_permissions(self):
        with tempfile.TemporaryDirectory() as d:
            f = pathlib.Path(d) / 'secret'
            s = pg.ensure_secret(f)
            self.assertEqual(s, pg.ensure_secret(f))
            self.assertEqual(f.stat().st_mode & 0o777, 0o600)
            f.chmod(0o644)
            with self.assertRaises(RuntimeError):
                pg.ensure_secret(f)

    def test_reject_missing_artifact(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                verify.validate(pathlib.Path(d))

    def test_vendor_files_not_shipped(self):
        # Step 53 and Step 56 intentionally store third-party binaries under
        # vendor/ during local use. Having that directory is not an error.
        # The Ansible role source tree itself must not ship binary artifacts.
        role = ROOT / 'roles/iceberg_rest_catalog'
        binaries = [p for p in role.rglob('*') if p.is_file() and
                    (p.suffix in ('.jar', '.zip', '.tgz', '.gz', '.class') or
                     p.name.endswith('.tar.gz'))]
        self.assertEqual(binaries, [])

    def test_java_is_isolated_in_unit(self):
        unit = (ROOT / 'roles/iceberg_rest_catalog/templates/bigtop-iceberg-rest.service.j2').read_text()
        self.assertIn('Environment=JAVA_HOME=', unit)
        self.assertIn('jdk17', unit)
        self.assertNotIn('update-alternatives', unit)

    def test_pg_password_not_in_version_control(self):
        role = (ROOT / 'roles/iceberg_rest_catalog/tasks/apply.yml').read_text()
        self.assertIn('no_log: true', role)
        self.assertIn('postgres.pass', role)

    def test_separate_warehouse_required(self):
        role = (ROOT / 'roles/iceberg_rest_catalog/tasks/main.yml').read_text()
        self.assertIn('/data/olist/iceberg-rest', role)
        self.assertIn('iceberg_rest_catalog_apply:', (ROOT / 'roles/iceberg_rest_catalog/defaults/main.yml').read_text())

    @unittest.skipIf(yaml is None, 'PyYAML not installed')
    def test_yaml_parses(self):
        for p in list((ROOT / 'playbooks').glob('*.yml')) + list((ROOT / 'roles/iceberg_rest_catalog').rglob('*.yml')):
            with self.subTest(file=p):
                self.assertIsInstance(yaml.safe_load(p.read_text()), (dict, list))

    def test_all_role_var_and_register_names_are_role_prefixed(self):
        """Match ansible-lint var-naming[no-role-prefix] for this role."""
        role = ROOT / 'roles/iceberg_rest_catalog'
        prefix = 'iceberg_rest_catalog_'
        names = []
        for path in (role / 'defaults').glob('*.yml'):
            data = yaml.safe_load(path.read_text()) if yaml else None
            if data:
                names.extend(data.keys())
        for path in (role / 'tasks').glob('*.yml'):
            text = path.read_text()
            names.extend(__import__('re').findall(r'^\s*register: (iceberg_rest_[a-z0-9_]+)\s*$', text, __import__('re').M))
        self.assertTrue(names)
        self.assertEqual([name for name in names if not name.startswith(prefix)], [])

    def test_existing_cli_flags_remain_compatible(self):
        defaults = (ROOT / 'roles/iceberg_rest_catalog/defaults/main.yml').read_text()
        play = (ROOT / 'playbooks/56-iceberg-rest-catalog.yml').read_text()
        for legacy in ('iceberg_rest_apply', 'iceberg_rest_spark_smoke'):
            self.assertIn(legacy + ' | default(false)', defaults)
            self.assertIn(legacy + ' | default(false)', play)

    def test_spark_has_pinned_java11_compatible_jar(self):
        p = (ROOT / 'playbooks/56-iceberg-rest-catalog.yml').read_text()
        self.assertIn('iceberg-spark-runtime-3.5_2.12-1.10.1.jar', p)
        self.assertIn('iceberg_rest_catalog_spark_smoke | default(iceberg_rest_spark_smoke | default(false))', p)

    def test_no_existing_tables_modified(self):
        smoke = (ROOT / 'roles/iceberg_rest_catalog/files/56_spark_rest_smoke.py').read_text()
        self.assertIn('catalog_smoke', smoke)
        self.assertNotIn('DROP TABLE', smoke)
        self.assertNotIn('DELETE FROM', smoke)


if __name__ == '__main__':
    unittest.main()

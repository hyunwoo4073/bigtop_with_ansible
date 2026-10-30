"""Static/runtime safety tests for step 42; does not require PostgreSQL."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from jinja2 import Environment, StrictUndefined
import yaml

ROOT = Path(__file__).resolve().parents[1]
FILES = ROOT / 'roles' / 'governance_postgres_catalog' / 'files'


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class GovernanceCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runner = module('ingestion', FILES / 'run_ingestion.py')
        cls.installer = module('token', FILES / 'install_token.py')
        cls.base = yaml.safe_load((ROOT / 'roles/governance_postgres_catalog/templates/workflow.yaml.j2').read_text())

    def test_metadata_only(self):
        self.assertEqual(self.base['source']['sourceConfig']['config']['type'], 'DatabaseMetadata')
        self.assertFalse(self.base['source']['sourceConfig']['config']['markDeletedTables'])
        self.assertEqual(self.base['sink']['type'], 'metadata-rest')

    def test_postgres_connector_type_matches_openmetadata_2(self):
        # 2.0.6 PostgreSQL connector uses 'Postgres', not 'PostgreSQL'.
        self.assertEqual(self.base['source']['type'], 'postgres')
        self.assertEqual(self.base['source']['serviceConnection']['config']['type'], 'Postgres')

    def test_wrong_connector_type_rejected_before_cli(self):
        bad = yaml.safe_load(yaml.safe_dump(self.base))
        bad['source']['serviceConnection']['config']['type'] = 'PostgreSQL'
        with self.assertRaisesRegex(ValueError, 'expected Postgres'):
            self.runner.build_config(yaml.safe_dump(bad), 'secret_pw', 'abc.def.ghi')

    def test_no_credentials_in_committed_workflow(self):
        cfg = self.base
        self.assertEqual(cfg['source']['serviceConnection']['config']['authType']['password'], '')
        self.assertEqual(cfg['workflowConfig']['openMetadataServerConfig']['securityConfig']['jwtToken'], '')

    def test_credential_injection_does_not_modify_source(self):
        source = yaml.safe_dump(self.base)
        cfg = self.runner.build_config(source, 'secret_pw', 'abc.def.ghi')
        self.assertEqual(cfg['source']['serviceConnection']['config']['authType']['password'], 'secret_pw')
        self.assertEqual(cfg['workflowConfig']['openMetadataServerConfig']['securityConfig']['jwtToken'], 'abc.def.ghi')
        self.assertNotIn('secret_pw', source)
        self.assertFalse(cfg['workflowConfig']['openMetadataServerConfig']['storeServiceConnection'])

    def test_reject_nonmetadata(self):
        bad = dict(self.base)
        bad['source'] = dict(self.base['source'])
        bad['source']['type'] = 'mysql'
        with self.assertRaises(ValueError):
            self.runner.build_config(yaml.safe_dump(bad), 'x', 'a.b.c')

    def test_token_install_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'bot.token'
            self.installer.save_token(path, 'aaaaaa.bbbbbb.' + 'c' * 30)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                self.installer.save_token(path, 'd' * 30 + '.e.f')
            self.assertTrue(path.read_text().startswith('aaaaaa.bbbbbb'))

    def test_token_rotation_explicit_only(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'bot.token'
            self.installer.save_token(path, 'aaaaaa.bbbbbb.' + 'c' * 30)
            self.installer.save_token(path, 'bbbbbb.cccccc.' + 'd' * 30, replace=True)
            self.assertTrue(path.read_text().startswith('bbbbbb.cccccc'))

    def test_secure_read_permissions(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'token'
            path.write_text('x')
            path.chmod(0o644)
            with self.assertRaises(PermissionError):
                self.runner.read_secret(path)
            path.chmod(0o600)
            self.assertEqual(self.runner.read_secret(path), 'x')

    def test_template_renders_as_valid_yaml(self):
        template = (ROOT / 'roles/governance_postgres_catalog/templates/workflow.yaml.j2').read_text()
        cfg = yaml.safe_load(Environment(undefined=StrictUndefined).from_string(template).render(
            governance_postgres_catalog_service='bigtop_worker3_postgresql',
            governance_postgres_catalog_reader='bigtop_catalog_reader',
            governance_postgres_catalog_db_host='127.0.0.1',
            governance_postgres_catalog_db_port=5432,
            governance_postgres_catalog_database='bigtop_console',
            governance_postgres_catalog_schema='public',
            governance_postgres_catalog_openmetadata_url='http://192.158.2.104:8585/api'))
        self.assertEqual(cfg['source']['serviceConnection']['config']['type'], 'Postgres')
        self.assertEqual(cfg['source']['serviceConnection']['config']['hostPort'], '127.0.0.1:5432')
        self.assertEqual(cfg['source']['sourceConfig']['config']['schemaFilterPattern']['includes'], ['^public$'])

    def test_runtime_yaml_is_private_and_deleted(self):
        with tempfile.TemporaryDirectory() as d:
            base_path = Path(d) / 'base.yaml'
            base_path.write_text(yaml.safe_dump(self.base))
            password_path = Path(d) / 'reader.password'
            password_path.write_text('dbpass')
            password_path.chmod(0o600)
            token_path = Path(d) / 'bot.token'
            token_path.write_text('aaa.bbb.ccc')
            token_path.chmod(0o600)
            real_temp_dir = tempfile.TemporaryDirectory
            paths = []
            def temp_dir(*args, **kwargs):
                kwargs['dir'] = d
                return real_temp_dir(*args, **kwargs)
            def mocked_cli(args, **kwargs):
                generated = Path(args[-1])
                self.assertEqual(generated.stat().st_mode & 0o777, 0o600)
                data = yaml.safe_load(generated.read_text())
                self.assertEqual(data['source']['serviceConnection']['config']['type'], 'Postgres')
                self.assertEqual(data['source']['serviceConnection']['config']['authType']['password'], 'dbpass')
                self.assertEqual(data['workflowConfig']['openMetadataServerConfig']['securityConfig']['jwtToken'], 'aaa.bbb.ccc')
                paths.append(generated)
                return type('Result', (), {'returncode': 0})()
            with patch.object(self.runner.os, 'geteuid', return_value=0), \
                 patch.object(self.runner.tempfile, 'TemporaryDirectory', side_effect=temp_dir), \
                 patch.object(self.runner.subprocess, 'run', side_effect=mocked_cli):
                self.assertEqual(self.runner.run(base_path, token_path, password_path, '/fake/metadata'), 0)
            self.assertFalse(paths[0].exists())

    def test_postgres_tasks_explicit_become(self):
        tasks = yaml.safe_load((ROOT / 'roles/governance_postgres_catalog/tasks/main.yml').read_text())
        privileged = [task for task in tasks if task.get('become_user') == 'postgres']
        self.assertEqual(len(privileged), 6)
        for task in privileged:
            self.assertIs(task.get('become'), True, task['name'])

    def test_privilege_tasks_use_required_login_db(self):
        """community.postgresql >= 5 requires login_db, not removed database alias."""
        tasks = yaml.safe_load((ROOT / 'roles/governance_postgres_catalog/tasks/main.yml').read_text())
        grants = [t for t in tasks if 'community.postgresql.postgresql_privs' in t]
        self.assertEqual(len(grants), 4)
        for task in grants:
            args = task['community.postgresql.postgresql_privs']
            self.assertEqual(args.get('login_db'), '{{ governance_postgres_catalog_database }}', task['name'])
            self.assertNotIn('database', args, task['name'])
            self.assertTrue(task.get('become'), task['name'])
            self.assertEqual(task.get('become_user'), 'postgres', task['name'])

    def test_role_names(self):
        task = (ROOT / 'roles/governance_postgres_catalog/tasks/main.yml').read_text()
        self.assertIn('community.postgresql.postgresql_user:', task)
        self.assertNotIn('ansible_connection=local', task)


if __name__ == '__main__':
    unittest.main()

import importlib.util
from pathlib import Path
import unittest

BASE = Path(__file__).resolve().parents[1]


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, str(path))
    obj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj


live = module(BASE / 'roles/olist_metadata76_es/files/step76_es_remote.py', 'step76_es_remote')
source = module(BASE / 'scripts/step76_d4_patch_template.py', 'step76_d4_patch_template')

COMPOSE = '''name: bigtop-openmetadata
services:
  mysql:
    image: mysql:8.0

  elasticsearch:
    image: elasticsearch:9.3.0
    environment:
      discovery.type: single-node
      ES_JAVA_OPTS: '-Xms768m -Xmx768m'
    volumes:
      - es-data:/usr/share/elasticsearch/data
    networks: [governance_net]

  execute-migrate-all:
    image: metadata:2.0.6
'''


class Step76Tests(unittest.TestCase):
    def test_compose_patch_adds_repo_only_to_es(self):
        new = live.patch_compose(COMPOSE)
        self.assertIn('path.repo: /usr/share/elasticsearch/snapshots', new)
        self.assertIn('/var/backups/bigtop-step76-es:/usr/share/elasticsearch/snapshots', new)
        self.assertEqual(new.count('path.repo:'), 1)
        self.assertIn('  mysql:\n    image: mysql:8.0', new)

    def test_compose_patch_idempotent(self):
        result = live.patch_compose(COMPOSE)
        self.assertEqual(live.patch_compose(result), result)

    def test_template_patch_idempotent(self):
        result = source.patch(COMPOSE)
        self.assertEqual(source.patch(result), result)

    def test_live_and_template_patch_equal(self):
        self.assertEqual(live.patch_compose(COMPOSE), source.patch(COMPOSE))

    def test_reject_existing_custom_repo(self):
        modified = COMPOSE.replace('      ES_JAVA_OPTS:', '      path.repo: /other/path\n      ES_JAVA_OPTS:')
        with self.assertRaises(live.StepError):
            live.patch_compose(modified)

    def test_reject_unrecognized_service_block(self):
        with self.assertRaises(live.StepError):
            live.patch_compose(COMPOSE.replace('      - es-data:/usr/share/elasticsearch/data','      - es-data:/other'))

    def test_snapshot_identifier(self):
        self.assertEqual(live.snapshot_id('20261115T082737Z-dc572b3b'),
                         'step76_20261115t082737z_dc572b3b')

    def test_reject_bad_backup_ids(self):
        for value in ('../../secrets', '', '20261115', '20261115T082737Z-!'):
            with self.assertRaises(live.StepError):
                live.snapshot_id(value)

    def test_fixed_host_paths(self):
        self.assertEqual(str(live.HOST_REPO), '/var/backups/bigtop-step76-es')
        self.assertEqual(live.PROJECT, 'bigtop-openmetadata')

    def test_safe_rename_prefix_and_source_exclusion(self):
        import inspect
        text = inspect.getsource(live.restore_sample)
        self.assertIn('step76_verify_', text)
        self.assertIn("'include_global_state':False", text)
        self.assertIn("'include_aliases':False", text)
        self.assertIn("api('DELETE','/' + dest", text)

if __name__ == '__main__':
    unittest.main()

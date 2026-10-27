"""Service UI shortcut API and source integration smoke tests (no cluster needed)."""
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from shutil import copy2

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / 'web_console_ui_shortcuts_feature' / 'assets'
PATCHER = ROOT / 'web_console_ui_shortcuts_feature' / 'patch_sources.py'


class FakeApp:
    def get(self, _url):
        return lambda func: func


class LinkAPITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'service-ui-links.json'
        block = (ASSETS / 'service_ui_links_api.py.txt').read_text()
        self.env = {'app': FakeApp(), 'json': json, 'Path': lambda _x: self.path}
        exec(compile(block, 'service_ui_links_api.py', 'exec'), self.env)
        self.api = self.env['api_service_ui_links']

    def save(self, links):
        self.path.write_text(json.dumps({'links': links}))

    def test_missing_config_returns_unavailable_not_server_error(self):
        data = self.api()
        self.assertFalse(data['ok'])
        self.assertEqual(data['links'], [])

    def test_bad_json_is_handled(self):
        self.path.write_text('{ broken')
        self.assertFalse(self.api()['ok'])

    def test_normal_and_duplicate_links(self):
        self.save([
            {'title': 'NN', 'group': 'HDFS', 'host': 'master',
             'service': 'hadoop-hdfs-namenode', 'url': 'http://master:9870'},
            {'title': 'NN duplicate', 'group': 'HDFS', 'host': 'master',
             'service': 'hadoop-hdfs-namenode', 'url': 'http://master:9999'},
            {'title': 'NM', 'group': 'YARN', 'host': 'worker1',
             'service': 'hadoop-yarn-nodemanager', 'url': 'https://worker1:8042'},
        ])
        data = self.api()
        self.assertTrue(data['ok'])
        self.assertTrue(data['read_only'])
        self.assertEqual(len(data['links']), 2)
        self.assertEqual(data['links'][0]['url'], 'http://master:9870')

    def test_unsafe_or_malformed_urls_are_rejected(self):
        bad = ['javascript:alert(1)', 'file:///etc/passwd',
               'http://user:pass@master:9090', 'http://master:BAD',
               'http://master:9090/a b', 'http://']
        self.save([
            {'title': 'bad', 'group': 'Ops', 'host': f'bad{i}',
             'service': 'prometheus', 'url': url}
            for i, url in enumerate(bad)
        ])
        self.assertEqual(self.api()['links'], [])

    def test_missing_keys_and_wrong_payload_type(self):
        self.save([None, 13, {'title': 'only title'}])
        self.assertEqual(self.api()['links'], [])
        self.path.write_text(json.dumps({'links': 'not a list'}))
        self.assertEqual(self.api()['links'], [])

    def test_ansible_default_service_set_has_correct_addresses(self):
        import yaml
        values = yaml.safe_load((ROOT / 'roles/web_console_service_ui_links/defaults/main.yml').read_text())
        links = values['web_console_service_ui_links_items']
        self.assertEqual(len(links), 6)
        self.assertEqual(len({(l['host'], l['service']) for l in links}), len(links))
        actual = {(l['host'], l['service']): l['url'] for l in links}
        self.assertEqual(actual, {
            ('master', 'hadoop-hdfs-namenode'): 'http://master:9870',
            ('master', 'hadoop-yarn-resourcemanager'): 'http://master:18088',
            ('master', 'spark-history-server'): 'http://master:18080',
            ('worker3', 'prometheus'): 'http://worker3:9090',
            ('worker3', 'grafana-server'): 'http://worker3:23000',
            ('worker3', 'prometheus-alertmanager'): 'http://worker3:19093',
        })
        self.assertFalse(any(link['service'] in ('hadoop-hdfs-datanode',
                                                'hadoop-yarn-nodemanager') for link in links))

    def test_readme_sync_rewrites_only_managed_section_idempotently(self):
        script = ROOT / 'web_console_ui_shortcuts_feature' / 'sync_readme.py'
        with tempfile.TemporaryDirectory() as d:
            readme = Path(d) / 'README.md'
            readme.write_text('PREFIX\n<!-- BEGIN BIGTOP SERVICE UI SHORTCUTS 39 -->\nOLD\n'
                              '<!-- END BIGTOP SERVICE UI SHORTCUTS 39 -->\nSUFFIX\n')
            cmd = ['python3', str(script), str(readme)]
            self.assertEqual(subprocess.run(cmd, capture_output=True).returncode, 0)
            once = readme.read_bytes()
            self.assertIn(b'YARN `master:18088`', once)
            self.assertIn(b'PREFIX', once)
            self.assertIn(b'SUFFIX', once)
            self.assertNotIn(b'OLD', once)
            self.assertEqual(subprocess.run(cmd, capture_output=True).returncode, 0)
            self.assertEqual(readme.read_bytes(), once)

    def test_source_integrator_is_repeatable(self):
        # Reconstructed frontend/backend sources are optional fixture files.
        fixture = Path(__file__).parent / 'fixtures' / 'web_console_snapshot'
        if not fixture.exists():
            self.skipTest('Source snapshot fixture not provided on target VM')
        with tempfile.TemporaryDirectory() as d:
            backend = Path(d) / 'app.py.j2'
            frontend = Path(d) / 'main.jsx'
            copy2(fixture / 'app.py.j2', backend)
            copy2(fixture / 'main.jsx', frontend)
            cmd = ['python3', str(PATCHER), str(backend), str(frontend),
                   str(ASSETS / 'service_ui_links_api.py.txt')]
            self.assertEqual(subprocess.run(cmd, capture_output=True).returncode, 0)
            before = (backend.read_bytes(), frontend.read_bytes())
            self.assertEqual(subprocess.run(cmd, capture_output=True).returncode, 0)
            self.assertEqual(before, (backend.read_bytes(), frontend.read_bytes()))


if __name__ == '__main__':
    unittest.main()

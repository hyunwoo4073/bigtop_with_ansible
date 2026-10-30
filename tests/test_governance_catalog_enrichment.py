"""Contract and regression tests: no live OpenMetadata or PostgreSQL changes."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'roles/governance_catalog_enrichment/files/enrich_catalog.py'
POLICY = ROOT / 'roles/governance_catalog_enrichment/files/governance_policy.yaml'
spec = importlib.util.spec_from_file_location('enrich_catalog', SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class FakeClient:
    def __init__(self):
        self.calls = []
        self.entities = {}
        self.entities[('tables', 'bigtop_worker3_postgresql.bigtop_console.public.platform_hosts')] = {
            'id': 'uuid-host', 'fullyQualifiedName': 'bigtop_worker3_postgresql.bigtop_console.public.platform_hosts',
            'description': '', 'tags': [], 'owners': [],
            'columns': [{'name': 'host_name'}, {'name': 'inventory_address'}]
        }
        self.entities[('tables', 'bigtop_worker3_postgresql.bigtop_console.public.platform_service_instances')] = {
            'id': 'uuid-service', 'fullyQualifiedName': 'bigtop_worker3_postgresql.bigtop_console.public.platform_service_instances',
            'description': '', 'tags': [], 'owners': [],
            'columns': [{'name': 'service_name'}, {'name': 'component_name'}]
        }
    def named(self, endpoint, name, fields=None):
        self.calls.append(('GET', endpoint, name))
        return self.entities.get((endpoint, name))
    def request(self, method, endpoint, body):
        self.calls.append((method, endpoint, body))
        if method == 'POST':
            name = body['name']
            if endpoint == 'tags':
                name = body['classification'] + '.' + name
            if endpoint == 'glossaryTerms':
                name = body['glossary'] + '.' + name
            self.entities[(endpoint, name)] = {'id': 'new-' + name, 'name': name}
        if method == 'PATCH':
            asset = next(v for (kind, key), v in self.entities.items() if kind == 'tables' and v['id'] == endpoint.split('/')[-1])
            for op in body:
                path = op['path']
                if path == '/description':
                    asset['description'] = op['value']
                elif path == '/tags':
                    asset['tags'] = op['value']
                elif path == '/tags/-':
                    asset['tags'].append(op['value'])
                elif path.startswith('/columns/'):
                    _, _, i, field = path.split('/')
                    asset['columns'][int(i)][field] = op['value']
                elif path == '/owners':
                    asset['owners'] = op['value']
        return {}


class GovernanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = mod.validate(yaml.safe_load(POLICY.read_text()))

    def test_policy_targets_only_ingested_schema(self):
        self.assertEqual(len(self.policy['tables']), 2)
        self.assertTrue(all(t['fqn'].startswith('bigtop_worker3_postgresql.bigtop_console.public.') for t in self.policy['tables']))

    def test_default_has_no_guessed_owner(self):
        self.assertTrue(all('owner' not in t for t in self.policy['tables']))

    def test_only_governance_metadata_no_db_connection(self):
        source = SCRIPT.read_text()
        self.assertNotIn('psycopg', source)
        self.assertNotIn('DELETE', source)
        self.assertNotIn('docker', source)

    def test_plan_never_writes(self):
        api = FakeClient()
        mod.run(api, self.policy, False)
        self.assertTrue(all(c[0] == 'GET' for c in api.calls))

    def test_apply_and_repeat_is_idempotent(self):
        api = FakeClient()
        mod.run(api, self.policy, True)
        self.assertEqual(len([c for c in api.calls if c[0] == 'PATCH']), 2)
        api.calls.clear()
        mod.run(api, self.policy, True)
        self.assertFalse(any(c[0] in ('POST', 'PATCH') for c in api.calls))

    def test_preflight_failure_does_not_mutate(self):
        api = FakeClient()
        api.entities.pop(('tables', self.policy['tables'][1]['fqn']))
        with self.assertRaises(mod.CatalogError):
            mod.run(api, self.policy, True)
        self.assertTrue(all(c[0] == 'GET' for c in api.calls))

    def test_preserve_user_description_and_tags(self):
        asset = FakeClient().entities[('tables', self.policy['tables'][0]['fqn'])]
        asset['description'] = 'Manual human description'
        asset['tags'] = [{'tagFQN': 'Other.Custom', 'state': 'Confirmed'}]
        asset['columns'][0]['description'] = 'Human column description'
        asset['owners'] = [{'id': 'human', 'type': 'team'}]
        ops = mod.annotation_patch(self.policy['tables'][0], asset, {'id':'new','type':'team'}, 'BigtopPlatform')
        self.assertNotIn('/description', [x['path'] for x in ops])
        self.assertNotIn('/columns/0/description', [x['path'] for x in ops])
        self.assertNotIn('/owners', [x['path'] for x in ops])
        self.assertFalse(any(x['path']=='/tags' for x in ops))

    def test_fail_if_column_missing(self):
        asset = {'columns': [], 'tags': [], 'owners': []}
        with self.assertRaises(mod.CatalogError):
            mod.annotation_patch(self.policy['tables'][0], asset)

    def test_owner_must_exist(self):
        api = FakeClient()
        spec2 = yaml.safe_load(POLICY.read_text())
        spec2['tables'][0]['owner'] = {'type':'team','name':'MissingTeam'}
        with self.assertRaises(mod.CatalogError):
            mod.run(api, spec2, True)
        self.assertTrue(all(c[0] == 'GET' for c in api.calls))

    def test_reject_unconfigured_tag(self):
        pol = yaml.safe_load(POLICY.read_text())
        pol['tables'][0]['tags'] = ['PII.Sensitive']
        with self.assertRaises(mod.CatalogError):
            mod.validate(pol)

    def test_protect_secret_mode(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / 'token'
            f.write_text('aaa.bbb.ccc')
            f.chmod(0o644)
            with self.assertRaises(mod.CatalogError):
                mod.credential(f)
            f.chmod(0o600)
            self.assertEqual(mod.credential(f), 'aaa.bbb.ccc')

    def test_playbook_scope_worker3_and_no_timer(self):
        p = yaml.safe_load((ROOT / 'playbooks/43-governance-catalog-enrichment.yml').read_text())
        self.assertEqual(p[0]['hosts'], 'ops')
        self.assertIn('governance_catalog_enrichment', str(p))
        self.assertNotIn('timer', str(p).lower())


if __name__ == '__main__':
    unittest.main()

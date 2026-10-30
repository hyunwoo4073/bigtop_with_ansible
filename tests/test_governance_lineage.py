"""Step 44 tests are offline; no PostgreSQL or OpenMetadata is mutated."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
FILES = ROOT / 'roles' / 'governance_lineage' / 'files'
modspec = importlib.util.spec_from_file_location('manage_lineage', FILES / 'manage_lineage.py')
app = importlib.util.module_from_spec(modspec)
modspec.loader.exec_module(app)

SOURCE_HOSTS = 'bigtop_worker3_postgresql.bigtop_console.public.platform_hosts'
SOURCE_SERVICES = 'bigtop_worker3_postgresql.bigtop_console.public.platform_service_instances'
TARGET = 'bigtop_worker3_postgresql.bigtop_console.public.v_bigtop_service_inventory_lineage'


class FakeAPI:
    def __init__(self, existing=None, missing=None):
        self.existing = set(existing or [])
        self.missing = set(missing or [])
        self.calls = []
        self.assets = {}
        for name, idx, kind, cols in [
            (SOURCE_HOSTS, 'id-hosts', 'Regular', ['cluster_slug', 'host_name', 'inventory_address']),
            (SOURCE_SERVICES, 'id-services', 'Regular', ['cluster_slug', 'host_name', 'service_name',
                                                       'component_name', 'group_name']),
            (TARGET, 'id-view', 'View', ['cluster_slug', 'host_name', 'inventory_address',
                                         'service_name', 'component_name', 'group_name']),
        ]:
            self.assets[name] = {'id': idx, 'fullyQualifiedName': name, 'tableType': kind,
                                 'columns': [{'name': c} for c in cols]}

    def table(self, fqn):
        self.calls.append(('GET_TABLE', fqn))
        return self.assets.get(fqn) if fqn not in self.missing else None

    def downstream(self, fqn):
        self.calls.append(('GET_GRAPH', fqn))
        src_id = self.assets[fqn]['id']
        return {'downstreamEdges': ([{'fromEntity': src_id, 'toEntity': 'id-view'}]
                                    if fqn in self.existing else [])}

    def add_edge(self, body):
        self.calls.append(('PUT', body))
        source_id = body['edge']['fromEntity']['id']
        for fqn, asset in self.assets.items():
            if asset['id'] == source_id:
                self.existing.add(fqn)
                return {'status': 'ok'}
        raise AssertionError('Unknown source ID')


class LineageTests(unittest.TestCase):
    def setUp(self):
        self.policy = app.read_policy(FILES / 'lineage_policy.yaml')

    def test_exact_real_derived_tables(self):
        self.assertEqual({s['fqn'] for s in self.policy['sources']},
                         {SOURCE_HOSTS, SOURCE_SERVICES})
        self.assertEqual(self.policy['target'], TARGET)

    def test_plan_does_not_write(self):
        api = FakeAPI()
        self.assertEqual(app.run(api, self.policy, apply=False), 2)
        self.assertFalse(any(c[0] == 'PUT' for c in api.calls))

    def test_apply_and_repeat_is_idempotent(self):
        api = FakeAPI()
        self.assertEqual(app.run(api, self.policy, apply=True), 2)
        self.assertEqual(app.run(api, self.policy, apply=True), 0)
        self.assertEqual(len([c for c in api.calls if c[0] == 'PUT']), 2)

    def test_preserve_auto_detected_lineage(self):
        api = FakeAPI(existing={SOURCE_HOSTS, SOURCE_SERVICES})
        self.assertEqual(app.run(api, self.policy, apply=True), 0)
        self.assertFalse(any(c[0] == 'PUT' for c in api.calls))

    def test_partial_existing_only_adds_missing(self):
        api = FakeAPI(existing={SOURCE_HOSTS})
        self.assertEqual(app.run(api, self.policy, apply=True), 1)
        self.assertEqual(len([c for c in api.calls if c[0] == 'PUT']), 1)

    def test_missing_table_blocks_all_writes(self):
        api = FakeAPI(missing={TARGET})
        with self.assertRaises(app.LineageError):
            app.run(api, self.policy, apply=True)
        self.assertFalse(any(c[0] == 'PUT' for c in api.calls))

    def test_missing_column_blocks_all_writes(self):
        api = FakeAPI()
        api.assets[TARGET]['columns'].pop()
        with self.assertRaises(app.LineageError):
            app.run(api, self.policy, apply=True)
        self.assertFalse(any(c[0] == 'PUT' for c in api.calls))

    def test_column_lineage_uses_actual_sources_and_dest(self):
        refs = FakeAPI().assets
        edge = app.make_edge(self.policy, self.policy['sources'][0], refs)
        details = edge['edge']['lineageDetails']
        self.assertEqual(details['source'], 'Manual')
        self.assertEqual(details['columnsLineage'][0], {
            'fromColumns': [SOURCE_HOSTS + '.inventory_address'],
            'toColumn': TARGET + '.inventory_address',
        })
        self.assertEqual(edge['edge']['toEntity']['id'], 'id-view')

    def test_reject_unrelated_tables_and_bad_fqn(self):
        bad = dict(self.policy)
        bad['sources'] = [{'fqn': SOURCE_HOSTS, 'columns': {'inventory_address': 'inventory_address'}},
                          {'fqn': SOURCE_HOSTS, 'columns': {'inventory_address': 'inventory_address'}}]
        with self.assertRaises(app.LineageError):
            app.read_policy_from_dict(bad) if hasattr(app, 'read_policy_from_dict') else self._validate(bad)

    def _validate(self, spec):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            p = Path(root) / 'policy.yaml'
            p.write_text(yaml.safe_dump(spec), encoding='utf8')
            app.read_policy(p)

    def test_view_is_sql_only_no_base_table_writes(self):
        sql = (FILES / 'create_view.sql').read_text()
        self.assertIn('CREATE VIEW public.v_bigtop_service_inventory_lineage', sql)
        self.assertIn('JOIN public.platform_hosts', sql)
        self.assertIn('FROM public.platform_service_instances', sql)
        self.assertNotIn('DROP ', sql.upper().replace('-- MANAGED BY ANSIBLE STEP 44. NO DROP AND NO BASE-TABLE WRITES.', ''))
        self.assertNotIn('DELETE FROM', sql.upper())
        self.assertIn('bigtop-governance-lineage:step44:v1', sql)

    def test_playbook_does_not_target_existing_hadoop_group(self):
        play = yaml.safe_load((ROOT / 'playbooks/44-governance-lineage.yml').read_text())
        self.assertEqual(play[0]['hosts'], 'ops')
        self.assertEqual(play[0]['roles'][0]['role'], 'governance_lineage')

    def test_explicit_become_for_postgres_tasks(self):
        tasks = yaml.safe_load((ROOT / 'roles/governance_lineage/tasks/main.yml').read_text())
        for task in tasks:
            if task.get('become_user') == 'postgres':
                self.assertIs(task.get('become'), True, task['name'])

    def test_secret_not_in_policy_or_service_file(self):
        data = (FILES / 'lineage_policy.yaml').read_text()
        self.assertNotIn('jwtToken:', data)
        self.assertNotIn('password:', data)
        service = (ROOT / 'roles/governance_lineage/templates/lineage.service.j2').read_text()
        self.assertIn('--token-file', service)
        self.assertNotIn('jwtToken', service)


if __name__ == '__main__':
    unittest.main()

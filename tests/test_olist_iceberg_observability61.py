"""Local Step61 unit tests; no Hadoop, Grafana or OpenMetadata needed."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/61-iceberg-observability.py'
spec = importlib.util.spec_from_file_location('step61', SCRIPT)
step = importlib.util.module_from_spec(spec)
spec.loader.exec_module(step)


def write_artifacts(root):
    artifacts = root / 'artifacts'
    artifacts.mkdir()
    counts = {t: 10 for _, _, t in step.MAPPING}
    counts[step.GOLD] = 55877
    stage = {
        'schema': 'bigtop.olist.iceberg-wap-stage.v1',
        'status': 'PASS', 'target_table': step.EXPECTED_TARGET,
        'source_namespace': 'olist58_v3', 'branch': 'audit_v3',
        'quality_checks_total': 29, 'quality_checks_failed': 0,
        'checks': [{'id': f'dq{i}', 'status': 'PASS'} for i in range(29)],
        'source_snapshot_ids': {t: str(i + 100) for i, (_, _, t) in enumerate(step.MAPPING)},
        'expected_gold_count': 55877, 'candidate_gold_count': 55877,
        'main_snapshot_before': '400', 'audit_snapshot_id': '500',
    }
    raw = json.dumps(stage, sort_keys=True).encode()
    docs = {
        '58-iceberg-v3-proof.json': {
            'schema': 'bigtop.olist.iceberg-materialization.v1',
            'status': 'PASS', 'namespace': 'olist58_v3', 'source_version': 'v3',
            'parity': 'bidirectional_exceptAll_exact_multiset', 'tables': counts,
        },
        '59-olist-wap-stage.json': stage,
        '59-olist-wap-publish.json': {
            'schema': 'bigtop.olist.iceberg-wap-publish.v1', 'status': 'PASS',
            'target_table': step.EXPECTED_TARGET, 'branch': 'audit_v3',
            'main_snapshot_id': '500', 'published_rows': 55877,
            'stage_report_sha256': hashlib.sha256(raw).hexdigest(),
        },
        '60-iceberg-release-last-attempt.json': {
            'schema': 'bigtop.olist.iceberg-release-run.v1', 'status': 'PASS',
            'target': step.EXPECTED_TARGET, 'source_version': 'v3', 'mode': 'run',
        },
    }
    for name, obj in docs.items():
        data = raw if name == '59-olist-wap-stage.json' else json.dumps(obj).encode()
        (artifacts / name).write_bytes(data)
    return artifacts


class FakeOM:
    def __init__(self, specs, missing=False):
        self.items = {step.source_name(z, n): self.record(step.source_name(z, n))
                      for z, n, _ in step.MAPPING}
        if not missing:
            self.items.update({n: self.record(n, spec) for n, spec in specs.items()})
        self.edges = set() if missing else set(step.edge_names())
        self.ops = []
    def record(self, name, spec=None):
        out = {'id': 'id_' + name, 'name': name, 'service': {'name': step.SERVICE}}
        if spec:
            out.update({'prefix': spec['prefix'], 'fullPath': spec['fullPath']})
        return out
    def get(self, path):
        if path.startswith('services/'):
            return {'name': step.SERVICE, 'serviceType': 'CustomStorage'}
        raise AssertionError(path)
    def container(self, name):
        return self.items.get(name)
    def downstream(self, name):
        return {'downstreamEdges': [
            {'fromEntity': self.items[a]['id'], 'toEntity': self.items[b]['id']}
            for a, b in self.edges if a == name
        ]}
    def post(self, path, payload):
        self.ops.append(('POST', path))
        self.items[payload['name']] = self.record(payload['name'], payload)
    def put(self, path, payload):
        self.ops.append(('PUT', path))
        src = payload['edge']['fromEntity']['id']
        dst = payload['edge']['toEntity']['id']
        a = next(k for k, v in self.items.items() if v['id'] == src)
        b = next(k for k, v in self.items.items() if v['id'] == dst)
        self.edges.add((a, b))


class Step61Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.artifacts = write_artifacts(self.root)

    def test_valid_evidence(self):
        evidence = step.evidence_from(self.root)
        self.assertEqual(evidence['gold_rows'], 55877)
        self.assertEqual(len(evidence['snapshots']), 7)

    def test_reject_failed_dq(self):
        path = self.artifacts / '59-olist-wap-stage.json'
        stage = json.loads(path.read_text())
        stage['quality_checks_failed'] = 1
        path.write_text(json.dumps(stage))
        with self.assertRaisesRegex(step.ObservationError, 'quality'):
            step.evidence_from(self.root)

    def test_reject_missing_table_proof(self):
        path = self.artifacts / '58-iceberg-v3-proof.json'
        obj = json.loads(path.read_text())
        del obj['tables']['silver_orders']
        path.write_text(json.dumps(obj))
        with self.assertRaisesRegex(step.ObservationError, 'seven-table'):
            step.evidence_from(self.root)

    def test_reject_stale_stage_digest(self):
        path = self.artifacts / '59-olist-wap-publish.json'
        obj = json.loads(path.read_text())
        obj['stage_report_sha256'] = '0'*64
        path.write_text(json.dumps(obj))
        with self.assertRaisesRegex(step.ObservationError, 'bound to'):
            step.evidence_from(self.root)

    def test_reject_gold_snapshot_drift_in_proof(self):
        path = self.artifacts / '59-olist-wap-publish.json'
        obj = json.loads(path.read_text())
        obj['main_snapshot_id'] = '999'
        path.write_text(json.dumps(obj))
        with self.assertRaises(step.ObservationError):
            step.evidence_from(self.root)

    def test_reject_failed_latest_release(self):
        path = self.artifacts / '60-iceberg-release-last-attempt.json'
        obj = json.loads(path.read_text())
        obj['status'] = 'FAIL'
        path.write_text(json.dumps(obj))
        with self.assertRaisesRegex(step.ObservationError, 'Latest'):
            step.evidence_from(self.root)

    def test_reject_unsafe_table_location(self):
        with self.assertRaises(step.ObservationError):
            step.safe_location('hdfs://master:9000/data/olist/iceberg-lab/other')
        with self.assertRaises(step.ObservationError):
            step.safe_location(step.REST_ROOT + '../bad')

    def test_exact_manual_source_names(self):
        self.assertEqual(step.source_name('curated', 'orders'), 'curated_v3_orders')
        self.assertEqual(len(step.edge_names()), 8)
        self.assertEqual(step.edge_names()[-1], ('iceberg_58_v3_' + step.GOLD,
                                                 'iceberg_59_v3_' + step.GOLD))

    def sample_results(self):
        d = {}
        for _, _, target in step.MAPPING:
            d[target] = {'location': step.REST_ROOT + 'olist58_v3/' + target,
                         'snapshot': '123', 'rows': 10}
        d['published_gold'] = {'location': step.REST_ROOT + 'olist59_v3/' + step.GOLD,
                               'snapshot': '500', 'rows': 55877}
        return d

    def test_containers_have_real_hdfs_paths(self):
        specs = step.container_specs(self.sample_results())
        self.assertEqual(len(specs), 8)
        self.assertTrue(all(v['fullPath'].startswith(step.REST_ROOT)
                            for v in specs.values()))

    def test_plan_does_not_mutate_openmetadata(self):
        specs = step.container_specs(self.sample_results())
        client = FakeOM(specs, missing=True)
        step.reconcile_metadata(client, specs, 'plan')
        self.assertEqual(client.ops, [])
        self.assertEqual(len(client.items), 7)

    def test_apply_idempotence(self):
        specs = step.container_specs(self.sample_results())
        client = FakeOM(specs, missing=True)
        step.reconcile_metadata(client, specs, 'apply')
        self.assertEqual(len([x for x in client.ops if x[0] == 'POST']), 8)
        self.assertEqual(len([x for x in client.ops if x[0] == 'PUT']), 8)
        step.reconcile_metadata(client, specs, 'apply')
        self.assertEqual(len(client.ops), 16)
        step.reconcile_metadata(client, specs, 'verify')

    def test_verify_fails_if_lineage_missing(self):
        specs = step.container_specs(self.sample_results())
        client = FakeOM(specs)
        client.edges.pop()
        with self.assertRaisesRegex(step.ObservationError, 'lineage edge missing'):
            step.reconcile_metadata(client, specs, 'verify')

    def test_fail_on_existing_target_path_collision(self):
        specs = step.container_specs(self.sample_results())
        client = FakeOM(specs)
        client.items['iceberg_58_v3_silver_orders']['fullPath'] = step.REST_ROOT + 'mismatch'
        with self.assertRaisesRegex(step.ObservationError, 'collision'):
            step.reconcile_metadata(client, specs, 'apply')
        self.assertFalse(client.ops)

    def test_atomic_metric_write_and_no_stale_success_on_failure(self):
        dest = self.root / 'olist_iceberg61.prom'
        healthy = step.metric_lines(True, 1762000000, {'gold_rows': 55877})
        step.atomic_metrics(dest, healthy)
        self.assertIn('olist_iceberg61_published_gold_rows{source_version="v3"} 55877',
                      dest.read_text())
        step.atomic_metrics(dest, step.metric_lines(False, 1762000001))
        self.assertIn('olist_iceberg61_observation_success{source_version="v3"} 0',
                      dest.read_text())
        self.assertNotIn('olist_iceberg61_published_gold_rows', dest.read_text())

    def test_reject_non_prom_suffix(self):
        with self.assertRaises(step.ObservationError):
            step.atomic_metrics(self.root / 'bad.txt', 'hi')

    def test_private_bot_token_enforced(self):
        f = self.root / 'token'
        f.write_text('fake_secret')
        f.chmod(0o644)
        with self.assertRaises(step.ObservationError):
            step.private_token(f)
        f.chmod(0o600)
        self.assertEqual(step.private_token(f), 'fake_secret')

    def test_rest_metadata_snapshot(self):
        payload={'metadata': {'location': step.REST_ROOT + 'olist58_v3/silver_orders',
                              'current-snapshot-id': 101, 'refs': {}},
                 'identifier': {'namespace': ['olist58_v3'], 'name': 'silver_orders'}}
        with mock.patch.object(step, 'http_json', return_value=payload):
            self.assertEqual(step.ice_meta('http://host:9001/iceberg',
                                            'olist58_v3','silver_orders')['snapshot'], '101')

    def test_live_snapshot_and_trino_proof_consistency(self):
        evidence = step.evidence_from(self.root)
        def fake_rest(_url, namespace, name):
            if namespace == 'olist59_v3':
                return {'location': step.REST_ROOT + namespace + '/' + name,
                        'snapshot': '500',
                        'refs': {'main': {'snapshot-id': 500},
                                 'audit_v3': {'snapshot-id': 500}}}
            return {'location': step.REST_ROOT + namespace + '/' + name,
                    'snapshot': evidence['snapshots'][name], 'refs': {}}
        def fake_trino(_url, sql):
            table = sql.split('.')[-1]
            return [[evidence['gold_rows'] if 'olist59_v3' in sql
                     else evidence['counts'][table]]]
        with mock.patch.object(step, 'ice_meta', side_effect=fake_rest), \
             mock.patch.object(step, 'trino_rows', side_effect=fake_trino):
            found = step.verify_actual('http://unused/iceberg', 'http://unused', evidence)
        self.assertEqual(len(found), 8)
        self.assertEqual(found['published_gold']['snapshot'], '500')

    def test_live_gold_snapshot_drift_fails_closed(self):
        evidence = step.evidence_from(self.root)
        def fake_rest(_url, namespace, name):
            if namespace == 'olist59_v3':
                return {'location': step.REST_ROOT + namespace + '/' + name,
                        'snapshot': '999',
                        'refs': {'main': {'snapshot-id': 999},
                                 'audit_v3': {'snapshot-id': 500}}}
            return {'location': step.REST_ROOT + namespace + '/' + name,
                    'snapshot': evidence['snapshots'][name], 'refs': {}}
        with mock.patch.object(step, 'ice_meta', side_effect=fake_rest), \
             mock.patch.object(step, 'trino_rows', side_effect=lambda u, sql: [[
                 evidence['gold_rows'] if 'olist59_v3' in sql else
                 evidence['counts'][sql.split('.')[-1]]]]):
            with self.assertRaisesRegex(step.ObservationError, 'snapshot'):
                step.verify_actual('http://unused/iceberg', 'http://unused', evidence)

    def test_python_and_yml_file_are_bundled(self):
        self.assertTrue(SCRIPT.is_file())
        self.assertTrue((ROOT / 'playbooks/61-olist-iceberg-observability.yml').is_file())
        self.assertTrue((ROOT / 'roles/olist_iceberg_observability61/defaults/main.yml').is_file())


if __name__ == '__main__':
    unittest.main()

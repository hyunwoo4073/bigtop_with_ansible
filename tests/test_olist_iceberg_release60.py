"""Unit contracts for Step60 guarded immutable v3 Iceberg release orchestration."""
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

HERE = Path(__file__).resolve().parent.parent
SCRIPT = HERE / 'scripts/60-run-iceberg-release.py'
spec = importlib.util.spec_from_file_location('step60', SCRIPT)
step = importlib.util.module_from_spec(spec)
spec.loader.exec_module(step)


def proof58():
    return {'schema': 'bigtop.olist.iceberg-materialization.v1', 'status': 'PASS',
            'source_version': 'v3', 'namespace': 'olist58_v3',
            'parity': 'bidirectional_exceptAll_exact_multiset',
            'tables': {'gold_daily_category_sales': 55877}}


def stage():
    return {'schema': 'bigtop.olist.iceberg-wap-stage.v1', 'status': 'PASS',
            'target_table': step.EXPECTED_TARGET, 'branch': 'audit_v3',
            'quality_checks_total': 29, 'quality_checks_failed': 0,
            'expected_gold_count': 55877,
            'main_snapshot_before': '111', 'audit_snapshot_id': '222',
            'checks': [{'status': 'PASS', 'id': str(i)} for i in range(29)]}


def publish(raw):
    import hashlib
    return {'schema': 'bigtop.olist.iceberg-wap-publish.v1', 'status': 'PASS',
            'target_table': step.EXPECTED_TARGET, 'branch': 'audit_v3',
            'main_snapshot_id': '222', 'published_rows': 55877,
            'stage_report_sha256': hashlib.sha256(raw).hexdigest()}


class Release60Tests(unittest.TestCase):
    def test_pinned_immutable_gold(self):
        self.assertEqual(step.EXPECTED_TARGET, 'shared.olist59_v3.gold_daily_category_sales')

    def test_proof58_pass(self):
        self.assertEqual(step.verify_proof58(proof58()), 55877)

    def test_proof58_reject_inexact(self):
        o = proof58()
        o['parity'] = 'count_only'
        with self.assertRaises(step.GateError):
            step.verify_proof58(o)

    def test_proof58_reject_zero_or_bool(self):
        for val in [0, True, None]:
            with self.subTest(val=val):
                o = proof58()
                o['tables']['gold_daily_category_sales'] = val
                with self.assertRaises(step.GateError):
                    step.verify_proof58(o)

    def test_stage_all_29_pass(self):
        self.assertEqual(step.verify_stage(stage(), 55877), 55877)

    def test_stage_reject_failed_check(self):
        p = stage()
        p['checks'][0]['status'] = 'FAIL'
        with self.assertRaises(step.GateError):
            step.verify_stage(p, 55877)

    def test_stage_reject_wrong_count(self):
        with self.assertRaises(step.GateError):
            step.verify_stage(stage(), 500)

    def test_stage_reject_same_snapshot(self):
        p = stage()
        p['main_snapshot_before'] = p['audit_snapshot_id']
        with self.assertRaises(step.GateError):
            step.verify_stage(p, 55877)

    def test_publish_sha256_binding(self):
        raw = json.dumps(stage()).encode()
        step.verify_publish(publish(raw), stage(), raw)
        with self.assertRaises(step.GateError):
            step.verify_publish(publish(raw), stage(), raw + b' ')

    def test_local_evidence_reject_publish_wrong_ref(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'artifacts').mkdir()
            (root / 'artifacts' / step.PROOF58).write_text(json.dumps(proof58()))
            raw = json.dumps(stage()).encode()
            (root / 'artifacts' / step.STAGE).write_bytes(raw)
            p = publish(raw)
            p['main_snapshot_id'] = '777'
            (root / 'artifacts' / step.PUBLISH).write_text(json.dumps(p))
            with self.assertRaises(step.GateError):
                step.read_evidence(root, require_publication=True)

    def test_local_evidence_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'artifacts').mkdir()
            (root / 'artifacts' / step.PROOF58).write_text(json.dumps(proof58()))
            raw = json.dumps(stage()).encode()
            (root / 'artifacts' / step.STAGE).write_bytes(raw)
            (root / 'artifacts' / step.PUBLISH).write_text(json.dumps(publish(raw)))
            self.assertEqual(step.read_evidence(root, True), 55877)

    def test_publication_state_seed(self):
        with patch.object(step, 'trino_sql', return_value=[[1]]) as query:
            self.assertEqual(step.publication_state('http://metadata:8080', 55877), 'staged')
            query.assert_called_once()

    def test_publication_state_published(self):
        with patch.object(step, 'trino_sql', return_value=[[55877]]):
            self.assertEqual(step.publication_state('http://metadata:8080', 55877),
                             'published_candidate')

    def test_publication_state_reject_partial(self):
        for rows in ([[0]], [[2]], [[True]], []):
            with self.subTest(rows=rows):
                with patch.object(step, 'trino_sql', return_value=rows):
                    with self.assertRaises(step.GateError):
                        step.publication_state('http://metadata:8080', 55877)

    def test_publish_requires_optin_before_network(self):
        with patch.object(step, 'read_evidence') as read:
            with self.assertRaises(step.GateError):
                step.publish_phase(Path('/tmp/project'), Path('/tmp/inv'), 'http://m', False)
            read.assert_not_called()

    def test_publish_quality_failure_never_calls_ansible(self):
        with patch.object(step, 'read_evidence', side_effect=step.GateError('bad DQ')):
            with patch.object(step, 'ansible') as ansible:
                with self.assertRaises(step.GateError):
                    step.publish_phase(Path('/tmp/project'), Path('/tmp/inv'), 'http://m', True)
                ansible.assert_not_called()

    def test_publish_uses_stage_verify_before_publish(self):
        events = []
        with patch.object(step, 'read_evidence', side_effect=[55877, 55877]), \
             patch.object(step, 'publication_state', return_value='staged'), \
             patch.object(step, 'trino_verify', side_effect=lambda *args, **kw: events.append('verify' + str(kw.get('mode')))), \
             patch.object(step, 'ansible', side_effect=lambda *args, **kw: events.append('publish')):
            step.publish_phase(Path('/tmp/p'), Path('/tmp/i'), 'http://m', True)
        self.assertEqual(events, ['verifystage', 'publish', 'verifypublish'])

    def test_already_published_skips_prepublication_seed_check(self):
        events = []
        with patch.object(step, 'read_evidence', side_effect=[55877, 55877]), \
             patch.object(step, 'publication_state', return_value='published_candidate'), \
             patch.object(step, 'trino_verify', side_effect=lambda *args, **kw: events.append('verify' + str(kw.get('mode')))), \
             patch.object(step, 'ansible', side_effect=lambda *args, **kw: events.append('publish')):
            step.publish_phase(Path('/tmp/p'), Path('/tmp/i'), 'http://m', True)
        self.assertEqual(events, ['publish', 'verifypublish'])

    def test_stage_rechecks_full_chain(self):
        events = []
        with patch.object(step, 'ansible', side_effect=lambda *args, **kw: events.append('ansible')), \
             patch.object(step, 'trino_verify', side_effect=lambda *args, **kw: events.append('trino')), \
             patch.object(step, 'read_evidence', return_value=55877), \
             patch.object(step, 'publication_state', return_value='staged'):
            step.stage_phase(Path('/tmp/p'), Path('/tmp/i'), 'http://m')
        self.assertEqual(events, ['ansible', 'trino', 'ansible', 'trino'])

    def test_no_systemd_timer_in_package(self):
        self.assertFalse(any(p.suffix == '.timer' for p in HERE.rglob('*')))

    def test_role_follows_prefix(self):
        f = HERE / 'roles/olist_iceberg_release60/defaults/main.yml'
        self.assertTrue(f.exists())
        for line in f.read_text().splitlines():
            if line.startswith('olist_'):
                self.assertTrue(line.startswith('olist_iceberg_release60_'))

    def test_plan_does_not_call_shell(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'inventory').mkdir()
            (root / 'inventory/hosts.ini').write_text('[all]\nmaster\n')
            args = SimpleNamespace(project=str(root), inventory=None,
                                   trino_url='http://metadata:8080', mode='plan',
                                   allow_publish=False)
            with patch.object(step, 'call') as cmd:
                step.run(args)
                cmd.assert_not_called()

    def test_run_requires_publish_flag_even_if_stage_already_done(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'inventory').mkdir()
            (root / 'inventory/hosts.ini').write_text('')
            args = SimpleNamespace(project=str(root), inventory=None,
                                   trino_url='http://metadata:8080', mode='run',
                                   allow_publish=False)
            with patch.object(step, 'validate_local'):
                with self.assertRaises(step.GateError):
                    step.run(args)


if __name__ == '__main__':
    unittest.main()

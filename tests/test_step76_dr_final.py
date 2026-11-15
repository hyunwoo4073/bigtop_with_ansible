"""Unit tests for local Step76 D5 evidence-only orchestrator."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'step76_dr_final.py'
spec = importlib.util.spec_from_file_location('step76_dr_final', MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
ID = '20261115T082737Z-dc572b3b'


class Step76FinalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        identity = self.root / mod.BACKUP_ID_FILE
        identity.parent.mkdir(parents=True)
        identity.write_text(ID + '\n')
        for _, file, _, _ in mod.STAGES:
            script = self.root / file
            script.parent.mkdir(parents=True, exist_ok=True)
            script.write_text('#!/bin/sh\n')

    def test_backup_id_and_safe_plan(self):
        self.assertEqual(mod.backup_id_from_file(self.root), ID)
        self.assertEqual(len(mod.plan(self.root)), 5)
        for step in mod.plan(self.root):
            self.assertEqual(step[3][0], 'verify')

    def test_reject_bad_id(self):
        (self.root / mod.BACKUP_ID_FILE).write_text('../wrong')
        with self.assertRaises(ValueError):
            mod.backup_id_from_file(self.root)

    def test_reject_symlink_id(self):
        identity = self.root / mod.BACKUP_ID_FILE
        identity.unlink()
        origin = self.root / 'other'
        origin.write_text(ID)
        identity.symlink_to(origin)
        with self.assertRaises(ValueError):
            mod.backup_id_from_file(self.root)

    def test_reject_missing_verifier(self):
        (self.root / mod.STAGES[0][1]).unlink()
        with self.assertRaises(ValueError):
            mod.plan(self.root)

    def test_marker_and_exit_code_both_required(self):
        stage = mod.STAGES[0]
        class Result:
            returncode = 1
            stdout = stage[2]
        failure = mod.run_stage(self.root, stage, run=lambda *args, **kwargs: Result())
        self.assertEqual(failure['status'], 'FAIL')
        class NoMarker:
            returncode = 0
            stdout = 'some output'
        missing = mod.run_stage(self.root, stage, run=lambda *args, **kwargs: NoMarker())
        self.assertEqual(missing['status'], 'FAIL')
        class Success:
            returncode = 0
            stdout = stage[2]
        success = mod.run_stage(self.root, stage, run=lambda *args, **kwargs: Success())
        self.assertEqual(success['status'], 'PASS')

    def test_stop_at_first_failure_and_write_report(self):
        called = []
        def runner(root, stage):
            called.append(stage[0])
            return {'stage': stage[0], 'status': 'FAIL', 'marker': stage[2]}
        report = mod.execute(self.root, runner=runner)
        self.assertEqual(report['result'], 'FAIL')
        self.assertEqual(len(called), 1)
        location = self.root / 'artifacts/step76' / f'dr-final-evidence-{ID}.json'
        self.assertTrue(location.exists())
        self.assertEqual(json.loads(location.read_text())['result'], 'FAIL')
        self.assertEqual(location.stat().st_mode & 0o777, 0o600)

    def test_full_success_creates_bounded_not_full_dr_report(self):
        def runner(root, stage):
            return {'stage': stage[0], 'status': 'PASS', 'marker': stage[2]}
        report = mod.execute(self.root, runner=runner)
        self.assertEqual(len(report['stages']), 5)
        self.assertEqual(report['result'], 'STEP76_D5_LAB_EVIDENCE_PASS')
        md = (self.root / 'artifacts/step76' / f'dr-final-evidence-{ID}.md').read_text()
        self.assertIn('RPO/RTO', md)
        self.assertIn('전체', md)
        self.assertIn('metadata VM', md)

    def test_no_backup_copy_restore_configure_commands(self):
        self.assertEqual([x[3][0] for x in mod.STAGES], ['verify'] * 5)
        self.assertEqual([x[1] for x in mod.STAGES], [
            'scripts/run_step76_backup.sh',
            'scripts/run_step76_restore.sh',
            'scripts/run_step76_offhost.sh',
            'scripts/run_step76_hdfs.sh',
            'scripts/run_step76_es.sh'])

if __name__ == '__main__':
    unittest.main()

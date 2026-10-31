"""48 Olist orchestrator tests. Only stdlib and mock programs; no live Hadoop needed."""
from __future__ import annotations

import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
ROLE = ROOT / "roles/data_pipeline_olist_scheduler"
SCRIPT = ROLE / "templates/run-pipeline.sh.j2"
TASKS = ROLE / "tasks/main.yml"
RULES = ROLE / "templates/olist-pipeline.rules.yml.j2"


def make_script(base: Path) -> Path:
    """Render test constants into the shell template without requiring Jinja in system python."""
    project = base / 'project'
    state = base / 'state'
    metrics = base / 'metrics'
    project.mkdir()
    state.mkdir()
    metrics.mkdir()
    (project / 'inventory').mkdir()
    (project / 'inventory/hosts.ini').write_text('[master]\nmaster\n')
    (project / 'playbooks').mkdir()
    (project / 'playbooks/46-olist-spark-etl.yml').write_text('---\n')
    (project / 'playbooks/47-olist-data-quality.yml').write_text('---\n')
    mock_ansible = project / 'ansible-playbook'
    mock_ansible.write_text('''#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$MOCK_TRACE"
case "$*" in
*46-olist-spark-etl.yml*) test "${FAIL_ETL:-0}" = 0 ;;
*47-olist-data-quality.yml*) test "${FAIL_QUALITY:-0}" = 0 ;;
*) exit 88 ;;
esac
''')
    mock_ansible.chmod(0o755)
    mock_runuser = project / 'runuser'
    mock_runuser.write_text('''#!/bin/sh
set -eu
[ "$1" = "-u" ] || exit 89
shift 2
[ "$1" = "--" ] || exit 90
shift
exec "$@"
''')
    mock_runuser.chmod(0o755)
    mapping = {
        'PROJECT': project,
        'INVENTORY': project / 'inventory/hosts.ini',
        'ANSIBLE': mock_ansible,
        'RUNNER': 'test-user',
        'RAW_VERSION': 'v1',
        'WAREHOUSE_VERSION': 'v1',
        'STATE_DIR': state,
        'METRICS_FILE': metrics / 'olist_pipeline.prom',
    }
    content = SCRIPT.read_text()
    for key, val in mapping.items():
        content = re.sub(r'^' + key + r'=.*$', key+'='+shlex.quote(str(val)), content, count=1, flags=re.M)
    content = content.replace('runuser -u "$RUNNER" --', shlex.quote(str(mock_runuser))+' -u "$RUNNER" --')
    assert '{{' not in content, 'unexpected rendering expression in script'
    output = project / 'run-pipeline.sh'
    output.write_text(content)
    output.chmod(0o755)
    return output


class PipelineSchedulerTests(unittest.TestCase):
    def test_expected_package_paths(self):
        self.assertTrue((ROOT/'playbooks/48-olist-pipeline-schedule.yml').is_file())
        self.assertTrue((ROLE/'templates/bigtop-olist-pipeline.timer.j2').is_file())

    def test_no_unconditional_timer_enable(self):
        text = (ROLE/'defaults/main.yml').read_text()
        self.assertIn('data_pipeline_olist_scheduler_timer_enabled: false', text)
        self.assertIn('data_pipeline_olist_scheduler_alerts_enabled: false', text)

    def test_timer_only_triggers_oneshot(self):
        unit = (ROLE/'templates/bigtop-olist-pipeline.timer.j2').read_text()
        service = (ROLE/'templates/bigtop-olist-pipeline.service.j2').read_text()
        self.assertIn('Unit=bigtop-olist-pipeline.service', unit)
        self.assertIn('Type=oneshot', service)
        self.assertIn('TimeoutStartSec=7200', service)

    def test_rules_catch_failure_missing_and_stale(self):
        rules = RULES.read_text()
        self.assertEqual(rules.count('- alert: '), 6)
        for name in ('OlistPipelineExecutionFailed', 'OlistPipelineExecutionStuck',
                     'OlistPipelineNeverExecuted', 'OlistPipelineScheduledRunStale',
                     'OlistQualityValidationFailed', 'OlistQualityReportStale'):
            self.assertIn(name, rules)

    def test_no_unintended_destructive_commands(self):
        for file in (SCRIPT, TASKS):
            text = file.read_text()
            self.assertNotIn('hdfs dfs -rm', text)
            self.assertNotIn('docker compose down -v', text)
            self.assertNotIn('git reset --hard', text)

    def test_role_requires_nonroot_runner(self):
        text = TASKS.read_text()
        self.assertIn("data_pipeline_olist_scheduler_runner_user != 'root'", text)
        self.assertIn('Confirm Ansible runner account is present', text)

    def test_script_has_no_syntax_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            script = make_script(Path(tmp))
            result = subprocess.run(['bash','-n',str(script)],capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)

    def test_etl_then_quality_and_success_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script = make_script(base)
            trace = base/'trace'
            env = dict(os.environ, MOCK_TRACE=str(trace))
            run = subprocess.run([str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(run.returncode,0,run.stderr)
            lines = trace.read_text().splitlines()
            self.assertEqual(len(lines),2)
            self.assertIn('46-olist-spark-etl.yml',lines[0])
            self.assertIn('47-olist-data-quality.yml',lines[1])
            self.assertIn('data_pipeline_olist_etl_output_version',lines[0])
            metrics = (base/'metrics/olist_pipeline.prom').read_text()
            self.assertIn('olist_pipeline_running{warehouse_version="v1"} 0',metrics)
            self.assertIn('olist_pipeline_last_attempt_success{warehouse_version="v1"} 1',metrics)
            self.assertRegex(metrics,r'olist_pipeline_last_success_timestamp_seconds\{warehouse_version="v1"\} [1-9][0-9]+')

    def test_etl_failure_prevents_quality(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp)
            script=make_script(base)
            trace=base/'trace'
            env=dict(os.environ,MOCK_TRACE=str(trace),FAIL_ETL='1')
            result=subprocess.run([str(script)],env=env,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            lines=trace.read_text().splitlines()
            self.assertEqual(len(lines),1)
            self.assertIn('46-olist-spark-etl.yml',lines[0])
            metrics=(base/'metrics/olist_pipeline.prom').read_text()
            self.assertIn('olist_pipeline_last_attempt_success{warehouse_version="v1"} 0',metrics)
            self.assertIn('olist_pipeline_running{warehouse_version="v1"} 0',metrics)

    def test_quality_failure_preserves_previous_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp)
            script=make_script(base)
            trace=base/'trace'
            env=dict(os.environ,MOCK_TRACE=str(trace))
            self.assertEqual(subprocess.run([str(script)],env=env,capture_output=True).returncode,0)
            first=(base/'state/last_success_epoch').read_text().strip()
            env['FAIL_QUALITY']='1'
            result=subprocess.run([str(script)],env=env,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertEqual((base/'state/last_success_epoch').read_text().strip(),first)
            metrics=(base/'metrics/olist_pipeline.prom').read_text()
            self.assertIn('olist_pipeline_last_attempt_success{warehouse_version="v1"} 0',metrics)
            self.assertIn('olist_pipeline_etl_success{warehouse_version="v1"} 1',metrics)
            self.assertIn('olist_pipeline_quality_success{warehouse_version="v1"} 0',metrics)

    def test_no_artifact_csv_or_credentials_in_package(self):
        # Inspect only files shipped by step 48, not the whole working tree.
        # In particular, olist/source/*.csv is intentional local input for step 45.
        package_files = [
            ROOT / 'docs/48-olist-apply.md',
            ROOT / 'docs/48-olist-pipeline-automation.md',
            ROOT / 'playbooks/48-olist-pipeline-schedule.yml',
            ROOT / 'tests/test_olist_scheduler.py',
        ]
        package_files.extend(ROLE.rglob('*'))
        forbidden_names = {'.env', 'hosts.ini', 'all.yml'}
        forbidden_suffixes = {'.csv', '.zip'}
        for path in package_files:
            if not path.is_file():
                continue
            with self.subTest(path=str(path.relative_to(ROOT))):
                self.assertNotIn(path.name, forbidden_names)
                self.assertNotIn(path.suffix.lower(), forbidden_suffixes)


if __name__ == '__main__':
    unittest.main()

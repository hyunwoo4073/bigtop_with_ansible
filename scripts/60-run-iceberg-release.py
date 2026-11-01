#!/usr/bin/env python3
"""Step 60: fail-closed orchestration for the immutable Olist v3 Iceberg Gold release.

Runs Step58's exact Spark parity, Trino reads, Step59 staging + DQ,
independent stage protection, explicit Gold publish, and Trino post-publish
verification. This is a one-shot release runner, not a new-data scheduler.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

CATALOG = 'iceberg_rest'
SOURCE = f'{CATALOG}.olist58_v3.gold_daily_category_sales'
TARGET = f'{CATALOG}.olist59_v3.gold_daily_category_sales'
STAGE = '59-olist-wap-stage.json'
PUBLISH = '59-olist-wap-publish.json'
PROOF58 = '58-iceberg-v3-proof.json'
EXPECTED_TARGET = 'shared.olist59_v3.gold_daily_category_sales'
LAST_ATTEMPT = '60-iceberg-release-last-attempt.json'
LOCK = '.60-iceberg-release.lock'
PASS58 = 'STEP58_SPARK_TRINO_SILVER_GOLD_CROSS_QUERY_PASS'
PASS_STAGE = 'STEP59_TRINO_STAGE_MAIN_PROTECTED_PASS'
PASS_PUBLISH = 'STEP59_TRINO_SPARK_WAP_CROSS_QUERY_PASS'


class GateError(RuntimeError):
    pass


def require(predicate, msg):
    if not predicate:
        raise GateError(msg)


def load_json(path):
    obj = json.loads(Path(path).read_text(encoding='utf-8'))
    require(isinstance(obj, dict), f'Expected JSON object: {path}')
    return obj


def verify_proof58(proof):
    require(proof.get('schema') == 'bigtop.olist.iceberg-materialization.v1' and
            proof.get('status') == 'PASS' and proof.get('source_version') == 'v3' and
            proof.get('namespace') == 'olist58_v3' and
            proof.get('parity') == 'bidirectional_exceptAll_exact_multiset',
            'Step58 requires exact approved v3 multiset parity proof')
    rows = proof.get('tables', {}).get('gold_daily_category_sales')
    require(type(rows) is int and rows > 1, 'Invalid Step58 Gold row count')
    return rows


def verify_stage(stage, expected):
    require(stage.get('schema') == 'bigtop.olist.iceberg-wap-stage.v1' and
            stage.get('status') == 'PASS' and
            stage.get('target_table') == EXPECTED_TARGET and
            stage.get('branch') == 'audit_v3' and
            stage.get('quality_checks_total') == 29 and
            stage.get('quality_checks_failed') == 0 and
            type(stage.get('expected_gold_count')) is int and
            stage['expected_gold_count'] == expected and
            isinstance(stage.get('checks'), list) and
            len(stage['checks']) == 29 and
            all(isinstance(c, dict) and c.get('status') == 'PASS' for c in stage['checks']),
            'Step59 Stage DQ 29 PASS proof absent/stale/invalid; publication denied')
    for key in ('main_snapshot_before', 'audit_snapshot_id'):
        value = stage.get(key)
        require(isinstance(value, str) and value.isdigit(),
                'Invalid stage snapshot: ' + key)
    require(stage['main_snapshot_before'] != stage['audit_snapshot_id'],
            'Audit candidate is not a distinct snapshot')
    return expected


def verify_publish(publish, stage, stage_raw):
    require(publish.get('schema') == 'bigtop.olist.iceberg-wap-publish.v1' and
            publish.get('status') == 'PASS' and
            publish.get('target_table') == EXPECTED_TARGET and
            publish.get('branch') == 'audit_v3' and
            publish.get('main_snapshot_id') == stage['audit_snapshot_id'] and
            publish.get('published_rows') == stage['expected_gold_count'] and
            publish.get('stage_report_sha256') == hashlib.sha256(stage_raw).hexdigest(),
            'Publication proof is missing, not snapshot-bound, or inconsistent')


def read_evidence(project, require_publication=False):
    artifacts = project / 'artifacts'
    proof58 = load_json(artifacts / PROOF58)
    expected = verify_proof58(proof58)
    stage_raw = (artifacts / STAGE).read_bytes()
    stage = json.loads(stage_raw)
    require(isinstance(stage, dict), 'Invalid Stage JSON')
    verify_stage(stage, expected)
    if require_publication:
        verify_publish(load_json(artifacts / PUBLISH), stage, stage_raw)
    return expected


def validate_local(project, inventory, mode):
    require(project.is_dir() and inventory.is_file(), 'Project or inventory missing')
    filenames = [
        'scripts/58-verify-trino.py', 'scripts/59-verify-trino.py',
        'playbooks/58-olist-iceberg-materialization.yml',
        'playbooks/59-olist-iceberg-gold-wap.yml',
    ]
    if mode != 'plan':
        for name in filenames:
            require((project / name).is_file(), f'Missing prerequisite {name}')
        require((project / '.venv/bin/ansible-playbook').is_file(),
                'Missing project .venv/bin/ansible-playbook')


def call(cmd, cwd, marker=None):
    # Commands are argv arrays, never shell-evaluated; no secrets in arguments.
    print('STEP60_RUN: ' + ' '.join(str(x) for x in cmd), flush=True)
    result = subprocess.run(cmd, cwd=str(cwd), text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    if result.stdout:
        print(result.stdout, end='' if result.stdout.endswith('\n') else '\n', flush=True)
    require(result.returncode == 0, f'Command failed rc={result.returncode}: {cmd[0]} {cmd[-1]}')
    if marker:
        require(marker in result.stdout, 'Missing required verification marker: ' + marker)


def trino_json(url, sql=None, timeout=45):
    headers = {'X-Trino-User': 'step60_release', 'Accept': 'application/json'}
    data = sql.encode('utf-8') if sql is not None else None
    if data is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    req = Request(url, headers=headers, data=data,
                  method='POST' if data is not None else 'GET')
    with urlopen(req, timeout=timeout) as stream:
        return json.load(stream)


def trino_sql(base, sql):
    base = base.rstrip('/')
    page = trino_json(base + '/v1/statement', sql)
    rows = []
    deadline = time.monotonic() + 300
    while True:
        if page.get('error'):
            error = page['error']
            raise GateError('Trino SQL failed: ' + str(error.get('errorName')) + ': ' +
                            str(error.get('message')))
        rows.extend(page.get('data', []))
        next_uri = page.get('nextUri')
        if not next_uri:
            return rows
        require(time.monotonic() < deadline, 'Trino SQL query timed out')
        u, b = urlsplit(next_uri), urlsplit(base)
        require(u.scheme == b.scheme and u.netloc == b.netloc,
                'Trino nextUri changed host or scheme')
        time.sleep(.15)
        page = trino_json(next_uri)


def publication_state(trino_url, expected):
    rows = trino_sql(trino_url, f'SELECT count(*) FROM {TARGET}')
    require(len(rows) == 1 and len(rows[0]) == 1 and type(rows[0][0]) is int,
            'Unexpected Trino Gold count result')
    count = rows[0][0]
    if count == 1:
        return 'staged'
    if count == expected:
        return 'published_candidate'
    raise GateError(f'Unexpected 59 main count {count}; expected seed=1 or published={expected}')


def ansible(project, inventory, step, mode=None):
    command = [str(project / '.venv/bin/ansible-playbook'), '-i', str(inventory),
               f'playbooks/{step}']
    if mode:
        command += ['-e', 'olist_iceberg_wap_mode=' + mode]
    else:
        command += ['-e', 'olist_iceberg_materialization_apply=true']
    call(command, project)


def trino_verify(project, url, kind, mode=None):
    if kind == 58:
        call([sys.executable, 'scripts/58-verify-trino.py', '--url', url,
              '--proof', f'artifacts/{PROOF58}'], project, marker=PASS58)
    else:
        args = [sys.executable, 'scripts/59-verify-trino.py', '--mode', mode,
                '--url', url, '--stage-proof', f'artifacts/{STAGE}',
                '--publish-proof', f'artifacts/{PUBLISH}']
        call(args, project, marker=PASS_STAGE if mode == 'stage' else PASS_PUBLISH)


def stage_phase(project, inventory, url):
    # Exact Spark parity is checked by Step58, and Trino reads all seven tables.
    ansible(project, inventory, '58-olist-iceberg-materialization.yml')
    trino_verify(project, url, 58)
    ansible(project, inventory, '59-olist-iceberg-gold-wap.yml', mode='stage')
    expected = read_evidence(project)
    state = publication_state(url, expected)
    if state == 'staged':
        trino_verify(project, url, 59, mode='stage')
        print('STEP60_STAGE_GATED_PASS: 58 parity + Trino + 29 DQ; main seed protected', flush=True)
    else:
        # Re-run after success: the seed was already published. Never claim main protected.
        require((project / 'artifacts' / PUBLISH).exists(),
                'Gold main looks published but local immutable publish evidence is missing; '
                'manual investigation required')
        read_evidence(project, require_publication=True)
        trino_verify(project, url, 59, mode='publish')
        print('STEP60_ALREADY_PUBLISHED_VERIFIED: immutable v3 recheck', flush=True)
    return state


def publish_phase(project, inventory, url, allow_publish):
    require(allow_publish, 'Publication requires explicit --allow-publish')
    expected = read_evidence(project)
    state = publication_state(url, expected)
    if state == 'staged':
        # An independent pre-publish check: Trino must see only the technical seed.
        trino_verify(project, url, 59, mode='stage')
    else:
        # If already released, Step59 will revalidate identical HDFS evidence and refs.
        print('STEP60_REVALIDATE_EXISTING_PUBLICATION', flush=True)
    # Step59 publish re-reads immutable HDFS proof, verifies 29 checks, source
    # snapshot refs, audit branch snapshot, current main, then fast_forwards.
    ansible(project, inventory, '59-olist-iceberg-gold-wap.yml', mode='publish')
    read_evidence(project, require_publication=True)
    trino_verify(project, url, 59, mode='publish')
    print('STEP60_GOLD_RELEASE_PASS: one Gold main; no seven-table atomic transaction', flush=True)


def verify_phase(project, url):
    expected = read_evidence(project, require_publication=True)
    trino_verify(project, url, 58)
    require(publication_state(url, expected) == 'published_candidate',
            'Gold main has not been published')
    trino_verify(project, url, 59, mode='publish')
    print('STEP60_READ_ONLY_VERIFY_PASS', flush=True)


def write_receipt(project, receipt):
    dest = project / 'artifacts' / LAST_ATTEMPT
    temp = dest.with_name(dest.name + '.tmp-' + str(os.getpid()))
    try:
        temp.write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n',
                        encoding='utf-8')
        os.chmod(temp, 0o600)
        os.replace(temp, dest)
    finally:
        if temp.exists():
            temp.unlink()


def run(args):
    project = Path(args.project).expanduser().resolve()
    inventory = Path(args.inventory).expanduser().resolve() if args.inventory else project / 'inventory/hosts.ini'
    validate_local(project, inventory, args.mode)
    require(args.trino_url.startswith(('http://', 'https://')),
            'Trino URL must be http(s)')
    if args.mode in ('publish', 'run'):
        require(args.allow_publish, 'Use --allow-publish to authorize Gold main publication')
    if args.mode == 'plan':
        print('STEP60_PLAN_ONLY: no Spark job, service installation, HDFS or Iceberg writes')
        print('Plan: Step58 Spark parity -> Trino all 7 -> Step59 stage/29 DQ -> '
              'Trino main protection -> explicit Gold publish -> Trino Gold validation')
        print('Immutable v3 only; no timer, new raw ingestion, or multi-table atomic release')
        return

    artifacts = project / 'artifacts'
    artifacts.mkdir(parents=True, exist_ok=True)
    with (artifacts / LOCK).open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise GateError('Another Step60 release runner holds the controller lock') from exc
        started = datetime.now(timezone.utc).isoformat()
        receipt = {'schema': 'bigtop.olist.iceberg-release-run.v1',
                   'source_version': 'v3', 'mode': args.mode,
                   'target': EXPECTED_TARGET,
                   'started_at_utc': started,
                   'status': 'FAIL', 'last_phase': 'preflight'}
        try:
            if args.mode in ('stage', 'run'):
                receipt['last_phase'] = 'stage'
                stage_phase(project, inventory, args.trino_url)
            if args.mode in ('publish', 'run'):
                receipt['last_phase'] = 'publish'
                publish_phase(project, inventory, args.trino_url, args.allow_publish)
            if args.mode == 'verify':
                receipt['last_phase'] = 'verify'
                verify_phase(project, args.trino_url)
            receipt['status'] = 'PASS'
            print('STEP60_PIPELINE_PASS: mode=' + args.mode, flush=True)
        except Exception as exc:
            receipt['error'] = f'{type(exc).__name__}: {str(exc)[:500]}'
            raise
        finally:
            receipt['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
            write_receipt(project, receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('plan', 'stage', 'publish', 'run', 'verify'),
                        default='plan')
    parser.add_argument('--project', default=str(Path(__file__).resolve().parent.parent))
    parser.add_argument('--inventory', default=None)
    parser.add_argument('--trino-url', default='http://192.158.2.104:8080')
    parser.add_argument('--allow-publish', action='store_true',
                        help='Required for publish/run; authorizes Gold main fast_forward')
    args = parser.parse_args()
    try:
        run(args)
    except (GateError, OSError, ValueError, RuntimeError, subprocess.SubprocessError,
            json.JSONDecodeError) as exc:
        print(f'STEP60_PIPELINE_FAIL: {type(exc).__name__}: {exc}',
              file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

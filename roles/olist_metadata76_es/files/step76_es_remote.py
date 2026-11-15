#!/usr/bin/env python3
"""Step76 D4: bounded OpenMetadata Elasticsearch snapshot / single-index restore proof.

Only metadata host, fixed Compose project & filesystem repository. Explicit mode required.
"""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path('/opt/bigtop-governance/openmetadata')
COMPOSE = ROOT / 'compose.yaml'
ENV_FILE = ROOT / '.env'
HOST_REPO = Path('/var/backups/bigtop-step76-es')
CONTAINER_REPO = '/usr/share/elasticsearch/snapshots'
REPOSITORY = 'step76_om_fs'
PROJECT = 'bigtop-openmetadata'
COMPOSE_ARGS = [
    'docker', 'compose', '--project-name', PROJECT, '--file', str(COMPOSE),
    '--env-file', str(ENV_FILE),
]
BACKUP_ID_RE = re.compile(r'^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$')


class StepError(RuntimeError):
    pass


def cmd(args, data=None, timeout=60, check=True):
    proc = subprocess.run(args, input=data, text=True, capture_output=True,
                          timeout=timeout, check=False)
    if check and proc.returncode:
        detail = (proc.stderr or proc.stdout).strip()[-600:]
        raise StepError(f'Command failed ({args[0]} rc={proc.returncode}): {detail}')
    return proc


def container_id():
    proc = cmd(['docker', 'ps', '-q', '--filter', f'label=com.docker.compose.project={PROJECT}',
                '--filter', 'label=com.docker.compose.service=elasticsearch'])
    ids = proc.stdout.strip().splitlines()
    if len(ids) != 1 or not re.fullmatch(r'[a-f0-9]{12,64}', ids[0]):
        raise StepError('Expected exactly one running OpenMetadata Elasticsearch container')
    return ids[0]


def api(method, path, body=None, timeout=45):
    cid = container_id()
    args = ['docker', 'exec']
    if body is not None:
        args.append('-i')
    args += [cid, 'curl', '-fsS', '--max-time', str(timeout), '-X', method,
             '-H', 'Content-Type: application/json']
    if body is not None:
        args += ['--data-binary', '@-']
    args += ['http://127.0.0.1:9200' + path]
    out = cmd(args, data=json.dumps(body) if body is not None else None,
              timeout=timeout + 25).stdout
    try:
        reply = json.loads(out)
    except ValueError as exc:
        raise StepError('Elasticsearch returned invalid JSON') from exc
    if isinstance(reply, dict) and (
        'error' in reply
        or (isinstance(reply.get('status'), int) and reply['status'] >= 400)
    ):
        raise StepError('Elasticsearch API returned error: ' + str(reply.get('error'))[:300])
    return reply


def node_repo_paths():
    resp = api('GET', '/_nodes/settings?filter_path=nodes.*.settings.path.repo')
    vals = []
    for obj in resp.get('nodes', {}).values():
        value = obj.get('settings', {}).get('path', {}).get('repo', [])
        if isinstance(value, str):
            vals.append(value)
        else:
            vals.extend(value)
    return vals


def snapshot_id(backup_id):
    if not BACKUP_ID_RE.fullmatch(backup_id):
        raise StepError('Invalid Step76 backup ID')
    return 'step76_' + backup_id.lower().replace('-', '_')


def status(backup_id):
    if not COMPOSE.exists() or not ENV_FILE.exists():
        raise StepError('Missing existing Step41 Compose deployment or .env')
    health = api('GET', '/_cluster/health')
    if health.get('status') not in ('green', 'yellow'):
        raise StepError('Elasticsearch is not green/yellow')
    paths = node_repo_paths()
    repos = api('GET', '/_snapshot')
    disk = shutil.disk_usage('/var/backups' if Path('/var/backups').exists() else '/')
    idx = api('GET', '/_cat/indices?format=json&h=index,health')
    if not isinstance(idx, list):
        raise StepError('Cannot read Elasticsearch indices')
    return dict(
        result='STEP76_ES_PREFLIGHT_PASS', backup_id=backup_id,
        cluster_health=health.get('status'), index_count=len(idx),
        repository_configured=CONTAINER_REPO in paths,
        repository_registered=REPOSITORY in repos,
        free_mib=disk.free // (1024*1024),
        requires_es_restart=(CONTAINER_REPO not in paths),
        note='ES snapshot repository lives on metadata VM; not offhost DR. This command is read-only.',
    )


def patch_compose(text):
    start = text.find('\n  elasticsearch:\n')
    if start < 0:
        raise StepError('Expected Compose elasticsearch service was not found')
    end = text.find('\n  execute-migrate-all:', start)
    if end < 0:
        raise StepError('Expected next OpenMetadata Compose service not found')
    block = text[start:end]
    anchor_env = "      ES_JAVA_OPTS: '-Xms768m -Xmx768m'"
    anchor_volume = '      - es-data:/usr/share/elasticsearch/data'
    setting = '      path.repo: ' + CONTAINER_REPO
    mount = '      - ' + str(HOST_REPO) + ':' + CONTAINER_REPO
    if anchor_env not in block or anchor_volume not in block:
        raise StepError('Unrecognized Elasticsearch Compose block; refusing edits')
    if 'path.repo:' in block and setting not in block:
        raise StepError('Another path.repo setting already exists')
    if ':' + CONTAINER_REPO in block and mount not in block:
        raise StepError('Another snapshot mount already exists')
    if setting not in block:
        block = block.replace(anchor_env, anchor_env + '\n' + setting, 1)
    if mount not in block:
        block = block.replace(anchor_volume, anchor_volume + '\n' + mount, 1)
    return text[:start] + block + text[end:]


def configure(backup_id):
    before = status(backup_id)
    if before['free_mib'] < 2048:
        raise StepError('Less than 2 GiB free on metadata; refusing snapshot setup')
    live = COMPOSE.read_text()
    after = patch_compose(live)
    if before['repository_configured']:
        if after == live:
            print(json.dumps({'result':'STEP76_ES_CONFIGURE_ALREADY_PASS', 'backup_id':backup_id}))
            return
        raise StepError('Elasticsearch has path.repo but Compose is not aligned: manual check required')
    if after == live:
        raise StepError('Compose is already configured but node path.repo absent; inspect runtime manually')
    HOST_REPO.mkdir(mode=0o770, parents=True, exist_ok=True)
    os.chown(HOST_REPO, 0, 0)
    os.chmod(HOST_REPO, 0o770)
    cfg_backup = ROOT / ('compose.yaml.step76d4.' + backup_id + '.bak')
    if cfg_backup.exists():
        raise StepError('Existing original Compose backup found; refusing to overwrite')
    shutil.copy2(COMPOSE, cfg_backup)
    os.chmod(cfg_backup, 0o600)
    temp = COMPOSE.with_suffix('.yaml.step76.tmp')
    temp.write_text(after)
    os.chmod(temp, 0o600)
    temp.replace(COMPOSE)
    try:
        cmd(COMPOSE_ARGS + ['config', '--quiet'], timeout=35)
        # Only ES is recreated, not MySQL, OpenMetadata or Kafka.
        cmd(COMPOSE_ARGS + ['up', '-d', '--no-deps', 'elasticsearch'], timeout=150)
        last_error = ''
        for _ in range(36):
            time.sleep(5)
            try:
                probe = api('GET', '/_cluster/health', timeout=15)
                if probe.get('status') in ('green','yellow') and CONTAINER_REPO in node_repo_paths():
                    print(json.dumps({'result':'STEP76_ES_CONFIGURE_PASS',
                        'backup_id':backup_id, 'cluster_health':probe.get('status'),
                        'compose_backup':str(cfg_backup), 'repository_dir':str(HOST_REPO)}))
                    return
            except (StepError, subprocess.TimeoutExpired) as exc:
                last_error = str(exc)
        raise StepError('Elasticsearch failed to become ready with path.repo: ' + last_error[:200])
    except Exception:
        # Restore previous Compose and attempt to bring the original ES service back.
        shutil.copy2(cfg_backup, COMPOSE)
        os.chmod(COMPOSE, 0o600)
        try:
            cmd(COMPOSE_ARGS + ['up', '-d', '--no-deps', 'elasticsearch'], timeout=150)
        except Exception as ex:
            print('Rollback startup failed, manual intervention required: ' + str(ex)[:300], file=sys.stderr)
        raise


def require_repo():
    if CONTAINER_REPO not in node_repo_paths():
        raise StepError('path.repo not configured. Run configure after authorizing brief Elasticsearch restart')
    repos = api('GET', '/_snapshot')
    if REPOSITORY in repos:
        info = repos[REPOSITORY]
        if info.get('type') != 'fs' or info.get('settings',{}).get('location') != CONTAINER_REPO:
            raise StepError('Existing repository has unexpected settings; refusing overwrite')
        return
    api('PUT', '/_snapshot/' + REPOSITORY + '?verify=true',
        {'type':'fs', 'settings':{'location':CONTAINER_REPO, 'compress':True}})
    api('POST', '/_snapshot/' + REPOSITORY + '/_verify')


def snapshot(backup_id):
    require_repo()
    snap = snapshot_id(backup_id)
    existing = api('GET','/_snapshot/' + REPOSITORY + '/' + snap, timeout=30) if snapshot_exists(snap) else None
    if existing:
        snaps = existing.get('snapshots', [])
        if len(snaps)==1 and snaps[0].get('state')=='SUCCESS':
            print(json.dumps({'result':'STEP76_ES_SNAPSHOT_PASS',
                'snapshot':snap,'existing':True, 'indices':len(snaps[0].get('indices',[]))}))
            return
        raise StepError('Snapshot already exists but is not SUCCESS; refusing to overwrite')
    health = api('GET','/_cluster/health')
    if health.get('status') not in ('green','yellow'):
        raise StepError('ES cluster not ready for snapshot')
    if shutil.disk_usage(HOST_REPO).free < 2 * 1024**3:
        raise StepError('Less than 2 GiB free for snapshot; refusing')
    raw = api('GET','/_cat/indices?format=json&h=index,health')
    indices = sorted({r['index'] for r in raw
        if r.get('index') and not r['index'].startswith(('.', 'step76_verify_'))})
    if not indices or any(r.get('health')=='red' for r in raw if r.get('index') in indices):
        raise StepError('No suitable healthy indices to snapshot')
    reply = api('PUT', '/_snapshot/' + REPOSITORY + '/' + snap + '?wait_for_completion=true',
                {'indices': ','.join(indices), 'ignore_unavailable':False,
                 'include_global_state':False, 'partial':False,
                 'feature_states':[]}, timeout=600)
    state = reply.get('snapshot',{})
    if state.get('state') != 'SUCCESS' or state.get('shards',{}).get('failed', -1) != 0:
        raise StepError('Elasticsearch snapshot did not complete with zero failed shards')
    print(json.dumps({'result':'STEP76_ES_SNAPSHOT_PASS', 'snapshot':snap,
        'indices':len(state.get('indices',[])),'shards_failed':0}))


def snapshot_exists(snap):
    all_snaps = api('GET', '/_snapshot/' + REPOSITORY + '/_all?verbose=false')
    return snap in {s.get('snapshot') for s in all_snaps.get('snapshots',[])}


def read_snapshot(backup_id):
    require_repo()
    snap = snapshot_id(backup_id)
    raw = api('GET', '/_snapshot/' + REPOSITORY + '/' + snap)
    snaps = raw.get('snapshots',[])
    if len(snaps)!=1 or snaps[0].get('state')!='SUCCESS' or snaps[0].get('shards',{}).get('failed',-1)!=0:
        raise StepError('Expected one successful snapshot with zero failed shards')
    return snaps[0]


def restore_sample(backup_id):
    snap = read_snapshot(backup_id)
    available = set(snap.get('indices', []))
    rows = api('GET', '/_cat/indices?format=json&bytes=b&h=index,docs.count,store.size')
    candidates=[]
    for row in rows:
        name=row.get('index','')
        if name not in available or name.startswith('.') or name.startswith('step76_verify_'):
            continue
        try:
            count=int(row.get('docs.count','0'))
            size=int(row.get('store.size','0'))
        except ValueError:
            continue
        if count > 0 and 0 < size <= 8*1024*1024:
            candidates.append((size,name,count))
    if not candidates:
        raise StepError('No populated index <=8MiB; refuse to restore a large index on this VM')
    _,source,_=min(candidates)
    dest='step76_verify_' + backup_id[-8:] + '_' + source
    if len(dest)>240:
        raise StepError('Test index name too long')
    names={r.get('index') for r in rows}
    if dest in names:
        raise StepError('Leftover Step76 test index exists: ' + dest + '; manual cleanup required')
    reply=None
    try:
        reply=api('POST', '/_snapshot/' + REPOSITORY + '/' + snapshot_id(backup_id) + '/_restore?wait_for_completion=true',
            {'indices':source, 'ignore_unavailable':False, 'include_global_state':False,
             'include_aliases':False, 'rename_pattern':'(.+)',
             'rename_replacement':'step76_verify_' + backup_id[-8:] + '_$1'}, timeout=500)
        if reply.get('accepted') is False:
            raise StepError('Elasticsearch restore refused')
        for _ in range(30):
            time.sleep(2)
            lookup=api('GET','/_cat/indices?format=json&h=index,health')
            candidates_now=[r for r in lookup if r.get('index') == dest]
            if candidates_now and candidates_now[0].get('health') in ('green','yellow'):
                count=api('GET', '/' + dest + '/_count').get('count',0)
                if count > 0:
                    evidence = HOST_REPO / ('restore_' + backup_id + '.json')
                    evidence.write_text(json.dumps({'backup_id':backup_id, 'source_index':source,
                        'restored_index':dest, 'restored_doc_count':count,
                        'snapshot':snapshot_id(backup_id)}) + '\n')
                    os.chmod(evidence,0o600)
                    print(json.dumps({'result':'STEP76_ES_RESTORE_SAMPLE_PASS',
                        'index':source,'restored_docs':count, 'cleanup':'pending'}))
                    return
        raise StepError('Restored index did not become readable')
    finally:
        # Delete only the exact scoped test index, never originals or wildcard targets.
        rows = api('GET','/_cat/indices?format=json&h=index')
        if dest in {r.get('index') for r in rows}:
            api('DELETE','/' + dest,timeout=40)
            print('STEP76_ES_TEST_INDEX_CLEANUP_PASS')


def verify(backup_id):
    snap = read_snapshot(backup_id)
    evidence=HOST_REPO / ('restore_' + backup_id + '.json')
    if not evidence.is_file():
        raise StepError('No isolated sample-restore proof. Run restore with explicit approval first')
    stored=json.loads(evidence.read_text())
    if stored.get('backup_id') != backup_id or stored.get('restored_doc_count',0) <= 0:
        raise StepError('Invalid sample restore evidence')
    dest=stored['restored_index']
    raw=api('GET','/_cat/indices?format=json&h=index')
    if dest in {r.get('index') for r in raw}:
        raise StepError('Temporary restored index still exists; cleanup required')
    print(json.dumps({'result':'STEP76_ES_VERIFY_PASS',
        'snapshot':snapshot_id(backup_id), 'indices':len(snap.get('indices',[])),
        'shards_failed':0,'sample_restored_docs':stored['restored_doc_count'],
        'temporary_index_removed':True,
        'note':'Snapshot repository is on metadata VM only. Off-host copy still needed for node loss.'}))


def main():
    if len(sys.argv)!=3 or sys.argv[1] not in ('preflight','configure','snapshot','restore','verify'):
        raise StepError('Usage: script preflight|configure|snapshot|restore|verify BACKUP_ID')
    mode, bid=sys.argv[1:]
    snapshot_id(bid)
    if mode=='preflight':
        print(json.dumps(status(bid), indent=2))
    elif mode=='configure':
        configure(bid)
    elif mode=='snapshot':
        snapshot(bid)
    elif mode=='restore':
        restore_sample(bid)
    elif mode=='verify':
        verify(bid)

if __name__=='__main__':
    try:
        main()
    except (StepError, OSError, subprocess.TimeoutExpired, ValueError) as exc:
        print('STEP76_ES_ERROR: ' + str(exc), file=sys.stderr)
        sys.exit(1)

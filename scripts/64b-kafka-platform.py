#!/usr/bin/env python3
"""Step 64B: read-only Kafka quorum/topic polling; explicit OpenMetadata registration.

No Kafka topic creation/deletion, broker restart, group offset modification or JMX agent.
"""
import argparse
import json
import os
import re
import socket
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib import error, parse, request

KAFKA_BIN = Path('/opt/bigtop-kafka-kraft64/kafka/bin')
JAVA_HOME = Path('/opt/bigtop-kafka-kraft64/jdk17')
BROKERS = [('worker1', '192.158.2.101', 9092), ('worker2', '192.158.2.102', 9092), ('worker3', '192.158.2.103', 9092)]
DEFAULT_BOOTSTRAP = ','.join('%s:%s' % (h,p) for _,h,p in BROKERS)
DEFAULT_METRICS = '/var/lib/prometheus/node-exporter/kafka_platform64b.prom'
DEFAULT_OM = 'http://192.158.2.104:8585/api'
DEFAULT_TOKEN = '/etc/bigtop-governance/pg-catalog/bot.token'
SERVICE = 'bigtop_kafka_kraft64'

class Step64BError(RuntimeError):
    pass

def run_cli(script, bootstrap, *args):
    allowed = {'kafka-metadata-quorum.sh', 'kafka-topics.sh'}
    if script not in allowed:
        raise Step64BError('Unsupported Kafka CLI')
    env = os.environ.copy()
    env['JAVA_HOME'] = str(JAVA_HOME)
    env['KAFKA_HEAP_OPTS'] = '-Xms128m -Xmx256m'
    cmd = [str(KAFKA_BIN/script)]
    cmd += ['--bootstrap-server', bootstrap] + list(args)
    try:
        completed = subprocess.run(cmd, capture_output=True, text=True, timeout=50, env=env, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise Step64BError('%s invocation failed: %s' % (script, exc)) from exc
    if completed.returncode:
        raise Step64BError('%s returned rc=%s: %s' % (script, completed.returncode, completed.stderr[-700:]))
    return completed.stdout

def parse_quorum(stdout):
    match = re.search(r'(?m)^LeaderId:\s*(-?\d+)\s*$', stdout)
    if not match:
        raise Step64BError('Kafka quorum output lacks LeaderId')
    leader = int(match.group(1))
    if leader < 0:
        raise Step64BError('Kafka quorum has no elected leader')
    voter_start = re.search(r'(?m)^CurrentVoters:\s*\[', stdout)
    if not voter_start:
        raise Step64BError('Kafka quorum output lacks CurrentVoters')
    # Kafka 4.x CurrentVoters uses nested arrays (e.g. endpoints: ["..."]).
    begin = stdout.find('[', voter_start.start())
    level = 0
    end = None
    quoted = False
    escape = False
    for pos in range(begin, len(stdout)):
        char = stdout[pos]
        if quoted:
            if escape:
                escape = False
            elif char == chr(92):
                escape = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char == '[':
            level += 1
        elif char == ']':
            level -= 1
            if level == 0:
                end = pos + 1
                break
    if end is None:
        raise Step64BError('Incomplete CurrentVoters JSON')
    payload = stdout[begin:end]
    try:
        voters_raw = json.loads(payload)
    except ValueError as exc:
        raise Step64BError('Cannot parse CurrentVoters') from exc
    voters = [v['id'] if isinstance(v, dict) else v for v in voters_raw]
    if len(set(voters)) != 3 or set(voters) != {1, 2, 3}:
        raise Step64BError('Unexpected KRaft voter IDs: %s' % voters)
    return {'leader': leader, 'voters': voters}

def parse_topic_describe(stdout):
    """Parse Kafka 4.x --describe output; count only partition detail lines."""
    topics = {}
    for line in stdout.splitlines():
        # Header lines carry PartitionCount / ReplicationFactor, partition lines carry Isr.
        name = re.search(r'\bTopic:\s*([^\s]+)', line)
        if not name:
            continue
        topic = name.group(1)
        if not re.fullmatch(r'[A-Za-z0-9._-]+', topic):
            raise Step64BError('Unexpected Kafka topic name')
        topic_data = topics.setdefault(topic, {'partitions': {}, 'rf': None})
        header = re.search(r'\bReplicationFactor:\s*(\d+)', line)
        if header:
            topic_data['rf'] = int(header.group(1))
        part = re.search(r'\bPartition:\s*(\d+)', line)
        if not part:
            continue
        leader = re.search(r'\bLeader:\s*(-?\d+|none)', line, re.IGNORECASE)
        replicas = re.search(r'\bReplicas:\s*([0-9][0-9, ]*)', line, re.IGNORECASE)
        isr = re.search(r'\bIsr:\s*([0-9, ]*)', line, re.IGNORECASE)
        if not (leader and replicas and isr):
            raise Step64BError('Incomplete Kafka partition output: %s' % line[:220])
        def nums(value):
            return {int(n) for n in re.findall(r'\d+', value)}
        partition_id = int(part.group(1))
        topic_data['partitions'][partition_id] = {
            'leader': -1 if leader.group(1).lower() == 'none' else int(leader.group(1)),
            'replicas': nums(replicas.group(1)),
            'isr': nums(isr.group(1))
        }
    if not topics:
        # Empty cluster with no topics is valid, but nonempty unparsed output isn't.
        if stdout.strip():
            raise Step64BError('Kafka topic describe returned unparsed output')
        return {}
    for name, detail in topics.items():
        if detail['rf'] is None:
            # Some CLI output might omit the header; a partition must carry replica count.
            if detail['partitions']:
                detail['rf'] = len(next(iter(detail['partitions'].values()))['replicas'])
            else:
                raise Step64BError('Topic header without RF: %s' % name)
    return topics

def tcp_up(host, port):
    try:
        with socket.create_connection((host, port), timeout=2):
            return 1
    except OSError:
        return 0

def inspect(bootstrap):
    quorum = parse_quorum(run_cli('kafka-metadata-quorum.sh', bootstrap, 'describe', '--status'))
    topics = parse_topic_describe(run_cli('kafka-topics.sh', bootstrap, '--describe'))
    reachability = {name: tcp_up(host, port) for name,host,port in BROKERS}
    part_count = sum(len(t['partitions']) for t in topics.values())
    under = sum(len(p['isr']) < len(p['replicas']) for t in topics.values() for p in t['partitions'].values())
    offline = sum(p['leader'] < 0 for t in topics.values() for p in t['partitions'].values())
    return {'quorum': quorum, 'topics': topics, 'broker_tcp': reachability,
            'topic_count': len(topics), 'partition_count': part_count,
            'under_replicated': under, 'offline': offline}

def prometheus_content(state, when):
    lines = [
        '# HELP bigtop_kafka64_collector_success Last CLI collection success (1=pass).',
        '# TYPE bigtop_kafka64_collector_success gauge',
        'bigtop_kafka64_collector_success %d' % (1 if state else 0),
        '# HELP bigtop_kafka64_last_attempt_unixtime UNIX time of most recent collection attempt.',
        '# TYPE bigtop_kafka64_last_attempt_unixtime gauge',
        'bigtop_kafka64_last_attempt_unixtime %d' % int(when),
    ]
    if state:
        for key,val in [
            ('quorum_leader_present', 1), ('quorum_voters', len(state['quorum']['voters'])),
            ('topics', state['topic_count']), ('partitions', state['partition_count']),
            ('under_replicated_partitions', state['under_replicated']),
            ('offline_partitions', state['offline'])]:
            metric='bigtop_kafka64_'+key
            lines += ['# TYPE '+metric+' gauge', metric+' '+str(val)]
        lines += ['# TYPE bigtop_kafka64_broker_tcp_up gauge']
        for node, value in state['broker_tcp'].items():
            lines.append('bigtop_kafka64_broker_tcp_up{node="%s"} %d' % (node, value))
    return '\n'.join(lines)+'\n'

def atomic_write(path, content):
    dest = Path(path)
    if not dest.parent.is_dir():
        raise Step64BError('Existing Node Exporter textfile directory missing: %s' % dest.parent)
    # Never create a different monitoring tree or follow symlinked final files.
    if dest.is_symlink():
        raise Step64BError('Refusing symlinked metrics path')
    fd, tmp = tempfile.mkstemp(prefix='.kafka64b-', dir=str(dest.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, dest)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)

def http(method, path, payload=None, token=None, base=DEFAULT_OM):
    headers = {'Accept':'application/json'}
    if token:
        headers['Authorization'] = 'Bearer '+token
    data = None
    if payload is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(payload).encode('utf-8')
    url = base.rstrip('/') + '/v1/' + path.lstrip('/')
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=18) as r:
            body = r.read(5_000_000)
            return r.status, json.loads(body) if body else {}
    except error.HTTPError as exc:
        if exc.code == 404:
            return 404, None
        # Do not log response body: could include auth/internal details.
        raise Step64BError('OpenMetadata %s request returned HTTP %d on %s' % (method, exc.code, path)) from exc
    except (OSError, ValueError) as exc:
        raise Step64BError('OpenMetadata %s request failed on %s: %s' % (method, path, type(exc).__name__)) from exc

def read_token(path):
    p=Path(path)
    metadata=p.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise Step64BError('OpenMetadata token must be a regular 0600 file')
    token=p.read_text(encoding='utf-8').strip()
    if not token or any(c.isspace() for c in token):
        raise Step64BError('Invalid token file')
    return token

def sync_openmetadata(state, token, base, service=SERVICE):
    path='services/messagingServices/name/'+parse.quote(service, safe='')
    status, existing=http('GET',path,token=token,base=base)
    if status == 404:
        payload={'name':service,'displayName':'Bigtop Kafka KRaft 64A',
                 'description':'Internal 3-node Kafka KRaft (Step64A), observed by Step64B. Topic ingestion and consumer lineage are not configured.',
                 'serviceType':'Kafka',
                 'connection':{'config':{'type':'Kafka','bootstrapServers':DEFAULT_BOOTSTRAP,'securityProtocol':'PLAINTEXT'}}}
        status, existing=http('POST','services/messagingServices',payload,token=token,base=base)
        if status not in (200,201):
            raise Step64BError('Kafka Messaging Service create unexpected status')
    else:
        if existing.get('serviceType') != 'Kafka':
            raise Step64BError('Messaging service name already exists with different type')
        # Preserve existing service configuration and avoid mutating credentials.
    registered=0
    for name, meta in sorted(state['topics'].items()):
        if name.startswith('__'):
            continue
        if len(name)>255:
            raise Step64BError('Unexpected topic name length')
        topic_path='topics/name/'+parse.quote(service+'.'+name,safe='')
        topic_status, present=http('GET',topic_path,token=token,base=base)
        if topic_status == 404:
            body={'name':name,'service':service,'partitions':len(meta['partitions']),
                  'replicationFactor':meta['rf'],
                  'description':'Registered from live Kafka CLI by Step64B. No message schema or processing lineage inferred.'}
            result_status,_=http('POST','topics',body,token=token,base=base)
            if result_status not in (200,201):
                raise Step64BError('OpenMetadata topic create unexpected response: '+name)
        else:
            if present.get('name') != name:
                raise Step64BError('OpenMetadata topic name mismatch: '+name)
        registered+=1
    return registered

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['plan','metrics','metadata'],default='plan')
    p.add_argument('--bootstrap',default=DEFAULT_BOOTSTRAP)
    p.add_argument('--metrics-path',default=DEFAULT_METRICS)
    p.add_argument('--om-url',default=DEFAULT_OM)
    p.add_argument('--token-file',default=DEFAULT_TOKEN)
    a=p.parse_args(argv)
    if a.bootstrap != DEFAULT_BOOTSTRAP:
        raise Step64BError('Bootstrap must match isolated 64A cluster; no other Kafka clusters')
    stamp=int(time.time())
    try:
        state=inspect(a.bootstrap)
        if a.mode == 'plan':
            print('STEP64B_READ_ONLY_PLAN_PASS: quorum=%d voters=%d topics=%d partitions=%d under_replicated=%d offline=%d' %
                  (state['quorum']['leader'],len(state['quorum']['voters']),state['topic_count'],state['partition_count'],state['under_replicated'],state['offline']))
        elif a.mode == 'metrics':
            atomic_write(a.metrics_path, prometheus_content(state,stamp))
            print('STEP64B_KAFKA_METRICS_PASS: quorum=%d brokers_tcp=%s topics=%d partitions=%d under_replicated=%d offline=%d' %
                  (state['quorum']['leader'],state['broker_tcp'],state['topic_count'],state['partition_count'],state['under_replicated'],state['offline']))
        elif a.mode == 'metadata':
            token=read_token(a.token_file)
            number=sync_openmetadata(state,token,a.om_url)
            print('STEP64B_OPENMETADATA_PASS: service=%s topics=%d (non-internal only)' % (SERVICE,number))
    except Exception as exc:
        if a.mode == 'metrics':
            try:
                atomic_write(a.metrics_path,prometheus_content(None,stamp))
            except Exception:
                pass
        print('STEP64B_FAIL: %s' % exc,file=sys.stderr)
        return 1
    return 0

if __name__=='__main__':
    sys.exit(main())

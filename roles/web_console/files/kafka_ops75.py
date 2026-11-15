"""Step75: read-only Kafka broker metadata, KRaft quorum and partition ISR health.

No mutating Kafka operations. Routes inherit the existing Web Console auth boundary.
All command arguments are constant; subprocess never uses a shell.
"""
import os
from pathlib import Path
import re
import subprocess
import time

from fastapi import APIRouter

router = APIRouter(tags=['kafka-operations-health'])
KAFKA_BIN = Path(os.environ.get('WEB_CONSOLE_KAFKA_BIN', '/opt/bigtop-kafka-kraft64/kafka/bin'))
JAVA_HOME = os.environ.get('WEB_CONSOLE_KAFKA_JAVA_HOME', '/opt/bigtop-kafka-kraft64/jdk17')
BROKERS = os.environ.get(
    'WEB_CONSOLE_KAFKA_BROKERS',
    '192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092',
)
MAX_PARTITIONS = 200
MAX_CAPTURE = 2 * 1024 * 1024


def _read_only_cli(program, args, timeout=27):
    # Only code-owned, fixed executable names and read-only CLI switches.
    allowed = {
        'kafka-topics.sh': ('--describe',),
        'kafka-metadata-quorum.sh': ('describe', '--status'),
        'kafka-broker-api-versions.sh': (),
    }
    if program not in allowed or tuple(args) != allowed[program]:
        raise ValueError('Unsupported read-only Kafka command')
    path = KAFKA_BIN / program
    if not path.is_file():
        return None, f'{program}: missing Kafka CLI'
    env = os.environ.copy()
    if JAVA_HOME:
        env['JAVA_HOME'] = JAVA_HOME
    try:
        # Kafka broker tools share the --bootstrap-server option, but its
        # position differs for the metadata quorum CLI.
        argv = [str(path), '--bootstrap-server', BROKERS, *args]
        proc = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout,
            env=env, check=False,
        )
    except subprocess.TimeoutExpired:
        return None, f'{program}: timeout'
    except OSError as exc:
        return None, f'{program}: unavailable ({type(exc).__name__})'
    if proc.returncode:
        return None, f'{program}: command failed (exit {proc.returncode})'
    if len(proc.stdout) > MAX_CAPTURE:
        return None, f'{program}: result too large'
    return proc.stdout, None


def _parse_topic_describe(output):
    """Parse Kafka topic --describe partition rows (not topic-level configs)."""
    rows = []
    for line in output.splitlines():
        # Kafka output uses tabs/spaces, and may append Elr/LastKnownElr.
        topic = re.search(r'\bTopic:\s*(\S+)', line)
        part = re.search(r'\bPartition:\s*(\d+)', line)
        leader = re.search(r'\bLeader:\s*(-?\d+|none)', line, re.I)
        replicas = re.search(r'\bReplicas:\s*([\d,]*)', line)
        isr = re.search(r'\bIsr:\s*([\d,]*)', line, re.I)
        if not all((topic, part, leader, replicas, isr)):
            continue
        replica_ids = [int(x) for x in replicas.group(1).split(',') if x.isdecimal()]
        isr_ids = [int(x) for x in isr.group(1).split(',') if x.isdecimal()]
        l_text = leader.group(1)
        leader_id = int(l_text) if l_text.lstrip('-').isdecimal() else None
        rows.append({
            'topic': topic.group(1), 'partition': int(part.group(1)),
            'leader': leader_id, 'replicas': replica_ids, 'isr': isr_ids,
            'under_replicated': len(isr_ids) < len(replica_ids),
            'leader_missing': leader_id is None or leader_id < 0,
        })
    anomalies = [x for x in rows if x['under_replicated'] or x['leader_missing']]
    return {
        'partition_count': len(rows),
        'under_replicated_count': sum(x['under_replicated'] for x in rows),
        'leader_missing_count': sum(x['leader_missing'] for x in rows),
        'anomaly_count': len(anomalies),
        'anomalies': anomalies[:MAX_PARTITIONS],
        'anomalies_truncated': len(anomalies) > MAX_PARTITIONS,
        'parse_complete': bool(rows),
    }


def _parse_quorum_status(output):
    fields = {}
    for line in output.splitlines():
        if ':' in line:
            k, v = line.split(':', 1)
            fields[k.strip()] = v.strip()
    voters = fields.get('CurrentVoters', '')
    # Kafka <3.9: [1, 2, 3]; Kafka newer: [{id=1, directoryId=...}, ...]
    ids = re.findall(r'\bid\s*=\s*(\d+)', voters)
    if not ids:
        ids = re.findall(r'\b\d+\b', voters)
    leader_txt = fields.get('LeaderId', '')
    leader = int(leader_txt) if leader_txt.lstrip('-').isdecimal() else None
    valid = bool(leader is not None and leader >= 0 and ids)
    return {
        'cluster_id': fields.get('ClusterId'),
        'leader_id': leader,
        'leader_epoch': fields.get('LeaderEpoch'),
        'current_voters': [int(x) for x in ids],
        'voter_count': len(ids),
        'high_watermark': fields.get('HighWatermark'),
        'max_follower_lag': fields.get('MaxFollowerLag'),
        'parse_complete': valid,
    }


def _parse_brokers(output):
    # kafka-broker-api-versions.sh prints one heading per reachable broker.
    found = {}
    for line in output.splitlines():
        match = re.match(r'^\s*(\S+)\s+\(id:\s*(-?\d+)\s+rack:\s*[^)]*\)\s*->', line)
        if match:
            found[match.group(2)] = {'id': int(match.group(2)), 'endpoint': match.group(1)}
    return {'brokers': sorted(found.values(), key=lambda x: x['id']),
            'broker_count': len(found), 'parse_complete': bool(found)}


@router.get('/api/ops/kafka/cluster-health')
def kafka_cluster_health():
    errors = []
    topics, error = _read_only_cli('kafka-topics.sh', ['--describe'])
    if error:
        errors.append(error)
    quorum, error = _read_only_cli('kafka-metadata-quorum.sh', ['describe', '--status'])
    if error:
        errors.append(error)
    brokers, error = _read_only_cli('kafka-broker-api-versions.sh', [])
    if error:
        errors.append(error)
    partition_health = _parse_topic_describe(topics) if topics is not None else None
    quorum_health = _parse_quorum_status(quorum) if quorum is not None else None
    broker_health = _parse_brokers(brokers) if brokers is not None else None
    if partition_health and not partition_health['parse_complete']:
        errors.append('kafka-topics.sh: no partition metadata parsed')
    if quorum_health and not quorum_health['parse_complete']:
        errors.append('kafka-metadata-quorum.sh: incomplete quorum status')
    if broker_health and not broker_health['parse_complete']:
        errors.append('kafka-broker-api-versions.sh: no broker metadata parsed')
    return {
        'ok': not errors,
        'read_only': True,
        'checked_at': int(time.time()),
        'broker_metadata': broker_health,
        'quorum': quorum_health,
        'partition_health': partition_health,
        'errors': errors,
        'notes': [
            'Kafka CLI metadata snapshot, not a continuous health monitor.',
            'Quorum voters are controller IDs; broker IDs may be separate.',
            'Consumer-group lag remains on the Step70 selected-group endpoint.',
        ],
    }

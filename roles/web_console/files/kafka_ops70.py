"""Step70 read-only Kafka overview, topics, groups, and Step65 observability.

No Kafka produce/delete/offset reset calls; only TCP probes, read-only CLI,
and a fixed Prometheus query. This router inherits Web Console Nginx auth.
"""
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import APIRouter, HTTPException

router = APIRouter(tags=['kafka-operations'])
KAFKA_BIN = os.environ.get('WEB_CONSOLE_KAFKA_BIN', '/opt/bigtop-kafka-kraft64/kafka/bin')
JAVA_HOME = os.environ.get('WEB_CONSOLE_KAFKA_JAVA_HOME', '/opt/bigtop-kafka-kraft64/jdk17')
BROKERS = os.environ.get('WEB_CONSOLE_KAFKA_BROKERS', '192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092')
PROM_URL = os.environ.get('WEB_CONSOLE_PROMETHEUS_URL', 'http://127.0.0.1:9090').rstrip('/')
CDC_TOPIC = 'bigtop_olist_step65_cdc_v1'
MAX_ITEMS = 200
MAX_OUTPUT = 256 * 1024


def _broker_addresses():
    addresses = []
    for entry in BROKERS.split(','):
        raw = entry.strip()
        if not raw or ':' not in raw:
            continue
        host, port = raw.rsplit(':', 1)
        if not host or not port.isdecimal() or not 1 <= int(port) <= 65535:
            continue
        addresses.append((host, int(port)))
    return addresses[:15]


def _broker_tcp():
    result = []
    for host, port in _broker_addresses():
        connected = False
        try:
            with socket.create_connection((host, port), timeout=0.8):
                connected = True
        except OSError:
            pass
        result.append({'address': f'{host}:{port}', 'tcp_reachable': connected})
    return result


def _command(executable, args):
    path = Path(KAFKA_BIN) / executable
    if not path.is_file():
        raise HTTPException(status_code=503, detail=f'Kafka CLI not installed: {executable}')
    env = os.environ.copy()
    if JAVA_HOME:
        env['JAVA_HOME'] = JAVA_HOME
    try:
        result = subprocess.run(
            [str(path), '--bootstrap-server', BROKERS, *args],
            capture_output=True, text=True, timeout=18,
            check=False, env=env,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail=f'Kafka CLI timed out: {executable}')
    except OSError as exc:
        raise HTTPException(status_code=503, detail=f'Kafka CLI unavailable: {type(exc).__name__}')
    if result.returncode != 0:
        # Avoid accidentally returning complete logs (credentials, configs).
        raise HTTPException(status_code=503, detail=f'Kafka CLI failed: {executable} (exit {result.returncode})')
    if len(result.stdout) > MAX_OUTPUT:
        raise HTTPException(status_code=503, detail='Kafka CLI output exceeds safe limit')
    return result.stdout


def _prometheus_samples():
    expr = ('{__name__=~"bigtop_olist_step68_offset_lag|'
            'bigtop_olist_step67_collector_success|'
            'bigtop_olist_step67_last_run_timestamp_seconds|'
            'bigtop_olist_step67_kafka_end_offset|'
            'bigtop_olist_step67_spark_committed_offset"}')
    request = Request(
        PROM_URL + '/api/v1/query?' + urlencode({'query': expr}),
        headers={'Accept': 'application/json'},
    )
    try:
        with urlopen(request, timeout=4) as response:
            data = json.load(response)
        if data.get('status') != 'success':
            raise ValueError('Prometheus returned failure')
        return data.get('data', {}).get('result', []), None
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return [], f'Prometheus unavailable ({type(exc).__name__})'


def _metrics_view(samples):
    observed = {'kafka_end_offset': {}, 'spark_committed_offset': {}, 'offset_lag': {}}
    collectors = {}
    collector_times = {}
    mapping = {
        'bigtop_olist_step68_offset_lag': 'offset_lag',
        'bigtop_olist_step67_kafka_end_offset': 'kafka_end_offset',
        'bigtop_olist_step67_spark_committed_offset': 'spark_committed_offset',
    }
    for item in samples:
        label = item.get('metric') or {}
        name = label.get('__name__', '')
        raw = item.get('value', [None, None])
        try:
            number = float(raw[1])
            if not math.isfinite(number):
                continue
        except (TypeError, IndexError, ValueError):
            continue
        if name in mapping and label.get('topic') == CDC_TOPIC:
            part = label.get('partition')
            if part is not None:
                observed[mapping[name]][str(part)] = number
        if name == 'bigtop_olist_step67_collector_success':
            if label.get('source') in ('kafka', 'spark'):
                collectors[label['source']] = bool(number == 1)
        if name == 'bigtop_olist_step67_last_run_timestamp_seconds':
            if label.get('source') in ('kafka', 'spark'):
                collector_times[label['source']] = number
    partitions = sorted(set().union(*(set(x.keys()) for x in observed.values())),
                        key=lambda x: (not x.isdecimal(), int(x) if x.isdecimal() else x))
    rows = []
    for p in partitions:
        row = {'partition': p}
        for key in observed:
            row[key] = observed[key].get(p)
        rows.append(row)
    ages = {key: max(0, int(time.time() - value)) for key, value in collector_times.items()}
    return {'topic': CDC_TOPIC, 'partitions': rows,
            'collectors': collectors, 'collector_age_seconds': ages,
            'lag_total': sum(x['offset_lag'] for x in rows)
                         if rows and all(x['offset_lag'] is not None for x in rows) else None}


@router.get('/api/ops/kafka/overview')
def kafka_overview():
    samples, error = _prometheus_samples()
    return {'ok': True, 'read_only': True, 'checked_at': int(time.time()),
            'brokers': _broker_tcp(), 'broker_probe': 'TCP reachability only (not Kafka health)',
            'streaming': _metrics_view(samples),
            'prometheus_error': error}


def _list_names(tool):
    lines = [line.strip() for line in _command(tool, ['--list']).splitlines() if line.strip()]
    # Topic/group names are data, not command arguments until explicitly selected.
    return {'ok': True, 'read_only': True, 'total': len(lines),
            'truncated': len(lines) > MAX_ITEMS, 'items': lines[:MAX_ITEMS]}


@router.get('/api/ops/kafka/topics')
def kafka_topics():
    return _list_names('kafka-topics.sh')


@router.get('/api/ops/kafka/groups')
def kafka_groups():
    return _list_names('kafka-consumer-groups.sh')


@router.get('/api/ops/kafka/groups/{group_name}/lag')
def kafka_group_lag(group_name: str):
    if not 1 <= len(group_name) <= 128 or not re.fullmatch(r'[a-zA-Z0-9._-]+', group_name):
        raise HTTPException(status_code=400, detail='Invalid Kafka consumer group name')
    output = _command('kafka-consumer-groups.sh', ['--describe', '--group', group_name])
    entries = []
    for line in output.splitlines():
        fields = line.split()
        # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
        if len(fields) < 6 or fields[0] != group_name or not fields[2].isdecimal():
            continue
        def num(x):
            return int(x) if x.isdecimal() else None
        entries.append({'topic': fields[1], 'partition': int(fields[2]),
                        'current_offset': num(fields[3]), 'log_end_offset': num(fields[4]),
                        'lag': num(fields[5])})
    total_lag = sum(row['lag'] for row in entries) if entries and all(row['lag'] is not None for row in entries) else None
    return {'ok': True, 'read_only': True, 'group': group_name,
            'partitions': entries[:MAX_ITEMS], 'truncated': len(entries) > MAX_ITEMS,
            'total_lag': total_lag}

#!/usr/bin/env python3
"""Step65 lightweight Kafka publisher and Trino/HDFS verifier; NO Spark submission.

publisher/reconcile/replay: worker3, seed/proof envelope over stdin.
verify/store-proof: master as hadoop user with HDFS client and Trino read access.
Protects Step64 and Step65 snapshots, existing CDC tables and checkpoint.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import olist_kafka65 as core

KAFKA = '/opt/bigtop-kafka-kraft64/kafka/bin'
JAVA = '/opt/bigtop-kafka-kraft64/jdk17'
TRINO = 'http://192.158.2.104:8080'
SOURCE = 'iceberg_rest.olist64_kafka.fact_cdc_stream_v1'
TARGET = 'iceberg_rest.olist65_stream.fact_cdc_running_v1'
TOPIC = core.TOPIC
PROOF_SCHEMA = 'bigtop.olist.kafka-stream-produce.v1'
APPLY_SCHEMA = 'bigtop.olist.kafka-stream-apply.v1'


class LightError(RuntimeError):
    pass


def require(ok, message):
    if not ok:
        raise LightError(message)


def trino_request(url, sql=None):
    headers = {'X-Trino-User': 'step65_light', 'Accept': 'application/json'}
    if sql is not None:
        headers['Content-Type'] = 'text/plain; charset=utf-8'
    with urlopen(Request(url, data=sql.encode('utf-8') if sql else None,
                         headers=headers, method='POST' if sql else 'GET'),
                 timeout=30) as response:
        return json.load(response)


def sql(base, statement):
    result = trino_request(base.rstrip('/') + '/v1/statement', statement)
    values = []
    deadline = time.monotonic() + 180
    while True:
        require(not result.get('error'), 'Trino query failed: ' +
                str(result.get('error', {}).get('message')))
        values += result.get('data', [])
        next_uri = result.get('nextUri')
        if not next_uri:
            return values
        require(urlsplit(next_uri).netloc == urlsplit(base).netloc and
                urlsplit(next_uri).scheme == urlsplit(base).scheme,
                'Unexpected Trino nextUri')
        require(time.monotonic() < deadline, 'Trino query exceeded 180 seconds')
        time.sleep(0.1)
        result = trino_request(next_uri)


def trino_snapshot(base, table):
    catalog, schema, name = table.split('.')
    found = sql(base, f'SELECT snapshot_id FROM {catalog}.{schema}."{name}$refs" WHERE name = \'main\'')
    require(len(found) == 1, 'Missing or ambiguous Iceberg main snapshot: ' + table)
    return str(found[0][0])


def trino_rows(base, table):
    return sql(base, 'SELECT order_id, order_item_id, price, freight_value, item_total, '
               f'last_event_seq, is_deleted FROM {table} ORDER BY order_id, order_item_id')


def validate_trino(base):
    state = trino_request(base.rstrip('/') + '/v1/info')
    require(state.get('state') == 'ACTIVE' and
            state.get('nodeVersion', {}).get('version') == '484',
            'Expected ACTIVE Trino 484')


def check_source(base, seed):
    require(seed.get('status') == 'PASS' and
            seed.get('schema') == 'bigtop.olist.kafka-stream-seed.v1' and
            seed.get('table') == core.TARGET and seed.get('rows') == 520,
            'Invalid immutable Step65 seed proof')
    require(trino_snapshot(base, SOURCE) == str(seed['source_snapshot']),
            'Protected Step64 source snapshot drift')
    source = trino_rows(base, SOURCE)
    require(len(source) == 520 and core.digest(source) == seed['source_digest'],
            'Protected Step64 source row digest drift')
    return source


def kafka_run(args, *, stdin=None, timeout=45):
    env = os.environ.copy()
    env['JAVA_HOME'] = JAVA
    env['PATH'] = JAVA + '/bin:' + env.get('PATH', '')
    env['KAFKA_HEAP_OPTS'] = '-Xms128m -Xmx256m'
    require(Path(JAVA + '/bin/java').is_file(), 'Kafka dedicated Java17 missing')
    p = subprocess.run([KAFKA + '/' + args[0] + '.sh', *args[1:]], input=stdin,
                       capture_output=True, text=True, env=env, timeout=timeout)
    require(p.returncode == 0, f'Kafka CLI {args[0]} failed: {p.stderr[-700:]}')
    return p.stdout


def kafka_offset(bootstrap, when):
    output = kafka_run(['kafka-get-offsets', '--bootstrap-server', bootstrap,
                        '--topic', TOPIC, '--time', str(when)])
    lines = [s.strip() for s in output.splitlines() if s.strip()]
    valid = [int(m.group(1)) for line in lines
             if (m := re.fullmatch(re.escape(TOPIC) + r':0:(\d+)', line))]
    require(len(valid) == 1 and len(lines) == 1,
            'Expected exactly one Kafka partition and unambiguous offsets: ' + output)
    return valid[0]


def produce_lines(source):
    events = core.events_for(source)
    return ''.join(f"{e['order_id']}:{e['order_item_id']}|" +
                   json.dumps(e, sort_keys=True, separators=(',', ':')) + '\n'
                   for e in events)


def check_legacy_yarn_job():
    p = subprocess.run(['yarn', 'application', '-list', '-appStates', 'RUNNING,ACCEPTED'],
                       capture_output=True, text=True, timeout=45)
    require(p.returncode == 0, 'Cannot examine existing YARN application state')
    for line in p.stdout.splitlines():
        columns = line.split()
        if len(columns) >= 6 and re.fullmatch(r'application_\d+_\d+', columns[0]):
            if columns[1] in ('olist-kafka65-produce', 'olist-kafka65-replay', 'olist-kafka65-verify') and \
                    columns[3] == 'hadoop' and columns[5] in ('RUNNING', 'ACCEPTED'):
                raise LightError('Legacy Spark Kafka producer still active: ' +
                                 columns[0] + ' (' + columns[5] +
                                 '). Do not publish while it may start later.')


def proof_valid(proof, seed):
    return (isinstance(proof, dict) and proof.get('status') == 'PASS' and
            proof.get('schema') == PROOF_SCHEMA and proof.get('topic') == TOPIC and
            proof.get('messages') == 12 and proof.get('run_id') == core.RUN_ID and
            proof.get('source_snapshot') == seed['source_snapshot'])


def publish_proof(seed):
    return {'status': 'PASS', 'schema': PROOF_SCHEMA,
            'topic': TOPIC, 'messages': 12, 'run_id': core.RUN_ID,
            'source_snapshot': seed['source_snapshot'],
            'time_utc': datetime.now(timezone.utc).isoformat()}


def reconcile(bootstrap, source, seed):
    earliest = kafka_offset(bootstrap, -2)
    latest = kafka_offset(bootstrap, -1)
    require(earliest == 0 and latest == 12,
            f'Reconcile requires exactly 12 retained events at 0..11; earliest={earliest} latest={latest}')
    output = kafka_run(['kafka-console-consumer', '--bootstrap-server', bootstrap,
                        '--topic', TOPIC, '--partition', '0', '--offset', '0',
                        '--max-messages', '12', '--timeout-ms', '15000'], timeout=30)
    expected = Counter(json.dumps(e, sort_keys=True, separators=(',', ':'))
                       for e in core.events_for(source))
    actual = Counter(json.dumps(json.loads(x), sort_keys=True, separators=(',', ':'))
                     for x in output.splitlines() if x.strip())
    require(actual == expected, 'Existing Kafka events do not match Step65 fixture; refuse proof')
    print('STEP65_LIGHT_RECONCILE_PASS: existing 12 events match', file=sys.stderr)
    return publish_proof(seed)


def publisher(mode, bootstrap, trino, envelope):
    require(isinstance(envelope, dict), 'Missing Step65 proof envelope')
    seed, previous, applied = (envelope.get(n) for n in ('seed', 'produce', 'apply'))
    validate_trino(trino)
    source = check_source(trino, seed)
    if mode == 'replay':
        require(proof_valid(previous, seed) and applied and
                applied.get('schema') == APPLY_SCHEMA and applied.get('status') == 'PASS',
                'Replay requires first produce and verified apply evidence')
        require(trino_snapshot(trino, TARGET) == str(applied['main_snapshot']) and
                core.digest(trino_rows(trino, TARGET)) == applied['digest_sha256'],
                'Replay refused: protected Step65 post-image drift')
        before = kafka_offset(bootstrap, -1)
        require(kafka_offset(bootstrap, -2) == 0 and before == 12,
                f'Replay permitted only once from initial 12 offsets; current end={before}')
        kafka_run(['kafka-console-producer', '--bootstrap-server', bootstrap,
                   '--topic', TOPIC, '--reader-property', 'parse.key=true',
                   '--reader-property', 'key.separator=|',
                   '--command-property', 'acks=all',
                   '--command-property', 'enable.idempotence=true'],
                  stdin=produce_lines(source), timeout=60)
        after = kafka_offset(bootstrap, -1)
        require(after == before + 12, 'Replay offset count mismatch (partial publish?)')
        print('STEP65_LIGHT_REPLAY_PASS: 12 additional events', file=sys.stderr)
        return {'status': 'PASS', 'mode': 'replay', 'before': before, 'after': after}
    if previous is not None:
        require(proof_valid(previous, seed), 'Existing producer proof invalid')
        print('STEP65_LIGHT_PRODUCE_ALREADY_VERIFIED: skipped', file=sys.stderr)
        return previous
    require(applied is None, 'Apply proof exists without producer proof; refuse to publish')
    earliest, latest = kafka_offset(bootstrap, -2), kafka_offset(bootstrap, -1)
    if latest == 12 and earliest == 0:
        return reconcile(bootstrap, source, seed)
    require(mode == 'produce', 'Reconcile mode requires 12 retained Kafka messages')
    require(earliest == 0 and latest == 0,
            f'Kafka topic is not empty (earliest={earliest} latest={latest}); refuse duplicate publish')
    require(trino_snapshot(trino, TARGET) == str(seed['main_snapshot']),
            'Step65 target already changed but producer proof missing')
    kafka_run(['kafka-console-producer', '--bootstrap-server', bootstrap,
               '--topic', TOPIC, '--reader-property', 'parse.key=true',
               '--reader-property', 'key.separator=|',
               '--command-property', 'acks=all',
               '--command-property', 'enable.idempotence=true'],
              stdin=produce_lines(source), timeout=60)
    require(kafka_offset(bootstrap, -1) == 12,
            'Kafka publish returned but not all 12 offsets visible; reconcile before retry')
    print('STEP65_LIGHT_PRODUCE_PASS: messages=12 without YARN', file=sys.stderr)
    return publish_proof(seed)


def parse_offset_line(line):
    match = re.fullmatch(re.escape(TOPIC) + r':0:(\d+)', line.strip())
    require(match is not None, 'Invalid or ambiguous Kafka latest offset: ' + line)
    return int(match.group(1))


def checkpoint_end():
    listing = core.hdfs('-ls', core.CHECKPOINT + '/commits').decode('utf-8')
    commits = []
    for line in listing.splitlines():
        name = line.split()[-1].rsplit('/', 1)[-1] if line.split() else ''
        if name.isdecimal():
            commits.append(int(name))
    require(commits, 'No committed Step65 Spark microbatch')
    batch_id = max(commits)
    content = core.hdfs('-cat', core.CHECKPOINT + '/offsets/' + str(batch_id)).decode('utf-8')
    lines = content.strip().splitlines()
    require(len(lines) >= 2, 'Bad Spark offset log format')
    payload = json.loads(lines[-1])
    require(TOPIC in payload and '0' in payload[TOPIC], 'Checkpoint topic partition mismatch')
    return int(payload[TOPIC]['0'])


def verifier(trino, offset_line):
    validate_trino(trino)
    seed = core.read_proof(core.SEED_PROOF)
    producer = core.read_proof(core.PRODUCE_PROOF)
    source = check_source(trino, seed)
    require(proof_valid(producer, seed), 'Producer evidence invalid')
    expected = core.expected_after(source)
    target_rows = trino_rows(trino, TARGET)
    require(len(target_rows) == 521 and core.digest(target_rows) == core.digest(expected),
            'Iceberg target does not match CDC post-image (may still be processing)')
    require(sum(bool(r[6]) for r in target_rows) == 9 and
            sum(not bool(r[6]) for r in target_rows) == 512,
            'Iceberg tombstone/live totals wrong')
    latest = parse_offset_line(offset_line)
    require(latest >= 12, 'Kafka initial 12 events not present')
    committed = checkpoint_end()
    require(committed >= latest,
            f'Stream has not committed Kafka end: checkpoint={committed} kafka={latest}')
    target_snapshot = trino_snapshot(trino, TARGET)
    digest = core.digest(expected)
    if core.exists(core.APPLY_PROOF):
        applied = core.read_proof(core.APPLY_PROOF)
        require(applied.get('schema') == APPLY_SCHEMA and
                applied.get('table') == core.TARGET and
                applied.get('digest_sha256') == digest and
                str(applied.get('main_snapshot')) == target_snapshot and
                applied.get('checkpoint') == core.CHECKPOINT,
                'Replay unexpectedly changed target snapshot or existing evidence')
    else:
        core.write_proof(core.APPLY_PROOF, {
            'status': 'PASS', 'schema': APPLY_SCHEMA, 'table': core.TARGET,
            'topic': TOPIC, 'rows': 521, 'live_rows': 512, 'tombstones': 9,
            'digest_sha256': digest, 'main_snapshot': target_snapshot,
            'checkpoint': core.CHECKPOINT, 'source_snapshot': seed['source_snapshot'],
            'time_utc': datetime.now(timezone.utc).isoformat()})
    print(f'STEP65_LIGHT_VERIFY_PASS: 521/512/9 checkpoint={committed} kafka_end={latest} snapshot={target_snapshot}')


def store_proof():
    data = json.load(sys.stdin)
    seed = core.read_proof(core.SEED_PROOF)
    require(proof_valid(data, seed), 'Invalid remote producer proof; not storing')
    if core.exists(core.PRODUCE_PROOF):
        require(proof_valid(core.read_proof(core.PRODUCE_PROOF), seed),
                'Conflicting existing producer proof')
        print('STEP65_LIGHT_PRODUCE_PROOF_EXISTS')
    else:
        core.write_proof(core.PRODUCE_PROOF, data)
        print('STEP65_LIGHT_PRODUCE_PROOF_WRITTEN')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True,
                        choices=['produce', 'reconcile', 'replay', 'verify', 'store-proof', 'guard-yarn'])
    parser.add_argument('--bootstrap', default='192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092')
    parser.add_argument('--trino', default=TRINO)
    parser.add_argument('--offset-line', default='')
    args = parser.parse_args()
    require(args.trino == TRINO, 'Unexpected Trino endpoint')
    if args.mode in ('produce', 'reconcile', 'replay'):
        envelope = json.load(sys.stdin)
        print(json.dumps(publisher(args.mode, args.bootstrap, args.trino, envelope),
                         sort_keys=True, separators=(',', ':')))
    elif args.mode == 'store-proof':
        store_proof()
    elif args.mode == 'guard-yarn':
        check_legacy_yarn_job()
        print('STEP65_LIGHT_YARN_GUARD_PASS')
    else:
        verifier(args.trino, args.offset_line)


if __name__ == '__main__':
    try:
        main()
    except (LightError, core.GuardError, OSError, TypeError, ValueError, KeyError,
            TimeoutError, subprocess.TimeoutExpired) as exc:
        print('STEP65_LIGHT_FAILED: ' + str(exc), file=sys.stderr, flush=True)
        sys.exit(1)

#!/usr/bin/env python3
"""Step 61: evidence-bound OpenMetadata manual Iceberg registry and read-only metrics.

This does not automatically ingest Spark/OpenLineage, update Iceberg, or run ETL.
Plan: HTTP GET only. Apply: create *missing* OM Containers/lineage only.
Verify: require existing OM assets/edges; publish atomic node-exporter metrics.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

SERVICE = 'bigtop_hdfs_olist'
GOLD = 'gold_daily_category_sales'
SOURCE_VERSION = 'v3'
EXPECTED_TARGET = 'shared.olist59_v3.gold_daily_category_sales'
REST_ROOT = 'hdfs://master:9000/data/olist/iceberg-rest/'
# Exact correspondence to Step 58's MAPPING (not inferred from a table name).
MAPPING = (
    ('curated', 'orders', 'silver_orders'),
    ('curated', 'order_items', 'silver_order_items'),
    ('curated', 'customers', 'silver_customers'),
    ('curated', 'products', 'silver_products'),
    ('curated', 'order_item_fact', 'silver_order_item_fact'),
    ('quarantine', 'rows', 'quality_quarantine_rows'),
    ('mart', 'daily_category_sales', GOLD),
)


class ObservationError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise ObservationError(message)


def file_json(path):
    p = Path(path)
    require(p.is_file() and not p.is_symlink(), f'Missing/nonregular evidence: {p.name}')
    raw = p.read_bytes()
    try:
        obj = json.loads(raw)
    except ValueError as exc:
        raise ObservationError(f'Invalid JSON evidence: {p.name}') from exc
    require(isinstance(obj, dict), f'Expected object evidence: {p.name}')
    return obj, raw


def evidence_from(project):
    base = Path(project) / 'artifacts'
    proof58, _ = file_json(base / '58-iceberg-v3-proof.json')
    stage, stage_raw = file_json(base / '59-olist-wap-stage.json')
    publish, _ = file_json(base / '59-olist-wap-publish.json')
    receipt, _ = file_json(base / '60-iceberg-release-last-attempt.json')
    expected = {m[2] for m in MAPPING}
    counts = proof58.get('tables')
    require(proof58.get('schema') == 'bigtop.olist.iceberg-materialization.v1' and
            proof58.get('status') == 'PASS' and proof58.get('namespace') == 'olist58_v3' and
            proof58.get('source_version') == 'v3' and
            proof58.get('parity') == 'bidirectional_exceptAll_exact_multiset' and
            isinstance(counts, dict) and set(counts) == expected and
            all(type(v) is int and v >= 0 for v in counts.values()),
            'Step58 complete seven-table exact parity proof invalid')
    n = counts[GOLD]
    require(n > 1, 'Published Gold must have more than the technical seed')
    checks = stage.get('checks')
    require(stage.get('schema') == 'bigtop.olist.iceberg-wap-stage.v1' and
            stage.get('status') == 'PASS' and stage.get('target_table') == EXPECTED_TARGET and
            stage.get('source_namespace') == 'olist58_v3' and stage.get('branch') == 'audit_v3' and
            stage.get('quality_checks_total') == 29 and stage.get('quality_checks_failed') == 0 and
            isinstance(checks, list) and len(checks) == 29 and
            all(isinstance(c, dict) and c.get('status') == 'PASS' for c in checks) and
            stage.get('expected_gold_count') == n and stage.get('candidate_gold_count') == n,
            'Step59 29 quality checks and Gold candidate proof invalid')
    snaps = stage.get('source_snapshot_ids')
    require(isinstance(snaps, dict) and set(snaps) == expected and
            all(isinstance(v, str) and v.isdigit() for v in snaps.values()),
            'Step59 immutable source snapshot inventory incomplete')
    audit = stage.get('audit_snapshot_id')
    before = stage.get('main_snapshot_before')
    require(isinstance(audit, str) and audit.isdigit() and
            isinstance(before, str) and before.isdigit() and before != audit,
            'Stage must have distinct main and audited snapshots')
    require(publish.get('schema') == 'bigtop.olist.iceberg-wap-publish.v1' and
            publish.get('status') == 'PASS' and publish.get('target_table') == EXPECTED_TARGET and
            publish.get('branch') == 'audit_v3' and
            publish.get('main_snapshot_id') == audit and publish.get('published_rows') == n and
            publish.get('stage_report_sha256') == hashlib.sha256(stage_raw).hexdigest(),
            'Step59 publication is not bound to audited snapshot')
    require(receipt.get('schema') == 'bigtop.olist.iceberg-release-run.v1' and
            receipt.get('status') == 'PASS' and receipt.get('source_version') == 'v3' and
            receipt.get('target') == EXPECTED_TARGET and
            receipt.get('mode') in ('run', 'publish', 'verify'),
            'Latest Step60 receipt is not successful; re-verify release first')
    return {'counts': counts, 'snapshots': snaps, 'audit': audit,
            'checks': len(checks), 'gold_rows': n, 'receipt': receipt}


def private_token(path):
    p = Path(path)
    require(p.is_file() and not p.is_symlink() and not (p.stat().st_mode & 0o077),
            'OpenMetadata token must be a private 0600 regular file')
    token = p.read_text(encoding='utf-8').strip()
    require(token and not any(c.isspace() for c in token), 'OpenMetadata token invalid')
    return token


def http_json(method, url, payload=None, token=None):
    headers = {'Accept': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    raw = None
    if payload is not None:
        raw = json.dumps(payload).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = Request(url, data=raw, headers=headers, method=method)
    try:
        with urlopen(req, timeout=20) as response:
            b = response.read()
            return json.loads(b) if b else {}
    except HTTPError as exc:
        if method == 'GET' and exc.code == 404:
            return None
        # Never dump server response, which might contain private details.
        raise ObservationError(f'{method} {urlsplit(url).path.split("/")[1:4]} HTTP {exc.code}') from None
    except (OSError, URLError, TimeoutError) as exc:
        raise ObservationError(f'{method} HTTP failure ({type(exc).__name__})') from None


def trino_rows(base, sql):
    u = urlsplit(base)
    require(u.scheme in ('http', 'https') and u.hostname and not u.username and
            u.path in ('', '/') and not u.query, 'Invalid Trino HTTP URL')
    uri = base.rstrip('/') + '/v1/statement'
    def page(method, url, body=None):
        raw = body.encode() if body is not None else None
        req = Request(url, data=raw, method=method,
                      headers={'X-Trino-User': 'step61_observability',
                               'Content-Type': 'text/plain; charset=utf-8'})
        try:
            with urlopen(req, timeout=25) as response:
                return json.load(response)
        except (OSError, URLError, HTTPError, ValueError) as exc:
            raise ObservationError(f'Trino request failed ({type(exc).__name__})') from None
    current = page('POST', uri, sql)
    results = []
    deadline = time.monotonic() + 240
    while True:
        if current.get('error'):
            e = current['error']
            raise ObservationError('Trino query failed: ' + str(e.get('errorName')))
        results.extend(current.get('data') or [])
        next_uri = current.get('nextUri')
        if not next_uri:
            break
        v = urlsplit(next_uri)
        require(v.scheme == u.scheme and v.netloc == u.netloc and not v.username,
                'Trino nextUri unexpectedly changed origin')
        require(time.monotonic() < deadline, 'Trino query timed out')
        time.sleep(.15)
        current = page('GET', next_uri)
    return results


def safe_location(uri):
    require(isinstance(uri, str) and uri.startswith(REST_ROOT),
            'Iceberg physical table location outside dedicated Step56 HDFS warehouse')
    u = urlsplit(uri)
    require(u.scheme == 'hdfs' and u.netloc == 'master:9000' and
            not u.query and not u.fragment and '..' not in u.path.split('/') and
            '//' not in u.path, 'Unsafe Iceberg HDFS table location')
    return uri.rstrip('/')


def ice_meta(rest_url, namespace, name):
    base = rest_url.rstrip('/')
    u = urlsplit(base)
    require(u.scheme in ('http', 'https') and u.netloc and u.path == '/iceberg',
            'Expected REST Catalog URL ending in /iceberg')
    route = f'/v1/namespaces/{quote(namespace, safe="")}/tables/{quote(name, safe="")}'
    obj = http_json('GET', base + route)
    require(isinstance(obj, dict) and isinstance(obj.get('metadata'), dict),
            f'Missing Iceberg REST table {namespace}.{name}')
    metadata = obj['metadata']
    location = safe_location(metadata.get('location'))
    ident = obj.get('identifier')
    if ident is not None:
        require(isinstance(ident, dict) and ident.get('name') == name and
                ident.get('namespace') == [namespace], 'REST table identifier mismatch')
    current = metadata.get('current-snapshot-id')
    require(type(current) is int and current > 0, 'REST table has no committed snapshot')
    return {'location': location, 'snapshot': str(current), 'refs': metadata.get('refs') or {}}


def verify_actual(rest_url, trino_url, evidence):
    results = {}
    for zone, original, target in MAPPING:
        table = ice_meta(rest_url, 'olist58_v3', target)
        require(table['snapshot'] == evidence['snapshots'][target],
                f'Step58 snapshot drift: {target}')
        rows = trino_rows(trino_url,
                          f'SELECT count(*) FROM iceberg_rest.olist58_v3.{target}')
        require(rows == [[evidence['counts'][target]]], f'Trino row parity drift: {target}')
        results[target] = {'namespace': 'olist58_v3', 'location': table['location'],
                           'rows': evidence['counts'][target], 'snapshot': table['snapshot']}
    table = ice_meta(rest_url, 'olist59_v3', GOLD)
    refs = table['refs']
    require(isinstance(refs, dict) and
            str((refs.get('main') or {}).get('snapshot-id')) == evidence['audit'] and
            str((refs.get('audit_v3') or {}).get('snapshot-id')) == evidence['audit'] and
            table['snapshot'] == evidence['audit'],
            'Gold published main/audit REST snapshot does not match immutable proof')
    rows = trino_rows(trino_url,
                      f'SELECT count(*) FROM iceberg_rest.olist59_v3.{GOLD}')
    require(rows == [[evidence['gold_rows']]], 'Published Trino Gold count drift')
    results['published_gold'] = {'namespace': 'olist59_v3', 'location': table['location'],
                                 'rows': evidence['gold_rows'], 'snapshot': evidence['audit']}
    return results


def container_specs(results):
    spec = {}
    for zone, original, target in MAPPING:
        t = results[target]
        path = urlsplit(t['location']).path
        name = f'iceberg_58_v3_{target}'
        spec[name] = {'name': name, 'displayName': f'Iceberg 58 v3 {target}',
                      'service': SERVICE, 'prefix': path, 'fullPath': t['location'],
                      'fileFormats': ['parquet'],
                      'description': (f'Step58 verified Iceberg table olist58_v3.{target}; '
                                      f'rows={t["rows"]}; snapshot={t["snapshot"]}; '
                                      f'source={zone}/v3/{original}. '
                                      'Manual evidence-bound registration; not native Iceberg ingestion.')}
    gold = results['published_gold']
    name = 'iceberg_59_v3_gold_daily_category_sales'
    spec[name] = {'name': name, 'displayName': 'Iceberg 59 v3 published Gold',
                  'service': SERVICE, 'prefix': urlsplit(gold['location']).path,
                  'fullPath': gold['location'], 'fileFormats': ['parquet'],
                  'description': (f'Step59 audited Gold main. rows={gold["rows"]}; '
                                  f'snapshot={gold["snapshot"]}; 29/29 DQ PASS. '
                                  'One table WAP, NOT multi-table atomic promotion; '
                                  'manual evidence-bound registration.')}
    return spec


def source_name(zone, original):
    return f'{zone}_v3_{original}'


def edge_names():
    edges = [(source_name(zone, original), f'iceberg_58_v3_{target}')
             for zone, original, target in MAPPING]
    edges.append((f'iceberg_58_v3_{GOLD}', f'iceberg_59_v3_{GOLD}'))
    return edges


class OM:
    def __init__(self, base_url, token):
        u = urlsplit(base_url)
        require(u.scheme in ('http', 'https') and u.netloc and u.path == '/api',
                'Expected OpenMetadata base URL ending /api')
        self.base = base_url.rstrip('/') + '/v1/'
        self.token = token

    def get(self, route):
        return http_json('GET', self.base + route, token=self.token)

    def put(self, route, payload):
        return http_json('PUT', self.base + route, payload, token=self.token)

    def post(self, route, payload):
        return http_json('POST', self.base + route, payload, token=self.token)

    def container(self, name):
        return self.get('containers/name/' + quote(SERVICE + '.' + name, safe=''))

    def downstream(self, name):
        return self.get('lineage/container/name/' + quote(SERVICE + '.' + name, safe='') +
                        '?upstreamDepth=0&downstreamDepth=1')


def validate_object(obj, name, spec=None):
    require(isinstance(obj, dict) and obj.get('id') and obj.get('name') == name and
            (obj.get('service') or {}).get('name') == SERVICE,
            'Missing/conflicting OpenMetadata Container: ' + name)
    if spec is not None:
        require(obj.get('prefix') in (None, spec['prefix']) and
                obj.get('fullPath') in (None, spec['fullPath']),
                'OpenMetadata Container path collision: ' + name)


def contains_edge(graph, source_id, target_id):
    require(isinstance(graph, dict), 'Missing OpenMetadata lineage graph')
    return any(str(e.get('fromEntity')) == str(source_id) and
               str(e.get('toEntity')) == str(target_id)
               for e in graph.get('downstreamEdges') or [])


def reconcile_metadata(client, specs, mode):
    service = client.get('services/storageServices/name/' + quote(SERVICE, safe=''))
    require(isinstance(service, dict) and service.get('name') == SERVICE and
            service.get('serviceType') == 'CustomStorage',
            'Step50 CustomStorage service missing; run Step50 for v3 first')
    source_names = {name for name, _ in edge_names()} - set(specs)
    found = {}
    for name in sorted(source_names):
        obj = client.container(name)
        validate_object(obj, name)
        found[name] = obj
    # Check all target names before allowing any mutation.
    for name, spec in specs.items():
        obj = client.container(name)
        if obj is None:
            require(mode != 'verify', 'Step61 OpenMetadata target not registered: ' + name)
        else:
            validate_object(obj, name, spec)
        found[name] = obj
    created, linked = 0, 0
    for name, spec in specs.items():
        if found[name] is None:
            print('STEP61_OM_CREATE_CONTAINER: ' + name, flush=True)
            if mode == 'apply':
                client.post('containers', spec)
                found[name] = client.container(name)
                validate_object(found[name], name, spec)
                created += 1
    for source, target in edge_names():
        if found[source] is None or found[target] is None:
            print(f'STEP61_OM_PLAN_EDGE: {source} -> {target}', flush=True)
            continue
        graph = client.downstream(source)
        if not contains_edge(graph, found[source]['id'], found[target]['id']):
            print(f'STEP61_OM_CREATE_EDGE: {source} -> {target}', flush=True)
            if mode == 'apply':
                client.put('lineage', {'edge': {
                    'fromEntity': {'id': found[source]['id'], 'type': 'container'},
                    'toEntity': {'id': found[target]['id'], 'type': 'container'},
                    'description': ('Step58 Parquet-to-Iceberg verified materialization or '
                                    'Step59 Gold audited WAP; evidence-based workflow, '
                                    'not automated column lineage/OpenLineage.'),
                }})
                linked += 1
            else:
                require(mode != 'verify', f'Step61 lineage edge missing: {source} -> {target}')
    if mode in ('apply', 'verify'):
        for source, target in edge_names():
            graph = client.downstream(source)
            require(contains_edge(graph, found[source]['id'], found[target]['id']),
                    f'OpenMetadata lineage post-check failed: {source} -> {target}')
        print(f'STEP61_OPENMETADATA_PASS: assets={len(specs)} edges={len(edge_names())} '
              f'created={created} linked={linked}', flush=True)
    else:
        print(f'STEP61_OPENMETADATA_PLAN: assets={len(specs)} edges={len(edge_names())}', flush=True)


def metric_lines(healthy, observed_at, evidence=None):
    labels = '{source_version="v3"}'
    values = [
        ('olist_iceberg61_observation_success', 'End-to-end Step61 observation passed', int(healthy)),
        ('olist_iceberg61_last_check_timestamp_seconds', 'Time of last Step61 observation', observed_at),
    ]
    if healthy:
        values += [
            ('olist_iceberg61_quality_checks_passed', 'Approved Step59 quality checks passed', 29),
            ('olist_iceberg61_published_gold_rows', 'Rows in audited published Gold main',
             evidence['gold_rows']),
            ('olist_iceberg61_main_snapshot_verified', 'REST main matches published WAP snapshot', 1),
            ('olist_iceberg61_openmetadata_assets', 'Verified Step61 OpenMetadata assets', 8),
            ('olist_iceberg61_openmetadata_edges', 'Verified Step61 lineage edges', 8),
        ]
    lines = []
    for name, description, value in values:
        lines.extend([f'# HELP {name} {description}', f'# TYPE {name} gauge',
                      f'{name}{labels} {value}'])
    return '\n'.join(lines) + '\n'


def atomic_metrics(path, content):
    file = Path(path)
    require(file.suffix == '.prom' and file.parent.is_dir() and not file.is_symlink(),
            'Expected existing Node Exporter textfile directory and .prom filename')
    temp = None
    try:
        fd, temp = tempfile.mkstemp(prefix='.olist_iceberg61.', dir=file.parent)
        with os.fdopen(fd, 'w', encoding='utf-8') as writer:
            writer.write(content)
            writer.flush()
            os.fsync(writer.fileno())
        os.chmod(temp, 0o644)
        os.replace(temp, file)
    finally:
        if temp and os.path.exists(temp):
            os.unlink(temp)


def run(args):
    evidence = evidence_from(args.project)
    results = verify_actual(args.rest_url, args.trino_url, evidence)
    print(f'STEP61_ICEBERG_TRINO_PASS: tables=8 gold_rows={evidence["gold_rows"]} '
          f'snapshot={evidence["audit"]}', flush=True)
    token = private_token(args.token_file)
    client = OM(args.om_url, token)
    specs = container_specs(results)
    require(len(specs) == 8 and len(edge_names()) == 8, 'Invalid Step61 metadata inventory')
    reconcile_metadata(client, specs, args.mode)
    if args.mode in ('apply', 'verify'):
        atomic_metrics(args.metrics_file, metric_lines(True, int(time.time()), evidence))
        print('STEP61_METRICS_PASS: ' + args.metrics_file, flush=True)
    if args.mode == 'plan':
        print('STEP61_PLAN_PASS: no OpenMetadata changes or metrics file writes', flush=True)
    elif args.mode == 'apply':
        print('STEP61_APPLY_PASS: evidence-bound metadata registered', flush=True)
    else:
        print('STEP61_VERIFY_PASS: REST+Trino+OM+Prometheus textfile', flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=('plan', 'apply', 'verify'), default='plan')
    p.add_argument('--project', default=str(Path(__file__).resolve().parent.parent))
    p.add_argument('--rest-url', default='http://192.158.2.103:9001/iceberg')
    p.add_argument('--trino-url', default='http://192.158.2.104:8080')
    p.add_argument('--om-url', default='http://192.158.2.104:8585/api')
    p.add_argument('--token-file', default='/etc/bigtop-governance/pg-catalog/bot.token')
    p.add_argument('--metrics-file', default='/var/lib/prometheus/node-exporter/olist_iceberg61.prom')
    args = p.parse_args()
    try:
        run(args)
    except (ObservationError, ValueError, KeyError, OSError) as exc:
        if args.mode != 'plan':
            try:
                atomic_metrics(args.metrics_file, metric_lines(False, int(time.time())))
            except (ObservationError, OSError):
                pass
        print('STEP61_FAIL: ' + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Step 51: import actual per-output Spark file-input evidence for Olist snapshots.

Only the Step 46 --runtime-lineage execution can generate this evidence. This
bridge supports that specific batch ETL. It is NOT OpenLineage or a generic
Spark SQL lineage listener; no JAR or secrets are pushed into YARN containers.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlparse, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from uuid import UUID

RAW_FILENAMES = {
    'orders': 'olist_orders_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'products': 'olist_products_dataset.csv',
}
OUTPUTS = {
    'curated/orders', 'curated/order_items', 'curated/customers',
    'curated/products', 'curated/order_item_fact',
    'quarantine/rows', 'mart/daily_category_sales',
}
VERSION = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,31}\Z')
SERVICE = re.compile(r'[a-z][a-z0-9_]{2,63}\Z')


class EvidenceError(RuntimeError):
    pass


def read_private_token(path):
    p = Path(path)
    if not p.is_file() or p.is_symlink() or p.stat().st_mode & 0o077:
        raise EvidenceError('OpenMetadata Bot token must be a private (0600) regular file')
    value = p.read_text(encoding='utf-8').strip()
    if not value or any(ch.isspace() for ch in value):
        raise EvidenceError('Invalid Bot token file')
    return value


def load(path):
    p = Path(path)
    if not p.is_file() or p.is_symlink():
        raise EvidenceError('Expected a regular local JSON snapshot')
    obj = json.loads(p.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise EvidenceError('Expected JSON object')
    return obj


def hdfs_uri(uri, address):
    if not isinstance(uri, str):
        raise EvidenceError('Spark input/output must be string URIs')
    u = urlparse(uri)
    if u.scheme != 'hdfs' or u.netloc not in (address, '') or not u.path.startswith('/data/olist/'):
        raise EvidenceError('Unexpected HDFS URI origin/path; refused: ' + uri[:120])
    if u.params or u.query or u.fragment or '//' in u.path or '..' in u.path.split('/'):
        raise EvidenceError('Unsafe HDFS URI: ' + uri[:120])
    return u.path


def source_for(uri, raw_version, address):
    path = hdfs_uri(uri, address)
    for key, filename in RAW_FILENAMES.items():
        expected = f'/data/olist/raw/{raw_version}/{key}/{filename}'
        if path == expected:
            return key
    # inputFiles gives physical file scan leaves; we do not fabricate sources.
    raise EvidenceError('Runtime source was not one of four registered Olist core CSV files: ' + path)


def parse_evidence(event, manifest, raw_version, wh_version, address):
    if not VERSION.fullmatch(raw_version) or not VERSION.fullmatch(wh_version):
        raise EvidenceError('Invalid immutable snapshot version')
    if (event.get('schema') != 'bigtop.olist.spark-runtime-lineage.v1' or
            event.get('status') != 'COMPLETE' or
            event.get('pipeline') != 'olist_spark_batch_etl' or
            event.get('raw_version') != raw_version or
            event.get('warehouse_version') != wh_version):
        raise EvidenceError('Missing or mismatched Spark runtime evidence identity')
    if manifest.get('pipeline') != 'olist_spark_batch_etl' or manifest.get('raw_version') != raw_version:
        raise EvidenceError('Inconsistent Step 46 immutable ETL manifest')
    try:
        UUID(event['run_id'])
        start = datetime.fromisoformat(event['started_at'])
        end = datetime.fromisoformat(event['finished_at'])
        if not start.tzinfo or not end.tzinfo or end < start:
            raise ValueError('Runtime event timestamps are invalid')
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidenceError('Runtime event must have run UUID and valid UTC timestamps') from exc
    if not isinstance(event.get('writes'), list) or len(event['writes']) != len(OUTPUTS):
        raise EvidenceError('Expected exactly seven completed actual Parquet writes')
    rows = manifest.get('metrics', {}).get('output_rows', {})
    if set(rows) != OUTPUTS:
        raise EvidenceError('Published manifest does not contain exactly seven Parquet row metrics')
    outputs = {}
    for item in event['writes']:
        if not isinstance(item, dict) or item.get('output') not in OUTPUTS:
            raise EvidenceError('Unexpected runtime output')
        name = item['output']
        if name in outputs:
            raise EvidenceError('Duplicate Spark write destination')
        expected = f'/data/olist/warehouse/{wh_version}/{name}'
        if hdfs_uri(item.get('published_path'), address) != expected:
            raise EvidenceError('Runtime published output path mismatch: ' + name)
        staged = hdfs_uri(item.get('write_path'), address)
        if not staged.startswith(f'/data/olist/warehouse/_staging/{wh_version}-') or not staged.endswith('/' + name):
            raise EvidenceError('Spark write did not originate under the immutable staging area')
        if (not isinstance(item.get('output_rows'), int) or
                item['output_rows'] != rows[name]):
            raise EvidenceError('Runtime write count did not match published ETL manifest')
        files = item.get('input_files')
        if not isinstance(files, list):
            raise EvidenceError('Missing DataFrame.inputFiles evidence')
        inputs = {source_for(p, raw_version, address) for p in files}
        outputs[name] = inputs
    if set(outputs) != OUTPUTS:
        raise EvidenceError('Missing Spark writes')
    # Empty inputs are not fabricated into inferred lineage.
    return outputs


class OM:
    def __init__(self, url, token):
        if not re.fullmatch(r'https?://[^/\s]+/api', url):
            raise EvidenceError('Expected OpenMetadata API base URL http(s)://host:port/api')
        self.url = url
        self.token = token

    def call(self, method, route, payload=None):
        body = json.dumps(payload).encode('utf-8') if payload is not None else None
        headers = {'Authorization': 'Bearer ' + self.token, 'Accept': 'application/json'}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        req = Request(self.url + '/v1/' + route, data=body, headers=headers, method=method)
        try:
            with urlopen(req, timeout=20) as r:
                data = r.read()
                return json.loads(data) if data else {}
        except HTTPError as exc:
            if method == 'GET' and exc.code == 404:
                return None
            raise EvidenceError(f'OpenMetadata {method} {route.split("/")[0]} HTTP {exc.code}') from None
        except (OSError, URLError, TimeoutError) as exc:
            raise EvidenceError(f'OpenMetadata connection failure ({type(exc).__name__})') from None

    def container(self, fqn):
        return self.call('GET', 'containers/name/' + quote(fqn, safe=''))

    def graph(self, fqn):
        return self.call('GET', 'lineage/container/name/' + quote(fqn, safe='') +
                         '?upstreamDepth=0&downstreamDepth=1')


def add_run_lineage(client, outputs, service, raw_version, wh_version, apply):
    if not SERVICE.fullmatch(service):
        raise EvidenceError('Invalid OpenMetadata storage service name')
    all_names = {}
    for name in RAW_FILENAMES:
        all_names['raw/' + name] = f'{service}.raw_{raw_version}_{name}'
    for name in OUTPUTS:
        zone, table = name.split('/')
        all_names[name] = f'{service}.{zone}_{wh_version}_{table}'
    wanted_edges = sorted({('raw/' + source, dest) for dest, sources in outputs.items() for source in sources})
    # Strict preflight: every node exists before *any* mutation.
    objects = {}
    for key in {x for edge in wanted_edges for x in edge}:
        fqn = all_names[key]
        obj = client.container(fqn)
        if not isinstance(obj, dict) or not obj.get('id') or obj.get('fullyQualifiedName') not in (None, fqn):
            raise EvidenceError('Register Olist catalog containers with Step 50 before importing: ' + fqn)
        objects[key] = obj
    # Retrieve entire source graphs before writes, so GET errors fail closed.
    source_graphs = {}
    for source in sorted({edge[0] for edge in wanted_edges}):
        graph = client.graph(all_names[source])
        if not isinstance(graph, dict):
            raise EvidenceError('OpenMetadata lineage graph unavailable for ' + all_names[source])
        source_graphs[source] = graph
    print(f'OLIST RUNTIME LINEAGE {"APPLY" if apply else "PLAN"}: '
          f'executed_writes={len(outputs)} observed_edges={len(wanted_edges)}', flush=True)
    missing_outputs = sorted(set(outputs) - {n for n, sources in outputs.items() if sources})
    if missing_outputs:
        print('WARNING no physical file inputs reported by Spark for outputs: ' +
              ', '.join(missing_outputs), flush=True)
    created = 0
    for source, dest in wanted_edges:
        src, dst = objects[source], objects[dest]
        existing = any(str(e.get('fromEntity')) == str(src['id']) and
                       str(e.get('toEntity')) == str(dst['id'])
                       for e in (source_graphs[source].get('downstreamEdges') or []))
        print(('EXISTS ' if existing else 'CREATE ') + all_names[source] + ' -> ' + all_names[dest], flush=True)
        if not existing and apply:
            client.call('PUT', 'lineage', {'edge': {
                'fromEntity': {'id': src['id'], 'type': 'container'},
                'toEntity': {'id': dst['id'], 'type': 'container'},
                'description': ('Step 51 observed Spark DataFrame.inputFiles and completed '
                                'Parquet write; this is execution-derived physical-file '
                                'provenance, not inferred intermediate table lineage.'),
            }})
            created += 1
    print(f'OLIST RUNTIME LINEAGE {"CHANGED" if created else "UNCHANGED"}: '
          f'{created} new edges', flush=True)
    return created


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--plan', action='store_true')
    mode.add_argument('--apply', action='store_true')
    for arg in ('event-file', 'manifest-file', 'token-file', 'url', 'service',
                'raw-version', 'warehouse-version', 'hdfs-address'):
        parser.add_argument('--' + arg, required=True)
    args = parser.parse_args()
    try:
        if os.geteuid() != 0:
            raise EvidenceError('Run importer as root to read protected Step 42 token')
        event = load(args.event_file)
        manifest = load(args.manifest_file)
        outputs = parse_evidence(event, manifest, args.raw_version,
                                 args.warehouse_version, args.hdfs_address)
        add_run_lineage(OM(args.url, read_private_token(args.token_file)), outputs,
                        args.service, args.raw_version, args.warehouse_version, args.apply)
        return 0
    except (EvidenceError, OSError, ValueError, TypeError, KeyError) as exc:
        print('OLIST RUNTIME LINEAGE ERROR: ' + str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

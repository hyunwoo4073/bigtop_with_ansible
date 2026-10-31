#!/usr/bin/env python3
"""Olist HDFS snapshots -> OpenMetadata 2.0.6 CustomStorage containers.

This is a MANUAL metadata catalog + code-evidenced lineage. It is not a native
HDFS connector, not a runtime Spark/OpenLineage listener, and never touches data.
Default --plan performs GET requests only. --apply creates missing entities only.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

RAW_FILENAMES = {
    'orders': 'olist_orders_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'reviews': 'olist_order_reviews_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'category_translation': 'product_category_name_translation.csv',
}
OUTPUTS = (
    'curated/orders', 'curated/order_items', 'curated/customers',
    'curated/products', 'curated/order_item_fact', 'quarantine/rows',
    'mart/daily_category_sales',
)
# Based exclusively on the Step 46 build_tables implementation:
# 4 Raw sources -> corresponding validated tables;
# all 4 curated inputs -> joined item-level fact;
# fact -> daily delivered-order category mart;
# all 4 core sources -> rejected / orphan rows quarantine.
LINEAGE = tuple(
    [('raw', n), ('curated', n)] for n in ('orders', 'order_items', 'customers', 'products')
) + tuple(
    [ ('curated', n), ('curated', 'order_item_fact') ]
    for n in ('orders', 'order_items', 'customers', 'products')
) + (
    [('curated', 'order_item_fact'), ('mart', 'daily_category_sales')],
) + tuple(
    [('raw', n), ('quarantine', 'rows')]
    for n in ('orders', 'order_items', 'customers', 'products')
)

class CatalogError(RuntimeError):
    pass


def load_private_token(path):
    p = Path(path)
    if p.is_symlink() or not p.is_file() or (p.stat().st_mode & 0o077):
        raise CatalogError('Step 42 Bot token must be a regular private file with mode 0600')
    token = p.read_text(encoding='utf-8').strip()
    if not token or any(ch.isspace() for ch in token):
        raise CatalogError('Invalid Bot token file')
    return token


def load_json(path):
    p = Path(path)
    if p.is_symlink() or not p.is_file():
        raise CatalogError('Expected regular manifest file')
    doc = json.loads(p.read_text(encoding='utf-8'))
    if not isinstance(doc, dict):
        raise CatalogError('Manifest must be a JSON object')
    return doc


def validated_manifests(raw, warehouse, raw_v, wh_v):
    if raw.get('dataset') != 'olist' or raw.get('schema_version') != 1:
        raise CatalogError('Step 45 Olist Raw manifest mismatch')
    files = raw.get('files')
    if not isinstance(files, dict) or set(files) != set(RAW_FILENAMES):
        raise CatalogError('Step 45 snapshot must list all nine Olist sources')
    for name, filename in RAW_FILENAMES.items():
        item = files[name]
        if (not isinstance(item, dict) or item.get('filename') != filename or
                not isinstance(item.get('rows'), int) or item.get('rows') < 0 or
                not isinstance(item.get('bytes'), int) or item.get('bytes') <= 0 or
                not re.fullmatch(r'[0-9a-f]{64}', str(item.get('sha256', '')))):
            raise CatalogError('Raw source manifest mismatch: ' + name)
    if (warehouse.get('pipeline') != 'olist_spark_batch_etl' or
            warehouse.get('raw_version') != raw_v or
            warehouse.get('etl_version') not in (None, wh_v) or
            set(warehouse.get('tables', [])) != set(OUTPUTS)):
        raise CatalogError('Step 46 immutable warehouse manifest mismatch')
    metrics = warehouse.get('metrics', {})
    if not isinstance(metrics, dict) or not isinstance(metrics.get('output_rows'), dict):
        raise CatalogError('Expected Step 46 output row metrics')
    for output in OUTPUTS:
        count = metrics['output_rows'].get(output)
        if not isinstance(count, int) or count < 0:
            raise CatalogError('Missing output row metric: ' + output)
    return metrics


def name_for(zone, logical_name, raw_v, wh_v):
    version = raw_v if zone == 'raw' else wh_v
    return f'{zone}_{version}_{logical_name}'


def define_assets(service, raw, metrics, raw_v, wh_v, address):
    assets = {}
    base = f'hdfs://{address}'
    for name, file in RAW_FILENAMES.items():
        entity_name = name_for('raw', name, raw_v, wh_v)
        path = f'/data/olist/raw/{raw_v}/{name}/{file}'
        assets[('raw', name)] = {
            'name': entity_name,
            'displayName': f'Olist Raw {name} ({raw_v})',
            'service': service,
            'description': ('Immutable Step 45 CSV source in HDFS, manually registered. '
                            f'Rows: {raw["files"][name]["rows"]}; '
                            f'SHA-256: {raw["files"][name]["sha256"]}. '
                            'This does not imply automatic file ingestion.'),
            'prefix': path,
            'fullPath': base + path,
            'fileFormats': ['csv'],
            'numberOfObjects': 1,
            'size': round(raw['files'][name]['bytes'] / 1024.0, 3),
        }
    for output in OUTPUTS:
        zone, name = output.split('/')
        path = f'/data/olist/warehouse/{wh_v}/{output}'
        assets[(zone, name)] = {
            'name': name_for(zone, name, raw_v, wh_v),
            'displayName': f'Olist {zone} {name} ({wh_v})',
            'service': service,
            'description': ('Immutable Step 46 Spark on YARN Cluster output in HDFS. '
                            f'Rows: {metrics["output_rows"][output]}. '
                            'Registered from verified output manifest and _SUCCESS; '
                            'column schema ingestion and Spark runtime lineage are not enabled.'),
            'prefix': path,
            'fullPath': base + path,
            'fileFormats': ['parquet'],
        }
    assert len(assets) == 16
    return assets


class OM:
    def __init__(self, url, token):
        if not re.fullmatch(r'https?://[^/\s]+/api', url):
            raise CatalogError('Expected OpenMetadata URL http(s)://hostname:port/api')
        self.url = url.rstrip('/')
        self.token = token

    def call(self, method, path, body=None):
        headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + self.token}
        payload = None
        if body is not None:
            payload = json.dumps(body, ensure_ascii=False).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        req = Request(f'{self.url}/v1/{path}', data=payload, headers=headers, method=method)
        try:
            with urlopen(req, timeout=20) as response:
                b = response.read()
                return json.loads(b) if b else {}
        except HTTPError as exc:
            if method == 'GET' and exc.code == 404:
                return None
            # Do not log HTTP response body or authentication headers.
            raise CatalogError(f'OpenMetadata {method} {path.split("/")[0]} HTTP {exc.code}') from None
        except (OSError, URLError, TimeoutError) as exc:
            raise CatalogError(f'OpenMetadata connection error ({type(exc).__name__})') from None

    def service(self, name):
        return self.call('GET', 'services/storageServices/name/' + quote(name, safe=''))

    def container(self, fqn):
        return self.call('GET', 'containers/name/' + quote(fqn, safe=''))

    def downstream(self, fqn):
        return self.call('GET', 'lineage/container/name/' + quote(fqn, safe='') +
                         '?upstreamDepth=0&downstreamDepth=1')


def verify_existing_service(obj, service):
    if not isinstance(obj, dict) or obj.get('serviceType') != 'CustomStorage' or obj.get('name') != service:
        raise CatalogError('Existing OpenMetadata storage service conflicts with requested catalog')


def verify_existing_container(obj, expected):
    if not isinstance(obj, dict) or not obj.get('id'):
        raise CatalogError('Malformed existing OpenMetadata Container')
    if obj.get('name') != expected['name'] or obj.get('service', {}).get('name') != expected['service']:
        raise CatalogError('Existing Container has conflicting identity')
    for field in ('prefix', 'fullPath'):
        if obj.get(field) not in (None, expected[field]):
            raise CatalogError('Existing Container has a different HDFS path: ' + expected['name'])


def existing_edge(graph, source_id, dest_id):
    if not isinstance(graph, dict):
        raise CatalogError('Cannot fetch existing lineage graph; refusing to write blindly')
    return any(str(e.get('fromEntity')) == str(source_id) and
               str(e.get('toEntity')) == str(dest_id)
               for e in (graph.get('downstreamEdges') or []))


def run(client, service, assets, apply):
    service_obj = client.service(service)
    if service_obj is not None:
        verify_existing_service(service_obj, service)
    # Preflight any existing objects BEFORE allowing any API mutation.
    existing = {}
    for key, spec in assets.items():
        fqn = service + '.' + spec['name']
        obj = client.container(fqn)
        if obj is not None:
            verify_existing_container(obj, spec)
        existing[key] = obj
    print(f'{"APPLY" if apply else "PLAN"} Olist Catalog service={service} assets={len(assets)} edges={len(LINEAGE)}', flush=True)
    changed = 0
    if service_obj is None:
        print('CREATE StorageService ' + service, flush=True)
        if apply:
            client.call('POST', 'services/storageServices', {
                'name': service,
                'displayName': 'Bigtop HDFS Olist Manual Catalog',
                'serviceType': 'CustomStorage',
                'description': ('Manually registered HDFS Olist immutable datasets. '
                                'No native HDFS connector or automated Spark lineage is configured.'),
                'connection': {'config': {'type': 'CustomStorage'}},
            })
            changed += 1
    for key, spec in assets.items():
        if existing[key] is None:
            print('CREATE Container ' + service + '.' + spec['name'], flush=True)
            if apply:
                client.call('POST', 'containers', spec)
                changed += 1
    if apply:
        service_obj = client.service(service)
        verify_existing_service(service_obj, service)
        # Resolve canonical server IDs before building lineage.
        for key, spec in assets.items():
            obj = client.container(service + '.' + spec['name'])
            verify_existing_container(obj, spec)
            existing[key] = obj
    for source, dest in LINEAGE:
        sfqn = service + '.' + assets[source]['name']
        dfqn = service + '.' + assets[dest]['name']
        if existing[source] is not None and existing[dest] is not None:
            graph = client.downstream(sfqn)
            if existing_edge(graph, existing[source]['id'], existing[dest]['id']):
                print('SKIP Lineage ' + sfqn + ' -> ' + dfqn, flush=True)
                continue
        print('CREATE Lineage ' + sfqn + ' -> ' + dfqn, flush=True)
        if apply:
            payload = {'edge': {
                'fromEntity': {'id': existing[source]['id'], 'type': 'container'},
                'toEntity': {'id': existing[dest]['id'], 'type': 'container'},
                'description': 'Manually mapped from Step 46 build_tables code; not runtime Spark/OpenLineage.',
            }}
            client.call('PUT', 'lineage', payload)
            changed += 1
    if apply:
        print(f'OLIST CATALOG {"CHANGED" if changed else "UNCHANGED"}: {changed} create operations', flush=True)
    else:
        print('OLIST CATALOG PLAN OK: zero write requests', flush=True)
    return changed


def main():
    p = argparse.ArgumentParser(description=__doc__)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--plan', action='store_true')
    mode.add_argument('--apply', action='store_true')
    for key in ('url', 'token-file', 'raw-manifest', 'warehouse-manifest',
                'raw-version', 'warehouse-version', 'service', 'hdfs-address'):
        p.add_argument('--' + key, required=True)
    args = p.parse_args()
    if os.geteuid() != 0:
        print('OLIST CATALOG ERROR: requires root to read protected token', file=sys.stderr)
        return 1
    try:
        for value in (args.raw_version, args.warehouse_version):
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,31}', value):
                raise CatalogError('Invalid immutable snapshot version')
        if not re.fullmatch(r'[a-z][a-z0-9_]{2,63}', args.service):
            raise CatalogError('Invalid service name')
        if not re.fullmatch(r'[A-Za-z0-9_.-]+:[0-9]+', args.hdfs_address):
            raise CatalogError('Invalid HDFS address')
        raw = load_json(args.raw_manifest)
        warehouse = load_json(args.warehouse_manifest)
        metrics = validated_manifests(raw, warehouse, args.raw_version, args.warehouse_version)
        assets = define_assets(args.service, raw, metrics, args.raw_version,
                               args.warehouse_version, args.hdfs_address)
        run(OM(args.url, load_private_token(args.token_file)), args.service, assets, args.apply)
        return 0
    except (CatalogError, OSError, ValueError, TypeError, KeyError) as exc:
        print('OLIST CATALOG ERROR: ' + str(exc), file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())

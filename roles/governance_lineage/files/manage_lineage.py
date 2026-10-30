#!/usr/bin/env python3
"""Only register evidence-based view lineage in OpenMetadata; plan by default.

Never changes PostgreSQL data. The separate Ansible role owns the SQL VIEW.
No deletion or overwrite of existing lineage (e.g. native ViewLineage ingestion).
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import yaml

FQN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*){3}$")
COL = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


class LineageError(Exception):
    pass


class OpenMetadataClient:
    def __init__(self, url, token):
        if not re.fullmatch(r"https?://[^/\s]+(?::\d+)?/api", url):
            raise LineageError("OpenMetadata API URL must have the form http(s)://host:port/api")
        self.url = url.rstrip('/')
        self.token = token

    def request(self, method, path, data=None, params=None):
        url = self.url + '/v1/' + path
        if params:
            url += '?' + urlencode(params)
        headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + self.token}
        body = None
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        req = Request(url, data=body, headers=headers, method=method)
        try:
            with urlopen(req, timeout=15) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            if method == 'GET' and exc.code == 404:
                return None
            # Do not echo the response body or headers, which may contain credentials.
            raise LineageError(f'OpenMetadata {method} returned HTTP {exc.code}') from None
        except (URLError, TimeoutError, OSError) as exc:
            raise LineageError(f'OpenMetadata connection error: {type(exc).__name__}') from None

    def table(self, fqn):
        return self.request('GET', 'tables/name/' + quote(fqn, safe=''), params={'fields': 'columns'})

    def downstream(self, source_fqn):
        return self.request('GET', 'lineage/table/name/' + quote(source_fqn, safe=''),
                            params={'upstreamDepth': '0', 'downstreamDepth': '1'})

    def add_edge(self, body):
        return self.request('PUT', 'lineage', data=body)


def read_token(path):
    p = Path(path)
    if p.is_symlink() or not p.is_file() or p.stat().st_mode & 0o077:
        raise LineageError('JWT token file must be a private regular file (0600)')
    token = p.read_text(encoding='utf-8').strip()
    if not token or any(ch.isspace() for ch in token):
        raise LineageError('Invalid JWT token file')
    return token


def read_policy(path):
    p = Path(path)
    if p.is_symlink() or not p.is_file():
        raise LineageError('Missing policy or symlink policy')
    doc = yaml.safe_load(p.read_text(encoding='utf-8'))
    if not isinstance(doc, dict) or doc.get('version') != 1:
        raise LineageError('Policy version must be 1')
    target = doc.get('target', '')
    sources = doc.get('sources')
    sql = doc.get('sqlQuery')
    if not isinstance(target, str) or not FQN.fullmatch(target):
        raise LineageError('Invalid target FQN')
    if not isinstance(sources, list) or len(sources) != 2 or not isinstance(sql, str):
        raise LineageError('Expect exactly two SQL-view sources and one SQL query')
    if len(sql) > 10000 or not sql.lstrip().upper().startswith('SELECT '):
        raise LineageError('Expected SELECT statement describing the real view')
    if not target.endswith('.v_bigtop_service_inventory_lineage'):
        raise LineageError('Only the Step 44 managed view is supported')
    unique = set()
    for source in sources:
        if not isinstance(source, dict) or not isinstance(source.get('fqn'), str):
            raise LineageError('Invalid lineage source')
        fqn = source['fqn']
        if not FQN.fullmatch(fqn) or fqn in unique or fqn == target:
            raise LineageError('Duplicate, self-referencing or invalid lineage source')
        if not fqn.startswith(target.rsplit('.', 1)[0] + '.'):
            raise LineageError('Source must be in same pre-existing catalog schema')
        unique.add(fqn)
        cols = source.get('columns')
        if not isinstance(cols, dict) or not cols:
            raise LineageError('Missing column-level evidence')
        for source_column, target_column in cols.items():
            if not COL.fullmatch(str(source_column)) or not COL.fullmatch(str(target_column)):
                raise LineageError('Invalid column mapping')
    if {s.rsplit('.', 1)[1] for s in unique} != {'platform_hosts', 'platform_service_instances'}:
        raise LineageError('Only real Step 32 base tables are valid sources')
    for table in ('platform_hosts', 'platform_service_instances'):
        if table not in sql:
            raise LineageError('SQL must reference both physical source tables')
    return doc


def verify_assets(client, policy):
    """Fail before any write if source/target tables or mapped columns are missing."""
    target = policy['target']
    refs = {}
    for fqn in [target] + [src['fqn'] for src in policy['sources']]:
        asset = client.table(fqn)
        if not asset or asset.get('fullyQualifiedName') != fqn or not asset.get('id'):
            raise LineageError('Catalog entity missing; re-run Step 42 ingestion: ' + fqn)
        refs[fqn] = asset
    if refs[target].get('tableType') not in ('View', 'view'):
        raise LineageError('Lineage target must be an ingested PostgreSQL VIEW, not a table')
    target_cols = {c.get('name') for c in (refs[target].get('columns') or [])}
    for source in policy['sources']:
        source_cols = {c.get('name') for c in (refs[source['fqn']].get('columns') or [])}
        for from_col, to_col in source['columns'].items():
            if from_col not in source_cols or to_col not in target_cols:
                raise LineageError(f'Column missing in catalog: {source["fqn"]}.{from_col} -> {target}.{to_col}')
    return refs


def already_exists(client, source_fqn, source_id, target_id):
    graph = client.downstream(source_fqn)
    if graph is None or not isinstance(graph, dict):
        raise LineageError('Unable to read lineage graph for ' + source_fqn)
    return any(str(edge.get('fromEntity')) == str(source_id)
               and str(edge.get('toEntity')) == str(target_id)
               for edge in (graph.get('downstreamEdges') or []))


def make_edge(policy, source, refs):
    """Manual fallback only, based on the actual SELECT projection of the view."""
    src = source['fqn']
    dest = policy['target']
    mapping = [{'fromColumns': [src + '.' + from_col],
                'toColumn': dest + '.' + to_col}
               for from_col, to_col in source['columns'].items()]
    return {'edge': {
        'fromEntity': {'id': refs[src]['id'], 'type': 'table'},
        'toEntity': {'id': refs[dest]['id'], 'type': 'table'},
        'lineageDetails': {
            'source': 'Manual',
            'description': 'Step 44 verified SQL VIEW; explicit API fallback if native ViewLineage missing',
            'sqlQuery': policy['sqlQuery'],
            'columnsLineage': mapping,
        }
    }}


def run(client, policy, apply=False):
    refs = verify_assets(client, policy)
    actions = []
    for source in policy['sources']:
        src_fqn = source['fqn']
        src = refs[src_fqn]
        dst = refs[policy['target']]
        has_edge = already_exists(client, src_fqn, src['id'], dst['id'])
        if has_edge:
            print(f'SKIP edge: {src_fqn} -> {policy["target"]} (already exists)')
        else:
            print(f'{"ADD" if apply else "PLAN ADD"} edge: {src_fqn} -> {policy["target"]}')
            actions.append(make_edge(policy, source, refs))
    # Preflight all existing lineage edges BEFORE any write. No overwrite/delete.
    if apply:
        for edge in actions:
            client.add_edge(edge)
    print(f'{"APPLIED" if apply else "PLAN OK"}: {len(actions)} new edge(s) out of 2')
    return len(actions)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url', required=True)
    p.add_argument('--token-file', required=True)
    p.add_argument('--policy', required=True)
    modes = p.add_mutually_exclusive_group(required=True)
    modes.add_argument('--plan', action='store_true')
    modes.add_argument('--apply', action='store_true')
    args = p.parse_args()
    if os.geteuid() != 0:
        print('LINEAGE FAILED: run as root (JWT file is root-only)', file=sys.stderr)
        return 1
    try:
        policy = read_policy(args.policy)
        run(OpenMetadataClient(args.url, read_token(args.token_file)), policy, apply=args.apply)
        return 0
    except (LineageError, KeyError, TypeError, ValueError, OSError, yaml.YAMLError) as exc:
        print('LINEAGE FAILED: ' + str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

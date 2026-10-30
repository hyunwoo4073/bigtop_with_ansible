#!/usr/bin/env python3
"""Conservative OpenMetadata governance bootstrap (plan first, opt-in apply).

Only writes OpenMetadata entities; it never connects to or modifies PostgreSQL.
No destructive delete and no replacement of existing human descriptions/owners/tags.
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

IDENT = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
FQN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*){3}$")


class CatalogError(Exception):
    pass


class Client:
    def __init__(self, endpoint, token):
        if not endpoint.startswith(('http://', 'https://')) or not endpoint.endswith('/api'):
            raise CatalogError('OpenMetadata URL must end with /api')
        if '\n' in endpoint or '\r' in endpoint:
            raise CatalogError('Invalid API endpoint')
        self.endpoint = endpoint.rstrip('/')
        self.token = token

    def request(self, method, path, payload=None, fields=None):
        url = self.endpoint + '/v1/' + path
        if fields:
            url += '?' + urlencode({'fields': fields})
        headers = {'Authorization': 'Bearer ' + self.token, 'Accept': 'application/json'}
        raw = None
        if payload is not None:
            raw = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            headers['Content-Type'] = ('application/json-patch+json' if method == 'PATCH'
                                       else 'application/json')
        req = Request(url, data=raw, headers=headers, method=method)
        try:
            with urlopen(req, timeout=15) as response:
                data = response.read()
                return json.loads(data) if data else {}
        except HTTPError as exc:
            if method == 'GET' and exc.code == 404:
                return None
            # Never echo response payload: a server/plugin might reflect credential fields.
            raise CatalogError(f'OpenMetadata {method} {path.split("?")[0]}: HTTP {exc.code}') from None
        except (URLError, TimeoutError, OSError) as exc:
            raise CatalogError(f'Cannot reach OpenMetadata ({type(exc).__name__})') from None

    def named(self, kind, name, fields=None):
        return self.request('GET', kind + '/name/' + quote(name, safe=''), fields=fields)


def credential(path):
    p = Path(path)
    if p.is_symlink() or not p.is_file() or (p.stat().st_mode & 0o077):
        raise CatalogError('Token file must be a regular root-only file (0600)')
    value = p.read_text(encoding='utf-8').strip()
    if not value or any(ch.isspace() for ch in value):
        raise CatalogError('Empty or invalid token file')
    return value


def validate(spec):
    if not isinstance(spec, dict) or spec.get('version') != 1:
        raise CatalogError('Policy must specify version: 1')
    classification = spec.get('classification')
    glossary = spec.get('glossary')
    tables = spec.get('tables')
    if not isinstance(classification, dict) or not isinstance(glossary, dict) or not isinstance(tables, list):
        raise CatalogError('Policy needs classification, glossary and tables')
    for node in (classification, glossary):
        if not IDENT.fullmatch(str(node.get('name', ''))) or not node.get('description'):
            raise CatalogError('Invalid classification/glossary name or description')
    for tag in classification.get('tags', []):
        if not IDENT.fullmatch(str(tag.get('name', ''))) or not tag.get('description'):
            raise CatalogError('Invalid tag')
    for term in glossary.get('terms', []):
        if not IDENT.fullmatch(str(term.get('name', ''))) or not term.get('description'):
            raise CatalogError('Invalid glossary term')
    fqn_set = set()
    known_tags = {classification['name'] + '.' + tag['name'] for tag in classification.get('tags', [])}
    known_terms = {glossary['name'] + '.' + t['name'] for t in glossary.get('terms', [])}
    for table in tables:
        fqn = str(table.get('fqn', ''))
        if not FQN.fullmatch(fqn) or fqn in fqn_set:
            raise CatalogError('Invalid or duplicate four-part table FQN')
        fqn_set.add(fqn)
        if not isinstance(table.get('description', ''), str):
            raise CatalogError('Invalid table description')
        if not set(table.get('tags', [])).issubset(known_tags | known_terms):
            raise CatalogError('Table references unconfigured governance label')
        for column, desc in table.get('columns', {}).items():
            if not IDENT.fullmatch(column) or not isinstance(desc, str):
                raise CatalogError('Invalid column description')
        owner = table.get('owner')
        if owner is not None:
            if (not isinstance(owner, dict) or owner.get('type') not in ('user', 'team')
                    or not IDENT.fullmatch(str(owner.get('name', '')))):
                raise CatalogError('Invalid owner; set type user/team and existing name')
    return spec


def label(fqn, kind):
    return {'tagFQN': fqn, 'source': kind, 'labelType': 'Manual', 'state': 'Confirmed'}


def annotation_patch(table, current, owner_ref=None, glossary_prefix=None):
    """Add only missing descriptions/labels/owner, retaining pre-existing user work."""
    patch = []
    if table.get('description') and not current.get('description'):
        patch.append({'op': 'add', 'path': '/description', 'value': table['description']})
    current_tags = current.get('tags') or []
    seen = {x['tagFQN'] for x in current_tags}
    new = []
    for tag in table.get('tags', []):
        if tag not in seen:
            new.append(label(tag, 'Glossary' if glossary_prefix and tag.startswith(glossary_prefix + '.') else 'Classification'))
            seen.add(tag)
    # Assign tags using JSON Patch append; never send/replace existing label arrays.
    if current.get('tags') is None and new:
        patch.append({'op': 'add', 'path': '/tags', 'value': new})
    else:
        for item in new:
            patch.append({'op': 'add', 'path': '/tags/-', 'value': item})
    cols = current.get('columns') or []
    col_index = {col.get('name'): i for i, col in enumerate(cols)}
    for column, description in table.get('columns', {}).items():
        if column not in col_index:
            raise CatalogError(f'Target column not ingested: {table["fqn"]}.{column}')
        if description and not cols[col_index[column]].get('description'):
            patch.append({'op': 'add', 'path': f'/columns/{col_index[column]}/description', 'value': description})
    if owner_ref is not None and not current.get('owners'):
        patch.append({'op': 'add', 'path': '/owners', 'value': [owner_ref]})
    return patch


def describe(kind, name, exists, apply):
    print(f'{"SKIP" if exists else ("CREATE" if apply else "PLAN CREATE")} {kind}: {name}')


def create_if_absent(client, endpoint, name, body, apply):
    current = client.named(endpoint, name)
    describe(endpoint, name, current is not None, apply)
    if current is None and apply:
        client.request('POST', endpoint, body)


def preflight(client, spec):
    """Validate target existence BEFORE any write, so wrong ingestion FQNs are safe."""
    current = {}
    owners = {}
    for table in spec['tables']:
        fqn = table['fqn']
        asset = client.named('tables', fqn, fields='columns,tags,owners')
        if asset is None:
            raise CatalogError(f'Table not found. Run Step 42 first: {fqn}')
        if asset.get('fullyQualifiedName') != fqn:
            raise CatalogError(f'Unexpected table identity for {fqn}')
        current[fqn] = asset
        owner = table.get('owner')
        if owner:
            kind = 'teams' if owner['type'] == 'team' else 'users'
            ent = client.named(kind, owner['name'])
            if not ent or 'id' not in ent:
                raise CatalogError(f'Owner must exist in OpenMetadata: {owner["name"]}')
            owners[fqn] = {'id': ent['id'], 'type': owner['type']}
        annotation_patch(table, asset, owners.get(fqn), spec['glossary']['name'])
    return current, owners


def run(client, spec, apply):
    current, owners = preflight(client, spec)
    c = spec['classification']
    create_if_absent(client, 'classifications', c['name'],
                     {'name': c['name'], 'description': c['description']}, apply)
    for tag in c.get('tags', []):
        name = c['name'] + '.' + tag['name']
        create_if_absent(client, 'tags', name, {'name': tag['name'], 'classification': c['name'],
                                               'description': tag['description']}, apply)
    g = spec['glossary']
    create_if_absent(client, 'glossaries', g['name'],
                     {'name': g['name'], 'description': g['description']}, apply)
    for term in g.get('terms', []):
        name = g['name'] + '.' + term['name']
        create_if_absent(client, 'glossaryTerms', name,
                         {'name': term['name'], 'glossary': g['name'],
                          'description': term['description']}, apply)
    changed = 0
    for t in spec['tables']:
        fqn = t['fqn']
        # Refresh immediately before write so changes added since preflight remain visible.
        actual = (client.named('tables', fqn, fields='columns,tags,owners') if apply else current[fqn])
        patch = annotation_patch(t, actual, owners.get(fqn), spec['glossary']['name'])
        if not patch:
            print(f'SKIP table: {fqn} (already annotated)')
            continue
        print(f'{"PATCH" if apply else "PLAN PATCH"} table: {fqn} ({len(patch)} operations)')
        if apply:
            client.request('PATCH', 'tables/' + quote(actual['id'], safe=''), patch)
        changed += 1
    print(('APPLIED' if apply else 'PLAN OK') + f': {len(spec["tables"])} table(s), {changed} requiring update')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy', required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--token-file', required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--plan', action='store_true')
    mode.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if os.geteuid() != 0:
        print('Run as root; JWT file is root-only', file=sys.stderr)
        return 1
    try:
        policy_file = Path(args.policy)
        if policy_file.is_symlink():
            raise CatalogError('Refusing symlink policy file')
        spec = validate(yaml.safe_load(policy_file.read_text(encoding='utf-8')))
        run(Client(args.url, credential(args.token_file)), spec, args.apply)
        return 0
    except (CatalogError, ValueError, KeyError, TypeError, OSError, yaml.YAMLError) as err:
        print('GOVERNANCE FAILED: ' + str(err), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

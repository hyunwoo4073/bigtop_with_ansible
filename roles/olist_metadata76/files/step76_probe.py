#!/usr/bin/env python3
"""Read-only topology and health discovery for Step76.

This script intentionally NEVER creates, modifies or drops any databases,
HDFS paths, Docker volumes or application services.
"""
import json
import os
import re
import shutil
import subprocess
import sys


def run(argv, timeout=25, env=None):
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=env,
                           check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise RuntimeError('command unavailable or timed out: ' + argv[0]) from e
    if p.returncode != 0:
        raise RuntimeError('command failed: ' + argv[0] + ' (exit=' + str(p.returncode) + ')')
    return p.stdout.strip()


def disk_info(path):
    u = shutil.disk_usage(path)
    return {'location': path, 'free_mib': u.free // 1048576, 'total_mib': u.total // 1048576}


def pg(database):
    if not re.fullmatch(r'[a-z_][a-z0-9_]*', database):
        raise ValueError('invalid PostgreSQL database name')
    available = run(['psql', '-X', '-A', '-t', '-d', 'postgres', '-c',
                     "SELECT datname FROM pg_database WHERE datname='" + database + "'"])
    if available != database:
        raise RuntimeError('expected catalog database is absent')
    tables = run(['psql', '-X', '-A', '-t', '-d', database, '-c',
                  "SELECT count(*) FROM pg_catalog.pg_tables "
                  "WHERE schemaname NOT IN ('pg_catalog','information_schema')"])
    size_bytes = run(['psql', '-X', '-A', '-t', '-d', database, '-c',
                      'SELECT pg_database_size(current_database())'])
    version = run(['pg_dump', '--version'])
    return {'component': 'catalog_postgresql', 'database': database,
            'size_bytes': int(size_bytes), 'user_table_count': int(tables),
            'pg_dump_version': version, 'storage': disk_info('/var'),
            'ready_for_backup': int(tables) > 0}


def docker_id(project, service):
    ids = run(['docker', 'ps', '-q', '--filter',
               'label=com.docker.compose.project=' + project,
               '--filter', 'label=com.docker.compose.service=' + service]).splitlines()
    if len(ids) != 1:
        raise RuntimeError('expected exactly one running ' + service + ' container; got ' + str(len(ids)))
    return ids[0]


def docker_summary(container_id):
    doc = json.loads(run(['docker', 'inspect', container_id], timeout=30))[0]
    mounts = [
        {'destination': m.get('Destination'), 'type': m.get('Type'),
         'volume': m.get('Name') if m.get('Type') == 'volume' else None}
        for m in doc.get('Mounts', [])
    ]
    return {'status': doc.get('State', {}).get('Status'),
            'health': doc.get('State', {}).get('Health', {}).get('Status', 'not-configured'),
            'mounts': mounts}


def mysql(project, database):
    if not re.fullmatch(r'[a-z_][a-z0-9_]*', database):
        raise ValueError('invalid MySQL database name')
    cid = docker_id(project, 'mysql')
    info = docker_summary(cid)
    mysql_command = ('MYSQL_PWD="$MYSQL_PASSWORD" exec mysql '
                     '--protocol=TCP -h 127.0.0.1 -u "$MYSQL_USER" -N -B '
                     '-D "$MYSQL_DATABASE" -e '
                     "'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()'")
    count = run(['docker', 'exec', cid, 'sh', '-c', mysql_command], timeout=40)
    dump_version = run(['docker', 'exec', cid, 'sh', '-c', 'mysqldump --version'])
    # The DB specified by the container must be the one we intend to protect.
    actual_name = run(['docker', 'exec', cid, 'sh', '-c', 'printf %s "$MYSQL_DATABASE"'])
    if actual_name != database:
        raise RuntimeError('OpenMetadata MySQL database name does not match expected')
    # Volume is Docker-managed; no secret env or host paths are emitted.
    mysql_volumes = [m for m in info['mounts'] if m['destination'] == '/var/lib/mysql']
    if not mysql_volumes:
        raise RuntimeError('MySQL persistent data mount not found')
    return {'component': 'openmetadata_mysql', 'compose_project': project,
            'database': database, 'user_table_count': int(count),
            'mysqldump_version': dump_version,
            'container': {'status': info['status'], 'health': info['health']},
            'persistent_mounts': mysql_volumes, 'storage': disk_info('/var'),
            'ready_for_backup': int(count) > 0}


def elasticsearch(project):
    es_id = docker_id(project, 'elasticsearch')
    app_id = docker_id(project, 'openmetadata-server')
    es_info = docker_summary(es_id)
    app_info = docker_summary(app_id)
    es_volumes = [m for m in es_info['mounts'] if m['destination'] == '/usr/share/elasticsearch/data']
    if not es_volumes:
        raise RuntimeError('Elasticsearch persistent data mount not found')
    data = json.loads(run(['docker', 'exec', es_id, 'sh', '-c',
                           'curl -fsS --max-time 5 http://127.0.0.1:9200/_cluster/health'], timeout=15))
    return {'component': 'openmetadata_search',
            'elasticsearch': {'status': es_info['status'], 'health': es_info['health'],
                              'cluster_health': data.get('status'),
                              'nodes': data.get('number_of_nodes'),
                              'persistent_mounts': es_volumes},
            'openmetadata_server': {'status': app_info['status'], 'health': app_info['health']},
            'note': 'Elasticsearch is a separate search index. MySQL dump is NOT a complete DR backup.'}


def hdfs(warehouse):
    if not warehouse.startswith('/data/olist/') or '..' in warehouse.split('/'):
        raise ValueError('warehouse outside approved HDFS prefix')
    # Script is staged as root to avoid Ansible unprivileged-become temp-file ACL errors.
    # Perform HDFS checks using the original Hadoop identity, not root.
    hdfs_cmd = ['runuser', '-u', 'hadoop', '--', 'hdfs', 'dfs']
    try:
        run(hdfs_cmd + ['-test', '-d', warehouse], timeout=25)
    except RuntimeError:
        return {'component': 'iceberg_hdfs_warehouse', 'path': warehouse,
                'exists': False, 'ready_for_inventory': False,
                'note': 'HDFS warehouse missing or inaccessible; confirm current catalog location.'}
    du = run(hdfs_cmd + ['-du', '-s', warehouse], timeout=60)
    cols = du.split()
    if len(cols) < 3:
        raise RuntimeError('unexpected HDFS du output')
    return {'component': 'iceberg_hdfs_warehouse', 'path': warehouse,
            'exists': True, 'logical_bytes': int(cols[0]),
            'replicated_bytes': int(cols[1]), 'ready_for_inventory': True,
            'note': 'Warehouse usage alone does not capture every Iceberg table metadata file location.'}


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in {'pg', 'mysql', 'search', 'hdfs'}:
        print('usage: step76_probe.py pg|mysql|search|hdfs ARG', file=sys.stderr)
        return 2
    component, target = sys.argv[1:]
    try:
        if component == 'pg':
            report = pg(target)
        elif component == 'mysql':
            report = mysql('bigtop-openmetadata', target)
        elif component == 'search':
            report = elasticsearch('bigtop-openmetadata')
        else:
            report = hdfs(target)
        report['ok'] = bool(report.get('ready_for_backup', report.get('ready_for_inventory', True)))
        print(json.dumps(report, sort_keys=True, ensure_ascii=False))
        return 0 if report['ok'] else 1
    except (RuntimeError, ValueError, OSError, json.JSONDecodeError) as e:
        print(json.dumps({'component': component, 'ok': False, 'error': str(e)},
                         ensure_ascii=False))
        return 1


if __name__ == '__main__':
    sys.exit(main())

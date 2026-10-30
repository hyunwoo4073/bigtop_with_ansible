#!/usr/bin/env python3
"""Run official OpenMetadata external PostgreSQL metadata-only ingestion.

No application DB port exposure or extra Airflow required. Secrets are read on
worker3 only; the temporary workflow is root-only and removed after execution.
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import yaml


def read_secret(path: Path):
    if path.is_symlink() or not path.is_file():
        raise RuntimeError('Required protected credential file missing or symlink')
    if path.stat().st_mode & 0o077:
        raise PermissionError('Secret file permissions must be 0600')
    value = path.read_text(encoding='utf-8').strip()
    if not value:
        raise ValueError('Empty credential file')
    return value


def build_config(base, password, token):
    config = yaml.safe_load(base)
    if config['source']['type'] != 'postgres' or config['source']['sourceConfig']['config']['type'] != 'DatabaseMetadata':
        raise ValueError('Refusing non-metadata PostgreSQL workflow')
    # OpenMetadata 2.0.x generated PostgresConnection schema requires 'Postgres',
    # not the legacy spelling 'PostgreSQL'. Reject it before invoking the CLI.
    if config['source']['serviceConnection']['config']['type'] != 'Postgres':
        raise ValueError('Invalid PostgreSQL connection type: expected Postgres')
    config['source']['serviceConnection']['config']['authType'] = {'password': password}
    om = config['workflowConfig']['openMetadataServerConfig']
    om['securityConfig'] = {'jwtToken': token}
    om['storeServiceConnection'] = False
    return config


def run(workflow, token_file, password_file, cli):
    if os.geteuid() != 0:
        raise PermissionError('Run as root on worker3')
    base = Path(workflow).read_text(encoding='utf-8')
    password = read_secret(Path(password_file))
    token = read_secret(Path(token_file))
    if len(token.split('.')) != 3:
        raise ValueError('Bot token is not a JWT')
    config = build_config(base, password, token)
    with tempfile.TemporaryDirectory(prefix='om-pg-catalog-', dir='/run') as directory:
        path = Path(directory) / 'workflow.yaml'
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8') as file:
            yaml.safe_dump(config, file, sort_keys=False, allow_unicode=True)
        result = subprocess.run([cli, 'ingest', '-c', str(path)], check=False, timeout=1700)
    return result.returncode


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--workflow', required=True)
    p.add_argument('--token', required=True)
    p.add_argument('--password', required=True)
    p.add_argument('--cli', required=True)
    a = p.parse_args()
    try:
        code = run(a.workflow, a.token, a.password, a.cli)
        print('CATALOG INGESTION OK' if code == 0 else 'CATALOG INGESTION FAILED')
        return code
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as err:
        # Deliberately do not print secret-bearing CLI configs or exception tracebacks.
        print('CATALOG INGESTION FAILED: ' + type(err).__name__, file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())

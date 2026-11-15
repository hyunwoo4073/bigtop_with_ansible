#!/usr/bin/env python3
"""Keep the Ansible-managed OpenMetadata Compose template aligned with D4.

Only modifies the known Elasticsearch service anchors. Safe to re-run.
"""
from pathlib import Path

TEMPLATE = Path('roles/governance_openmetadata/templates/compose.yaml.j2')
REPO = '/usr/share/elasticsearch/snapshots'
HOST = '/var/backups/bigtop-step76-es'


def patch(text: str) -> str:
    start = text.index('\n  elasticsearch:\n')
    end = text.index('\n  execute-migrate-all:', start)
    original = text[start:end]
    block = original
    env = "      ES_JAVA_OPTS: '-Xms768m -Xmx768m'"
    volume = '      - es-data:/usr/share/elasticsearch/data'
    if env not in block or volume not in block:
        raise ValueError('Unexpected Step41 OpenMetadata template: manual review required')
    setting = f'      path.repo: {REPO}'
    mount = f'      - {HOST}:{REPO}'
    if 'path.repo:' in block and setting not in block:
        raise ValueError('Existing path.repo is different; refusing overwrite')
    if f':{REPO}' in block and mount not in block:
        raise ValueError('Existing snapshot mount is different; refusing overwrite')
    if setting not in block:
        block = block.replace(env, env + '\n' + setting, 1)
    if mount not in block:
        block = block.replace(volume, volume + '\n' + mount, 1)
    return text[:start] + block + text[end:]


def main():
    if not TEMPLATE.is_file():
        raise SystemExit(f'Missing {TEMPLATE}. Live service is configured, but update template manually.')
    source = TEMPLATE.read_text()
    new = patch(source)
    if new == source:
        print('STEP76_ES_TEMPLATE_ALREADY_ALIGNED')
        return
    backup = TEMPLATE.with_name(TEMPLATE.name + '.step76d4.bak')
    if backup.exists():
        raise SystemExit(f'Backup path already exists: {backup}; refusing overwrite')
    backup.write_text(source)
    TEMPLATE.write_text(new)
    print('STEP76_ES_TEMPLATE_PATCH_PASS')

if __name__ == '__main__':
    main()

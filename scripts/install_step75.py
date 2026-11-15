#!/usr/bin/env python3
"""Non-destructive Step75 repo installer; only narrow patches to existing files."""
import argparse
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
NEW_FILES = [
    'playbooks/75-kafka-operations-ui.yml',
    'roles/web_console_kafka75/defaults/main.yml',
    'roles/web_console_kafka75/tasks/main.yml',
    'roles/web_console_kafka75/tasks/preflight.yml',
    'roles/web_console_kafka75/tasks/apply.yml',
    'roles/web_console_kafka75/tasks/verify.yml',
    'roles/web_console_kafka75/handlers/main.yml',
    'roles/web_console/files/kafka_ops75.py',
    'roles/web_console/files/frontend/src/KafkaHealthPanel75.jsx',
    'roles/web_console/files/frontend/src/kafka-health75.css',
    'scripts/run_step75.sh',
    'scripts/install_step75.py',
    'tests/test_step75_cluster_health.py',
    'docs/75-kafka-operations-ui.md',
]
PATCHES = {
    'roles/web_console/files/kafka_ops70.py': (
        "@router.get('/api/ops/kafka/overview')",
        "\n\n# Step75: only add a nested read-only router; retain all Step70 endpoints.\n"
        "from kafka_ops75 import router as kafka_ops75_router\n"
        "router.include_router(kafka_ops75_router)\n",
        'router.include_router(kafka_ops75_router)',
    ),
    'roles/web_console/files/frontend/src/KafkaOperationsView.jsx': (
        'export default function KafkaOperationsView',
        "import KafkaHealthPanel75 from './KafkaHealthPanel75'\n",
        'import KafkaHealthPanel75',
    ),
}


def patch_frontend(source):
    needle = '      <section className="card">\n        <h3>Kafka Brokers · TCP connectivity</h3>'
    if needle not in source:
        raise ValueError('Step70 Kafka Brokers section anchor not found')
    return source.replace(needle, '      <KafkaHealthPanel75 />\n' + needle, 1)


def prepare_changes(repo):
    changes = {}
    for rel in NEW_FILES:
        src, dst = ROOT / rel, repo / rel
        if not src.is_file():
            raise ValueError(f'Missing payload: {rel}')
        data = src.read_bytes()
        if dst.exists() and dst.read_bytes() != data:
            raise ValueError(f'Refusing to overwrite an existing differing file: {rel}')
        if not dst.exists():
            changes[rel] = data
    for rel, (anchor, marker, sentinel) in PATCHES.items():
        dst = repo / rel
        if not dst.is_file():
            raise ValueError(f'Missing Step70 base source: {rel}')
        current = dst.read_text()
        if sentinel in current:
            if rel.endswith('KafkaOperationsView.jsx') and '<KafkaHealthPanel75 />' not in current:
                raise ValueError(f'Partial Step75 frontend patch found: {rel}')
            continue
        if anchor not in current:
            raise ValueError(f'Cannot safely patch unknown Step70 revision: {rel}')
        if rel.endswith('KafkaOperationsView.jsx'):
            updated = marker + current
            updated = patch_frontend(updated)
        else:
            updated = current.rstrip('\n') + '\n' + marker
        changes[rel] = updated.encode()
    return changes


def install(repo):
    changes = prepare_changes(repo)
    if not changes:
        print('STEP75_INSTALL_ALREADY_APPLIED')
        return
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    backup_dir = repo / 'artifacts' / 'step75' / f'source-backups-{timestamp}'
    for rel in changes:
        dst = repo / rel
        if dst.exists():
            backup = backup_dir / rel
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dst, backup)
    for rel, data in changes.items():
        dst = repo / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(data)
        print(f'UPDATED {rel}')
    print(f'STEP75_INSTALL_PASS backups={backup_dir}')
    print('Next: make validate; bash scripts/run_step75.sh preflight')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('repo', type=Path)
    args = parser.parse_args()
    repo = args.repo.resolve()
    if not (repo / 'playbooks/70-kafka-web-console.yml').is_file():
        sys.exit('Not a Step70-enabled Bigtop repo')
    try:
        install(repo)
    except (OSError, ValueError) as exc:
        sys.exit(str(exc))


if __name__ == '__main__':
    main()

#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_BIN="${PROJECT_ROOT}/.venv/bin"

if [[ -d "${VENV_BIN}" ]]; then
  export PATH="${VENV_BIN}:${PATH}"
fi

echo "==> Check required commands"

required_commands=(
  git
  python3
  ansible
  ansible-playbook
  ansible-inventory
  yamllint
  ansible-lint
)

for cmd in "${required_commands[@]}"; do
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: required command not found: ${cmd}"
    echo
    echo "Install dev tools with:"
    echo "  python3 -m pip install --user -r requirements-dev.txt"
    exit 1
  fi
done

echo
echo "==> Check ignored sensitive/runtime files are not tracked"

tracked_sensitive_files="$(
  git ls-files \
    inventory/hosts.ini \
    inventory/group_vars/all.yml \
    artifacts \
    2>/dev/null || true
)"

if [[ -n "${tracked_sensitive_files}" ]]; then
  echo "ERROR: sensitive or runtime files are tracked by Git:"
  echo "${tracked_sensitive_files}"
  echo
  echo "Remove from Git index with:"
  echo "  git rm --cached inventory/hosts.ini inventory/group_vars/all.yml"
  echo "  git rm -r --cached artifacts/"
  exit 1
fi

echo "OK: sensitive/runtime files are not tracked"

echo
echo "==> Check duplicate top-level keys in group_vars examples"

python3 - <<'PY'
from pathlib import Path
from collections import Counter
import sys

files = [
    Path("inventory/group_vars/all.yml.example"),
]

failed = False

for file in files:
    if not file.exists():
        print(f"ERROR: missing file: {file}")
        failed = True
        continue

    keys = []
    for line_no, line in enumerate(file.read_text().splitlines(), 1):
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        if line.startswith(" ") or line.startswith("-"):
            continue

        if ":" in line:
            key = line.split(":", 1)[0].strip()
            keys.append((key, line_no))

    counts = Counter(key for key, _ in keys)

    duplicates = []
    for key, count in counts.items():
        if count > 1:
            lines = [str(line_no) for k, line_no in keys if k == key]
            duplicates.append((key, lines))

    print(f"\n{file}")
    if duplicates:
        failed = True
        for key, lines in duplicates:
            print(f"  duplicate: {key} -> lines {', '.join(lines)}")
    else:
        print("  no duplicate top-level keys")

if failed:
    sys.exit(1)
PY

echo
echo "==> Check Ansible inventory graph"

ansible-inventory --graph >/tmp/bigtop-ansible-inventory-graph.txt
cat /tmp/bigtop-ansible-inventory-graph.txt

echo
echo "==> Run yamllint"

yamllint \
  .yamllint \
  .ansible-lint \
  inventory/group_vars/all.yml.example \
  playbooks \
  roles \
  docs

echo
echo "==> Run ansible-lint"

ansible-lint

echo
echo "==> Run ansible-playbook syntax checks"

for playbook in playbooks/*.yml; do
  echo
  echo "syntax-check: ${playbook}"
  ansible-playbook "${playbook}" --syntax-check
done

echo
echo "OK: static validation completed"

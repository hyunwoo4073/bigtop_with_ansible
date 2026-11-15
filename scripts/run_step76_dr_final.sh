#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# No `make validate` here; CI or end-of-day Git workflow handles full static validation.
exec python3 scripts/step76_dr_final.py

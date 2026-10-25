# Bigtop Web Console — PostgreSQL action history (Ansible)

## Goal

Declarative provisioning using `roles/web_console_postgres` and `playbooks/31-web-console-postgres.yml`.
The existing `web_console` role remains responsible for backend/React/Nginx deployment.
PostgreSQL on worker3 owns action history only. Existing `actions.log` is preserved as a durability fallback.

## First-time controller dependencies

```bash
.venv/bin/ansible-galaxy collection install -r collections/requirements-web-console-postgres.yml
```

If the controller has `ansible-galaxy` directly instead of in `.venv/bin`, use it there.
Add `community.postgresql` to the project's `collections/requirements.yml` for normal future `make collections` runs.

## Source integration

Before deployment, the Git-tracked `roles/web_console/templates/app.py.j2` **must** contain the PostgreSQL hooks (`WEB_CONSOLE_DB_DSN`, `api_storage_health`, `_db_write_action`).
Apply the included `patches/web_console_pg_backend.patch` once to the working tree (if not already integrated), review it, and commit it. The source patch is a versioned application change, **not a recurring infrastructure setup operation**.
Do not overwrite any newer `app.py.j2` blindly. Verify `git apply --check` first.

## Repeatable deployment

```bash
cd ~/bigtop-cluster-ansible
.venv/bin/ansible-playbook playbooks/31-web-console-postgres.yml -e ansible_connection=local
```

The above single playbook safely repeats: install PostgreSQL, preserve/create credentials, ensure role/database/schema, deploy existing Web Console, install psycopg driver, configure systemd EnvironmentFile, import missing JSONL records, enable a periodic reconciliation timer, and verify API.

Important: `ansible_connection=local` is for execution **on worker3** only. When invoking from a different controller, use SSH as normal and do not set local.

## Verify

```bash
sudo systemctl is-active postgresql bigtop-web-console
sudo systemctl list-timers --all | grep bigtop-web-console-history-sync
curl -fsS http://127.0.0.1:18090/api/storage/health | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/actions/history | python3 -m json.tool
```

## Safety / recovery

- No DROP commands, no credential rotation on repeated run.
- Credentials stored only on worker3 under `/etc/bigtop-web-console/`; never commit real secret files.
- Database listens on PostgreSQL defaults (typically loopback), not opened to external clients by this role.
- The JSONL audit log is not deleted; its rows are imported with stable event IDs and `ON CONFLICT DO NOTHING`.
- Periodic sync reconciles events recorded while DB was temporarily down.
- Backup PostgreSQL database separately (`pg_dump`) before schema upgrades; idempotent DDL is not a full migration strategy. Add Alembic/SQL migrations for future changes.
- Current action audit data relies on Nginx Basic Auth; per-user identity and stronger access control are future work.
- Review PostgreSQL `pg_hba.conf` if TCP authentication test fails. Do not weaken remote access controls.

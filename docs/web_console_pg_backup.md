# Bigtop Web Console: PostgreSQL backup/restore validation

## Deploy

Run from worker3, the Ansible controller and PostgreSQL host:

```bash
make lint
.venv/bin/ansible-playbook playbooks/34-web-console-postgres-backup.yml -e ansible_connection=local
```

This role only configures the backup; it does not change, replace, or restart the application database.
It **requires** an existing local PostgreSQL database (`bigtop_console` by default).

Defaults: daily at 03:30 **worker3 server-local time** (with up to 5 minutes random delay), 14 day local retention, custom-format compressed dump. Archives are owned by `postgres`, mode 0600, under `/var/backups/bigtop-web-console/postgresql/` (0700).

## Initial backup and verification

```bash
sudo systemctl start bigtop-web-console-pg-backup.service
sudo journalctl -u bigtop-web-console-pg-backup.service -n 30 --no-pager
sudo -u postgres /usr/local/lib/bigtop-web-console/pg_restore_check.sh
sudo systemctl list-timers --all | grep bigtop-web-console-pg-backup
```

The restore check creates a disposable PostgreSQL DB, restores the latest archive, verifies the public table count, then drops **only that disposable DB**. No production data is modified. If cleanup fails, the command warns you with the temporary database name.

If `pg_restore` fails or the backup is unexpectedly small, the backup command exits nonzero and does **not** purge older backups.

## Configure time and retention

Override variables in inventory `group_vars`, not by editing managed files:

```yaml
web_console_pg_backup_calendar: '*-*-* 02:30:00'
web_console_pg_backup_retention_days: 21
```

Reapply Playbook 34. OnCalendar uses the worker3 server's timezone; verify with `timedatectl`.

## Recovery caution

The restore verifier never restores into `bigtop_console`. An actual production restore should be a separate, manually approved incident procedure: stop writers, take a fresh emergency backup, restore into a new database, validate, then plan a controlled switch. Do not run `pg_restore --clean` against the live database.

**Limits:** Local backups are not disaster recovery. Add off-host encrypted replication and monitoring of backup age/size in a later iteration. A custom-format dump is not a complete PostgreSQL cluster backup (global roles/configs are not included).

## Source-control policy

Commit `roles/web_console_pg_backup/`, `playbooks/34-web-console-postgres-backup.yml`, and this doc. Do **not** commit files from `/var/backups` or credentials from `/etc/bigtop-web-console`.

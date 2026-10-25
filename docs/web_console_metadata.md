# Bigtop Web Console — PostgreSQL metadata catalog, phase 1

## Source of truth

- Host inventory: Ansible `groups['all']`, inventory alias, `ansible_host`, group memberships.
- Service inventory: **deployed** Web Console `/opt/bigtop-web-console/app.py`, static `COMPONENTS` list.
- Database: PostgreSQL **read model**, **not** the authoritative service registry yet.
- No modifications to Hadoop services, React UI, FastAPI routes or existing Action History tables.

## Install/update (worker3)

```bash
cd ~/bigtop-cluster-ansible
unzip -n web_console_metadata_ansible.zip -d .
make lint
.venv/bin/ansible-playbook playbooks/32-web-console-metadata.yml -e ansible_connection=local
```

Use the same playbook for later deployments. `31-web-console-postgres.yml` is imported at the beginning; metadata synchronization happens only after Web Console + DB health checks succeed. Run with local connection **only when the controller is worker3**.

## Verify

```bash
# No secrets in output; only an inventory/catalog validation
sudo /opt/bigtop-web-console/.venv/bin/python \
  /usr/local/lib/bigtop-web-console/sync_metadata.py \
  --inventory /var/lib/bigtop-web-console/metadata-inventory.json \
  --app /opt/bigtop-web-console/app.py \
  --schema /usr/local/lib/bigtop-web-console/metadata_schema.sql \
  --dry-run

# DB read queries via local PostgreSQL admin account; no password printed
sudo -u postgres psql -d bigtop_console -c \
  'SELECT cluster_slug, host_name, inventory_address, is_present FROM platform_hosts ORDER BY host_name;'
sudo -u postgres psql -d bigtop_console -c \
  'SELECT cluster_slug, host_name, component_name, service_name, is_present FROM platform_service_instances ORDER BY host_name, service_name;'
sudo -u postgres psql -d bigtop_console -c \
  'SELECT cluster_slug, synced_at, host_count, service_count FROM platform_metadata_sync_runs ORDER BY id DESC LIMIT 5;'
```

## Safety properties

- Multiple runs use `INSERT ... ON CONFLICT DO UPDATE` and **never delete** metadata. Disappearing hosts/services are flagged `is_present=false`.
- Entire snapshot applies in a single PostgreSQL transaction. Missing inventory or unknown component host aborts before any database writes.
- Existing Action History and credentials remain untouched.
- Snapshot JSON contains only inventory names, ansible_host, and group memberships, **not** credentials or other hostvars.
- A service's runtime `active` state is not persisted here. This catalog records **declared metadata** only.
- First version uses CREATE TABLE IF NOT EXISTS. Future schema changes should use numbered migrations rather than destructive table recreation.

## Next phase

Add read-only `/api/metadata` and an Admin catalog view, then move the static service registry to a validated Ansible variable. Only after source-of-truth changes are designed should service control read from PostgreSQL.

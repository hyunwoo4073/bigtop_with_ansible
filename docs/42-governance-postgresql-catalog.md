# 42. OpenMetadata PostgreSQL Data Catalog (worker3 external ingestion)

## Design

- OpenMetadata **2.0.6** is already running on `metadata` VM (`192.158.2.104:8585`).
- The existing `bigtop_console` database listens on **worker3 loopback `127.0.0.1:5432`**.
- External ingestion runs locally on worker3 in a dedicated Python virtual environment, using `openmetadata-ingestion[postgres]==2.0.6.0`.
- **No Airflow ingestion container, no published PostgreSQL port, no SSH access from metadata to Hadoop**.
- The PostgreSQL catalog reader has `CONNECT`, `USAGE` and `SELECT` privileges, **not** DML or DDL privileges. Note: `SELECT` technically allows reading rows, although the workflow ingests metadata only.
- Catalogs the `bigtop_console.public` schemas, tables, columns and views; it does **not** run row profiling, sample data, usage or lineage.
- The OpenMetadata API Bot JWT is created in UI; not hard-coded into Ansible, Git, or shell history.
- Source credential storage in OpenMetadata is disabled (`storeServiceConnection: false`).
- Destructive `markDeleted*` flags disabled for initial ingestion.
- Optional daily systemd Timer is disabled by default until manual validation succeeds.

## Deploy (worker3)

```bash
cd ~/bigtop-cluster-ansible
unzip -o 42_governance_pg_catalog_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_governance_pg_catalog.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/42-governance-postgres-catalog.yml
```

Do not pass `-e ansible_connection=local` to the entire cluster. This Playbook is scoped to the existing `[ops]` worker3 only. The PostgreSQL reader password is generated once in a mode-0600 local root file; repeated deployments preserve it.

## One-time Bot token

Open the OpenMetadata UI at `http://192.158.2.104:8585` and go to **Settings → Bots → ingestion-bot → Generate Token**. Use a dedicated ingestion bot that can write metadata entities; do **not** paste the token into the shell command, `hosts.ini`, Ansible variables, or Git.

On worker3:

```bash
sudo python3 /usr/local/lib/bigtop-governance/pg-catalog/install_token.py
```

The program will prompt for the JWT without echoing it. For deliberate token rotation:

```bash
sudo python3 /usr/local/lib/bigtop-governance/pg-catalog/install_token.py --replace
```

## First ingestion

```bash
sudo systemctl start bigtop-governance-pg-catalog.service
sudo systemctl status bigtop-governance-pg-catalog.service --no-pager
sudo journalctl -u bigtop-governance-pg-catalog.service -n 80 --no-pager
```

The service is `Type=oneshot`: `inactive (dead)` **after a successful run is normal**; inspect systemd exit status or `CATALOG INGESTION OK` instead of requiring `active`.

In OpenMetadata UI navigate to **Explore → Databases → bigtop_worker3_postgresql → bigtop_console → public**. Verify columns on `platform_hosts` and `platform_service_instances` (after metadata sync 32), or other tables already present.

## Optional scheduling

Only after successful manual ingestion, enable the timer declaratively in Inventory group_vars (not a one-shot `systemctl enable`):

```yaml
governance_postgres_catalog_enable_timer: true
governance_postgres_catalog_timer_calendar: '*-*-* 04:30:00'
```

Then re-run Playbook 42. This runs daily at 04:30, worker3 local time. No HTTP request from the Web Console is required.

## Troubleshooting

- `401/403`: Bot JWT expired, wrong role or revoked; rotate the token with `--replace`.
- `500` ingestion: check `sudo journalctl -u bigtop-governance-pg-catalog.service -n 120 --no-pager`.
- Postgres auth error: confirm `psql -h 127.0.0.1 -d bigtop_console` on worker3, `postgresql` is active, and reader role grants.
- Cannot reach UI from worker3: `curl -I http://192.158.2.104:8585/`.
- Ingestion CLI install dependency error: view the failed `pip` Ansible task; VM may need free memory/disk.
- Don't expose 5432 to the network to fix failures; metadata collection is local to worker3.
- Avoid `docker compose down -v` on the metadata VM: its MySQL and Elasticsearch volumes contain the catalog.

This is a development PoC; TLS and secret management should be added before real use. OpenMetadata Bot JWTs are credentials even though the server is on a lab network.

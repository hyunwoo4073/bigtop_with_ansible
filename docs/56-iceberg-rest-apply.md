# 56 — Apply: Iceberg REST Catalog with PostgreSQL backend + HDFS

Commands run on **worker3**, in `~/bigtop-cluster-ansible`.

## 1. Extract and validate package

```bash
unzip -o 56_iceberg_rest_catalog_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_iceberg_rest_catalog.py' -v
make lint
```

## 2. Get offline/reproducible vendor assets

```bash
python3 scripts/56-fetch-iceberg-rest-assets.py
python3 scripts/56-verify-artifacts.py vendor/iceberg-rest
```

**Compatibility:** Previously documented `-e iceberg_rest_apply=true` and `-e iceberg_rest_spark_smoke=true` still work as aliases. Canonical role flags now use `iceberg_rest_catalog_` prefix to satisfy `ansible-lint` rules.

This fetches: pinned Gravitino standalone 1.3.1 (~100 MB), PostgreSQL JDBC 42.7.11, and portable Eclipse Temurin JDK17. The downloader expects outbound HTTPS and official upstream endpoints. If worker3 is offline, run the same script from an internet-connected machine and copy the `vendor/iceberg-rest/` directory, including `.sha256` files, to worker3. Do not commit binaries.

If the Artifacts downloader fails, stop: do not proceed with a partially downloaded directory. Do not install system Java17 or use update-alternatives.

## 3. Plan (read-only)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/56-iceberg-rest-catalog.yml
```

Expected: `PLAN ONLY` and no changes.

## 4. Install separate REST service, DB and HDFS warehouse

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/56-iceberg-rest-catalog.yml \
  -e iceberg_rest_catalog_apply=true
```

The playbook does not alter existing Spark/Hadoop Java, PostgreSQL global listening settings, or existing Iceberg lab tables. It installs systemd unit `bigtop-iceberg-rest.service` on worker3, creates a PostgreSQL DB/user under the existing PostgreSQL, and creates HDFS `/data/olist/iceberg-rest` (as `hadoop`). The service binds to worker3 VM network address port 9001; access to that address must be limited to isolated VirtualBox host-only network.

## 5. Verify REST endpoint, health and database

```bash
systemctl status bigtop-iceberg-rest --no-pager -l
curl -fsS http://192.158.2.103:9001/iceberg/v1/config | python3 -m json.tool
curl -fsS http://192.158.2.103:9001/iceberg/v1/namespaces | python3 -m json.tool
sudo journalctl -u bigtop-iceberg-rest -n 100 --no-pager
sudo -u postgres psql -Atc "SELECT datname FROM pg_database WHERE datname = 'iceberg_rest56';"
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/iceberg-rest'
```

**Do not print** `/etc/bigtop-iceberg-rest/postgres.pass` or server config; they contain credentials.

## 6. Spark-on-YARN shared Catalog write/read (required end-to-end verification)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/56-iceberg-rest-catalog.yml \
  -e iceberg_rest_catalog_apply=true \
  -e iceberg_rest_catalog_spark_smoke=true
```

It creates an isolated `shared.olist56.catalog_smoke` Iceberg table, inserts one row on first execution only and reads it back. The `STEP56_REST_SPARK_PASS` line is in the **YARN Driver stdout**, not necessarily the spark-submit client output. Locate the Application ID from the playbook output and inspect aggregated YARN logs if needed. It reuses `--jars` from Step 53, leaving worker1/2 Spark global classpaths unmodified.

> If YARN fails, retrieve the application logs (`ssh master 'sudo -u hadoop yarn logs -applicationId APPLICATION_ID'`). If error originates in Gravitino's Hadoop dependencies, inspect server journal separately. Do NOT patch `/usr/lib/spark/jars` or change global Java versions.

## 7. Commit

```bash
git add playbooks/56-iceberg-rest-catalog.yml \
  roles/iceberg_rest_catalog/ scripts/56-*.py \
  tests/test_iceberg_rest_catalog.py docs/56-iceberg-rest-*.md

git diff --cached --check
git commit -m 'feat: add isolated Iceberg REST catalog backed by PostgreSQL and HDFS'
```

Make sure `vendor/` is excluded from Git. Stop here after verifying service and Spark integration. Trino belongs to Step 57.

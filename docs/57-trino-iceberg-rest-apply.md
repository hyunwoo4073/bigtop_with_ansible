# Step 57 — install Trino and verify the Step 56 Iceberg table

Do **not** delete, alter, or re-run the Step 53–56 Iceberg tables. ZIP extraction is relative to the Ansible project on `worker3`.

```bash
cd ~/bigtop-cluster-ansible
unzip -o 57_trino_iceberg_rest_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_trino_iceberg_rest.py' -v
make lint
```

The Trino server tarball and Temurin 25 JDK are **not** in the ZIP. Fetch and validate them first (worker3 needs internet access and significant free disk space):

```bash
python3 scripts/57-fetch-trino-assets.py
python3 scripts/57-verify-artifacts.py vendor/trino57
ls -lh vendor/trino57/
```

If worker3 cannot reach the Internet, run the fetch script on an Internet-connected x86_64 Linux system using the same project files and copy the whole `vendor/trino57/` directory, including manifest, to worker3. `verify-artifacts` uses the same SHA-256s offline.

Default is an unprivileged plan (no changes):

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/57-trino-iceberg-rest.yml
```

Apply the isolated Trino service **only** on `metadata`:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/57-trino-iceberg-rest.yml \
  -e trino_iceberg_rest_apply=true
```

Run the cross-engine read-only SQL proof separately on `worker3`:

```bash
python3 scripts/57-verify-trino.py --url http://192.158.2.104:8080
```

Or re-run only the smoke Play from Ansible without modifying services:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/57-trino-iceberg-rest.yml \
  --limit localhost -e trino_iceberg_rest_smoke=true
```

Expected final proof (from actual SQL data read, not merely catalog metadata):

```
STEP57_TRINO_INFO_OK: version=484
STEP57_ICEBERG_ROW_PASS: [[56, 'spark']]
STEP57_ICEBERG_COUNT_PASS: [[1]]
STEP57_TRINO_SPARK_CROSS_QUERY_PASS
```

**Diagnostics** (metadata VM):

```bash
ssh metadata 'systemctl status bigtop-trino57 --no-pager -l'
ssh metadata 'sudo journalctl -u bigtop-trino57 -n 120 --no-pager'
ssh metadata 'sudo tail -n 100 /var/lib/bigtop-trino57/data/var/log/server.log'
ssh metadata 'free -h; systemctl show bigtop-trino57 -p MemoryCurrent -p MemoryMax'
curl -fsS http://192.158.2.104:8080/v1/info | python3 -m json.tool
```

The `server.log` path may not exist until startup has progressed; journalctl is the first diagnostic source.

Troubleshooting:

- `STEP57_ARTIFACTS_INVALID`: run `57-fetch-trino-assets.py` and compare SHA-256; do not skip verification. If GitHub/Adoptium is unreachable, transfer preverified artifacts.
- `MemAvailable` below 3000 MiB: do **not** stop OpenMetadata automatically. Inspect Docker `docker stats --no-stream` and VM size; increase RAM or choose a dedicated VM before deploying.
- `wait_for DataNode 9866` timeout: confirm actual `dfs.datanode.address`/bind and network. A NameNode connection is insufficient for Parquet reads.
- `getent hosts master` fails: repair DNS/hosts mapping on `metadata`; package intentionally does not alter `/etc/hosts`.
- Trino server fails startup: check dedicated JDK path, `journalctl`, startup heap, and conflicting ports/config.
- Catalog listing succeeds but query fails: verify `fs.hadoop.enabled`, HDFS client XML, DataNode connectivity, and HDFS identity. Trino must read actual Parquet files.
- Unexpected Spark row: confirm Step 56 `shared.olist56.catalog_smoke` identity; script will not overwrite it.

After PASS, commit only the newly added `docs/`, `playbooks/57*`, `roles/trino_iceberg_rest/`, `scripts/57*`, `tests/test_trino_iceberg_rest.py`. Do **not** stage `vendor/` tarballs or JDK artifacts.

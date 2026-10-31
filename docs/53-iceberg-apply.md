# Step 53: apply / test / troubleshoot

All shell commands below run on `worker3`, from `~/bigtop-cluster-ansible` unless otherwise noted.

## 0. Merge ZIP and prerequisites

```bash
cd ~/bigtop-cluster-ansible
unzip -o 53_iceberg_spark35_hdfs_lab_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_iceberg_spark_lab.py' -v
make lint
ssh master 'spark-submit --version'
```

Expected Spark `3.5.6`, Scala `2.12.18`, Java 11. Iceberg `1.10.1` is the Java-11-compatible runtime; Iceberg `1.11.0+` requires Java 17. No attempt to install a second Spark or Java version.

## 1. Place pinned Iceberg Spark 3.5 / Scala 2.12 JAR on worker3

Preferred if worker3 internet access is available:

```bash
bash scripts/53-download-iceberg-runtime.sh
```

If worker3 cannot access Maven Central, download the jar on your Windows PC or another internet-connected machine, copy it to worker3 under:

```text
~/bigtop-cluster-ansible/vendor/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar
```

Maven Central original URL:

```text
https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar
```

For local/offline copies verify file and checksum:

```bash
ls -lh vendor/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar
sha256sum vendor/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar
python3 scripts/53-check-iceberg-java11.py vendor/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar
```

Git: do **not** commit the runtime JAR (`vendor/iceberg/*.jar` must remain untracked; add it to `.gitignore` if needed).

## 2. Deploy runtime only (safe default)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/53-iceberg-hdfs-lab.yml
```

Installs:
- `/opt/bigtop-iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar` on master
- `/opt/bigtop-iceberg/iceberg_hdfs_smoke.py` on master

This step does NOT start Spark or create Iceberg tables.

## 3. Run isolated Spark on YARN smoke and independent Mart copy

Prerequisites:
- Step52 completed for v3: `/data/olist/published/v3/_APPROVAL.json`
- v3 Mart exists and contains `_SUCCESS`.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/53-iceberg-hdfs-lab.yml \
  -e iceberg_spark_lab_run_smoke=true
```

Expected: YARN app success, print proof with `"status":"PASS"`, smoke 2 snapshots, first snapshot rows 1, latest rows 2, Mart copy equal row counts. `PLAY RECAP failed=0`.

On worker3:

```bash
ssh master 'sudo -u hadoop hdfs dfs -text /data/olist/iceberg-validation/step53/part-*'
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/iceberg-lab/olist'
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/iceberg-lab/olist/daily_category_sales_v3_lab/metadata'
```

Iceberg tables:

- `lab.olist.iceberg_smoke_step53`
- `lab.olist.daily_category_sales_v3_lab`

HDFS layout created by Iceberg has `metadata/` plus actual data files; user code does not create its own version-specific `v1/v2/v3/_ETL_MANIFEST` tree for this new catalog.

## 4. Read and check the same catalog in Spark SQL

The Iceberg JAR and settings must also be supplied to independent Spark jobs. The role configures the catalog **per job**, not system-wide. On `master`:

```bash
sudo -u hadoop spark-sql \
 --master yarn \
 --jars /opt/bigtop-iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.catalog.lab=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.lab.type=hadoop \
 --conf spark.sql.catalog.lab.warehouse=hdfs://master:9000/data/olist/iceberg-lab \
 -e 'SELECT * FROM lab.olist.iceberg_smoke_step53 ORDER BY id'
```

`spark-sql` often executes in YARN client mode; this query is for interactive inspection, not the main cluster-mode smoke. JAR must be readable by the submit user.

Further SQL queries (same configuration):

```sql
SELECT snapshot_id, committed_at, operation FROM lab.olist.iceberg_smoke_step53.snapshots ORDER BY committed_at;
SELECT COUNT(*) FROM lab.olist.daily_category_sales_v3_lab;
```

## 5. Troubleshooting

- `UnsupportedClassVersionError`: preflight must reject an Iceberg Java-17 JAR; use 1.10.1 and redeploy.
- `jar missing` on worker3: execute downloader or copy from Windows. ZIP deliberately excludes large binary JAR.
- `Failed to find data source: iceberg`, `ClassNotFound`: check correct 3.5_2.12 runtime JAR shipped in `--jars` and YARN driver logs.
- `Failed to create namespace`, `Permission denied`: verify HDFS `/data/olist/iceberg-lab` and submit user `hadoop` permissions.
- YARN AM failed: `ssh master "sudo -u hadoop yarn logs -applicationId <application_id> | tail -150"`.
- A run could create tables and then fail before proof output. **Rerun the same step** (idempotence expected) rather than deleting the catalog or creating an `iceberg-v2` path.
- `spark-sql -e` flags or class loading might depend on the Bigtop wrapper; the main verified path is `spark-submit` job.
- Ensure `50` and `51` manually registered OpenMetadata Container lineage are not assumed to automatically include the new Iceberg catalog. No ingestion job for that yet.

## 6. Git

```bash
git add playbooks/53-iceberg-hdfs-lab.yml roles/iceberg_spark_lab/ docs/53-iceberg-apply.md docs/53-iceberg-architecture.md tests/test_iceberg_spark_lab.py scripts/53-download-iceberg-runtime.sh vendor/iceberg/.gitkeep
git commit -m 'feat: add isolated Iceberg Spark 3.5 HDFS catalog pilot'
```

Do not include any downloaded JAR in commits.

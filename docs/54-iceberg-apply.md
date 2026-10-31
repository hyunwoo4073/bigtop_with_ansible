# 54 — Apply / validate on worker3

Preconditions: Step53 passed, master has `/opt/bigtop-iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar`, and the 53 proof exists in HDFS. Run commands from `~/bigtop-cluster-ansible` on worker3.

## 1. Deploy and test

```bash
unzip -o 54_iceberg_transactions_wap_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_iceberg_transactions_wap.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/54-iceberg-transactions-wap.yml
```

Default `deploy_only` stages the application but **does not run Spark or write any Iceberg table**.

## 2. Stage only — no publish

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/54-iceberg-transactions-wap.yml -e iceberg_transactions_lab_mode=stage
```

This runs SQL INSERT, UPDATE, DELETE, MERGE, ADD COLUMN and Time Travel on a separate `lab.olist.iceberg_mutations_step54`; and stages a candidate write on `lab.olist.iceberg_wap_step54.branch_audit_step54` only. Expected stage proof `status=STAGED`, `main_rows=1`, `audit_rows=2`.

```bash
ssh master "sudo -u hadoop hdfs dfs -text '/data/olist/iceberg-validation/step54/stage/part-*'"
```

**Do not proceed to publish unless the stage result is successful.** The stage proof records exact main/audit snapshot IDs and a synthetic row-level quality check was completed. The operation is idempotent and can be rerun after a partial failure.

## 3. Explicit, separately requested publish

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/54-iceberg-transactions-wap.yml -e iceberg_transactions_lab_mode=publish
```

Only after revalidating approved branch/main snapshot IDs and candidate quality will it fast-forward `main`.

```bash
ssh master "sudo -u hadoop hdfs dfs -text '/data/olist/iceberg-validation/step54/publish/part-*'"
```

Expected: `status=PASS`, `result=PUBLISHED` first time; `result=ALREADY_PUBLISHED` on retry; `main` and `audit` snapshot IDs equal and row count 2.

## 4. Review actual Iceberg metadata / SQL

Verify two isolated tables and metadata snapshots (this is not a production dataset):

```bash
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/iceberg-lab/olist/iceberg_mutations_step54/metadata'
ssh master 'sudo -u hadoop hdfs dfs -ls /data/olist/iceberg-lab/olist/iceberg_wap_step54/metadata'
```

Using the same Iceberg Spark catalog and runtime JAR from Step53, SQL examples:

```sql
SELECT id,label,amount,audit_note FROM lab.olist.iceberg_mutations_step54 ORDER BY id;
SELECT snapshot_id,operation,committed_at FROM lab.olist.iceberg_mutations_step54.snapshots;
SELECT name,type,snapshot_id FROM lab.olist.iceberg_wap_step54.refs;
SELECT * FROM lab.olist.iceberg_wap_step54 ORDER BY id;
SELECT * FROM lab.olist.iceberg_wap_step54.branch_audit_step54 ORDER BY id;
```

When a YARN job fails, retrieve its real application ID from the Ansible stderr then examine the Driver traceback:

```bash
ssh master 'sudo -u hadoop yarn logs -applicationId <actual_application_id>' > /tmp/iceberg54-yarn.log 2>&1
grep -nE 'Traceback|Caused by:|Exception|ERROR|AnalysisException' /tmp/iceberg54-yarn.log | tail -60
```

**Do not delete or recreate the table** on a partial failure: the stage checks expected intermediate state and can be retried. If a table was modified outside this lab and fails safety checks, stop rather than reset it.

## 5. Git

```bash
git add playbooks/54-iceberg-transactions-wap.yml \
  roles/iceberg_transactions_lab/ \
  tests/test_iceberg_transactions_wap.py \
  docs/54-iceberg-apply.md docs/54-iceberg-architecture.md
git commit -m 'feat: exercise Iceberg transactions and gated WAP publication'
```

The package contains no JAR and changes no global Spark configuration.

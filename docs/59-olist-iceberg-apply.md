# 59 — Apply, audit and publish

Run on **worker3** in `~/bigtop-cluster-ansible` after Step58 and Trino cross-query PASS.

## Install package and run checks

```bash
unzip -o 59_olist_iceberg_gold_wap_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_iceberg_wap.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/59-olist-iceberg-gold-wap.yml
```

`plan` is the default and does not change the cluster.

## Stage (no Gold publish)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/59-olist-iceberg-gold-wap.yml \
  -e olist_iceberg_wap_mode=stage

python3 scripts/59-verify-trino.py --mode stage \
  --url http://192.158.2.104:8080
```

Expected: `STEP59_WAP_STAGED_PASS`, and Trino main row count is **1**, while the
full validated candidate exists in `audit_v3`.

**Do not publish** until the 29 quality rules and stage proof are inspected:

```bash
python3 - <<'PY'
import json
r=json.load(open('artifacts/59-olist-wap-stage.json'))
print('status',r['status'],'checks',r['quality_checks_total'],'failed',r['quality_checks_failed'])
print('target',r['target_table'],'branch',r['branch'])
print('main',r['main_snapshot_before'],'audit',r['audit_snapshot_id'])
for c in r['checks']:
    print(c['id'],c['status'],c['observed'],c['expected'])
PY
```

## Explicit publish (moves only Step59 Gold main)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/59-olist-iceberg-gold-wap.yml \
  -e olist_iceberg_wap_mode=publish

python3 scripts/59-verify-trino.py --mode publish \
  --url http://192.158.2.104:8080
```

Expected: `STEP59_WAP_PUBLISHED_PASS`,
`STEP59_TRINO_SPARK_WAP_CROSS_QUERY_PASS`.

## Troubleshooting

Driver exceptions live in the YARN container logs, not necessarily stdout of
`spark-submit` (cluster mode). Find the application ID in Ansible output:

```bash
yarn logs -applicationId <application_...> > /tmp/step59-yarn.log 2>&1
grep -n -E 'STEP59_WAP_FAILED|Traceback|Caused by:|Exception|Error' /tmp/step59-yarn.log | tail -80
```

If the REST Catalog reports `Couldn't find Iceberg configuration for catalog
hdfs://...`, check the Spark submit config: **never** configure
`spark.sql.catalog.shared.warehouse` as an HDFS path when using this Gravitino
REST deployment. The warehouse is configured on Gravitino server (Step56).

If stage fails **after branch creation** but before report, rerunning stage will
verify or resume by reading existing refs. Never delete the 58 source tables,
59 namespace, audit branch, or proof files as a first response.

If the 59 stage proof exists with FAIL, subsequent publish is blocked by design.

## Local artifacts and VCS

`artifacts/59-olist-wap-stage.json` and `artifacts/59-olist-wap-publish.json`
are runtime evidence, not source code. Review staged changes with
`git diff --stat && git status --short` and avoid adding `vendor/` or runtime
artifacts when committing.

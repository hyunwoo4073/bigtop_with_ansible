# Step 56 — test / ansible-lint repair

## Symptom

- A prior test rejected the entire `vendor/` directory even though earlier steps already downloaded and used binary dependencies there.
- `ansible-lint` reported 32 `var-naming[no-role-prefix]` errors in the `iceberg_rest_catalog` role.

## Changes

- Test now confirms binary files are not stored **inside the role source tree**, while permitting a local `vendor/` directory.
- Renamed defaults, register variables, and Jinja/role references consistently to `iceberg_rest_catalog_*`.
- Preserved original command-line flags `iceberg_rest_apply=true` and `iceberg_rest_spark_smoke=true` as aliases, so previous instructions still work.
- No host, PostgreSQL, HDFS, Java or service configuration is changed by extracting this patch.

## Apply (worker3)

```bash
cd ~/bigtop-cluster-ansible
unzip -o 56_iceberg_rest_catalog_lint_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_iceberg_rest_catalog.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/56-iceberg-rest-catalog.yml
```

Only after the test and lint pass, follow `docs/56-iceberg-rest-apply.md` for asset fetch and opt-in installation.

# 47 Quick Apply — Olist quality monitor

On **worker3**, repository root, while on the feature branch:

```bash
unzip -o 47_olist_data_quality_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_quality.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/47-olist-data-quality.yml
```

Review the `Show Olist quality summary` output (status `PASS`, number of checks, run ID, report HDFS path). Check the worker3 `.prom` textfile and Node Exporter endpoint. Do not use `ansible_connection=local` because worker3 is the Ansible controller and `master` is remote.

```bash
.venv/bin/ansible master -b --become-user hdfs -m shell \
  -a 'hdfs dfs -ls /data/olist/quality/v1/runs/'
sudo cat /var/lib/prometheus/node-exporter/olist_quality.prom
curl -fsS http://127.0.0.1:9100/metrics | grep '^olist_quality_'
```

See `docs/47-olist-data-quality.md` for quality semantics, thresholds, error behavior, monitoring caveats, and troubleshooting.

# 48 Quick Apply — Olist scheduled Spark/quality checks

On worker3 from repository root (ops = worker3):

```bash
unzip -o 48_olist_pipeline_automation_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_scheduler.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/48-olist-pipeline-schedule.yml -e ansible_connection=local
sudo systemctl start bigtop-olist-pipeline.service
sudo journalctl -u bigtop-olist-pipeline.service -n 80 --no-pager
sudo cat /var/lib/prometheus/node-exporter/olist_pipeline.prom
```

After checking manual success and Prometheus collection, enable daily timer:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/48-olist-pipeline-schedule.yml \
  -e ansible_connection=local -e data_pipeline_olist_scheduler_timer_enabled=true
```

For alerting, first check that Prometheus rules glob includes the deployed file and that worker3 Node Exporter metrics are scraped; then set `data_pipeline_olist_scheduler_alerts_enabled=true` in the same 48 command. See `docs/48-olist-pipeline-automation.md` for full operating/recovery notes.

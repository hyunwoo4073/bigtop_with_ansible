# 48 — Olist pipeline scheduling and failure alerts

## Purpose and runtime

Existing **46 Spark ETL** (YARN cluster) and **47 quality checks** (YARN cluster) are executed serially from **worker3**, which runs Ansible. No changes to 45–47 or HDFS Raw/Warehouse are made by this package.

```
worker3 systemd timer -> worker3 root wrapper
                       -> runuser -u <controller account> -- ansible-playbook 46 (master)
                       -> only on successful completion: ansible-playbook 47 (master)
                       -> HDFS quality/v1/runs/<uuid> report
                       -> worker3 Node Exporter textfile: olist_pipeline.prom and olist_quality.prom
                       -> Prometheus -> Alertmanager (if rule_files loaded and receiver configured)
```

The wrapper is root-owned only to write the Node Exporter textfile and lock state. **Both Ansible child processes execute as the configured non-root controller account** (usually `bae`). File write permissions on worker3 and SSH/sudo access to master must already work under that account.

## Apply

On worker3:

```bash
cd ~/bigtop-cluster-ansible
unzip -o 48_olist_pipeline_automation_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_scheduler.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/48-olist-pipeline-schedule.yml -e ansible_connection=local
```

**Default is safe**: timer is installed but **disabled**; alert rules are not installed until explicitly opted in. `ansible_connection=local` applies only when invoking **48** (hosts: ops = worker3) and is **not** passed to nested 46/47 playbooks (which must SSH to master).

Run one supervised cycle:

```bash
sudo systemctl start bigtop-olist-pipeline.service
sudo systemctl show bigtop-olist-pipeline.service -p Result -p ExecMainStatus
sudo journalctl -u bigtop-olist-pipeline.service -n 100 --no-pager
sudo cat /var/lib/prometheus/node-exporter/olist_pipeline.prom
curl -fsS http://127.0.0.1:9100/metrics | grep '^olist_pipeline_'
```

Output expected: `OLIST PIPELINE SUCCESS`, `Result=success`, `ExecMainStatus=0`, latest attempt success=1 and running=0. 46 verifies/skips the existing immutable `/data/olist/warehouse/v1` without rewriting it. 47 independently reruns quality and writes a new immutable report, including updated `olist_quality.prom`.

Enable daily 04:30 *worker3 local time* only after the supervised cycle works:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/48-olist-pipeline-schedule.yml \
  -e ansible_connection=local -e data_pipeline_olist_scheduler_timer_enabled=true
systemctl list-timers --all | grep bigtop-olist-pipeline
```

`Persistent=false` (default) avoids running immediately after VM boot just to catch up a missed daily window. `flock` prevents overlapping scripts, and the one-shot systemd unit prevents simultaneous service starts. The 7200s timeout fails the unit if a Spark stage hangs. At higher load, adjust settings after measuring YARN run times.

## Alert rules

The role can install six rules under `/etc/prometheus/rules/olist-pipeline.yml`:

- `OlistPipelineExecutionFailed`: orchestration failed, including cases where 47 never ran.
- `OlistPipelineExecutionStuck`: marked running for more than 2h.
- `OlistPipelineNeverExecuted`: exporter metrics missing.
- `OlistPipelineScheduledRunStale`: more than 32h since last attempt started.
- `OlistQualityValidationFailed`: quality report returned failure or failed checks.
- `OlistQualityReportStale`: no quality report update within 32h.

They are **opt-in** because the timer is disabled during the first manual run and a missing first report would otherwise generate premature alerts.

**Before enabling:**

```bash
sudo grep -A8 'rule_files' /etc/prometheus/prometheus.yml
sudo systemctl status prometheus --no-pager
curl -fsS http://127.0.0.1:9100/metrics | grep -E '^olist_(pipeline|quality)_' | head
curl -fsSG 'http://127.0.0.1:9090/api/v1/query' --data-urlencode 'query=olist_pipeline_last_attempt_success' | python3 -m json.tool
```

Verify Prometheus `rule_files` already includes `/etc/prometheus/rules/*.yml` (or the equivalent glob that covers `olist-pipeline.yml`). **48 intentionally does not overwrite `/etc/prometheus/prometheus.yml`.** If that glob is missing, update the **existing Prometheus Ansible role/configuration**, not a hand-edited runtime file.

After confirming the timer is enabled and the pipeline/quality metrics are scraped, enable alert rules:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/48-olist-pipeline-schedule.yml \
  -e ansible_connection=local \
  -e data_pipeline_olist_scheduler_timer_enabled=true \
  -e data_pipeline_olist_scheduler_alerts_enabled=true
sudo promtool check rules /etc/prometheus/rules/olist-pipeline.yml
curl -fsS http://127.0.0.1:9090/api/v1/rules | python3 -m json.tool | grep -A3 OlistPipeline
```

Prometheus supports rule reload on SIGHUP without restart. Alertmanager *routing to Slack/email* is a separate pre-existing setting; loaded rules do not imply notifications have been delivered. Alert `absent` checks test the overall presence of matching metrics, so ensure only the intended worker3 target exports these series. See official Prometheus docs for rules/reloads.

## Failure modes and recovery

- ETL fails: quality does not start; service fails, wrapper publishes `olist_pipeline_last_attempt_success=0` and maintains previous `last_success` timestamp.
- Quality fails: 47 still writes its FAIL report and `olist_quality_run_success=0` if possible; service fails, pipeline failure metrics are written.
- No quality report due Spark/YARN launch failure: quality metrics can be stale, but 48 pipeline failure metric indicates the failure.
- Termination: wrapper attempts to write failure on TERM/INT; SIGKILL and VM power loss cannot run its EXIT trap. The running/stuck/freshness alerts cover these cases when Prometheus remains available.
- **No concurrent runs**: flock returns 75 if another wrapper owns the lock, without overwriting its metrics. Do not launch additional 46/47 manually while the timer is running; those playbooks do not acquire the 48 lock.
- HDFS `/data/olist/raw/v1` and `/data/olist/warehouse/v1` are immutable. This schedule **does not ingest new raw data** or create new versions; it repeatedly verifies the existing ETL and refreshes quality reports. For changing production data, implement a version-aware ingestion orchestration in a future step.
- Disable schedule without deleting saved reports or service units:

```bash
sudo systemctl disable --now bigtop-olist-pipeline.timer
```

For complete disable using Ansible, rerun 48 with both `data_pipeline_olist_scheduler_timer_enabled=false` and `data_pipeline_olist_scheduler_alerts_enabled=false`.

## Next

49: Add a Grafana pipeline overview and then OpenMetadata ingestion of HDFS Curated/Mart assets and verified Spark lineage. More advanced future pipeline orchestration may be a better fit for Airflow as additional sources arrive.

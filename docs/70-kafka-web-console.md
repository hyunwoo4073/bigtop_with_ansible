# Step70 — Kafka Web Console integration (read-only)

This step extends the *existing* React + FastAPI Web Console on `worker3:1337`. There is no new server, no Kafka reconfiguration, no Spark stop/start, and no new Kafka topics. Existing Step65 data and checkpoint remain intact.

## Changes

* `main.jsx`: **no separate Kafka sidebar tab**. Kafka is integrated into the existing `Services` group/filter, Home group summary, and Grid views.
* `KafkaOperationsView.jsx`, `kafka-operations.css`: broker TCP probes, Step65 CDC committed lag, Kafka topics/groups, and selected group partition lag.
* `kafka_ops70.py`: separate FastAPI read-only router installed into existing Web Console Python app directory.
* `app.py.j2`: imports/registers router with existing FastAPI app.
* `web_console_service_ui_links/defaults/main.yml`: adds existing Kafbat shortcut (`metadata:18081`). Existing Grafana shortcut reused.
* `web_console_kafka`: Ansible role for preflight/apply/verify; does not reinstall full platform or nginx.

All API routes are GET. Kafka list/group operations use read-only Kafka CLI; commands are fixed executable names and arguments (no shell interpolation). The Step65 streaming/lag widget uses Step67/68 Prometheus metrics. **`TCP OK` is not equivalent to broker health/leader/ISR consistency.** Missing Prometheus data renders as unknown, never zero.

## Installation

Copy the ZIP to worker3 and extract it *outside* the repo. Run the bundled installer, which validates the uploaded source revisions before replacing tracked files and keeps backups under ignored `artifacts/step70/`.

```bash
cd ~/bigtop-cluster-ansible
mkdir -p /tmp/bigtop-step70
unzip -oq step70_kafka_web_console.zip -d /tmp/bigtop-step70
bash /tmp/bigtop-step70/install.sh "$PWD"

.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/70-kafka-web-console.yml --syntax-check
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/70-kafka-web-console.yml -e web_console_kafka_mode=preflight
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/70-kafka-web-console.yml -e web_console_kafka_mode=apply
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/70-kafka-web-console.yml -e web_console_kafka_mode=verify
```

Default mode is **plan**. Preflight and verify only read state; apply copies the router/3 React assets, builds the existing Vite app, validates a candidate Python template, adds Kafbat UI shortcut preserving other links, and restarts only `bigtop-web-console`. Nginx config and Basic Auth are not altered. Existing frontend `dist` is backed up once to `dist.pre-step70` before rebuild.

## API checks

On worker3:

```bash
curl -fsS http://127.0.0.1:18090/api/ops/kafka/overview | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/ops/kafka/topics | python3 -m json.tool
curl -fsS http://127.0.0.1:18090/api/ops/kafka/groups | python3 -m json.tool
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:1337/api/ops/kafka/overview  # expect 401
```

Visit `http://worker3:1337`, authenticate as usual, open **Services** and select **Kafka** using the existing group filter. The three broker systemd services appear as regular rows, with the Kafka operations/metrics panel beneath that group's list. The Kafbat link from the shortcut configuration is `http://metadata:18081`. If your browser cannot resolve `metadata` due to VM port forwarding or DNS, adjust the shortcut URL for the browser-accessible address; do not expose an unauthenticated admin UI publicly.

## Known limitations

* Broker cards are fixed TCP probes of three configured listeners, not authoritative broker/ISR health; Kafka CLI list success verifies metadata operations.
* Topics and group listings cap display at 200. Only selected group's lag is fetched; no all-groups cluster-wide lag claim.
* Step68 recording rules must have produced samples. A missing sample is shown as `—` rather than `0`.
* No topic creation, messages, offset reset, connector deployment, or lifecycle management is exposed here.
* The existing Nginx Basic Auth protects the console; Kafbat may need its own controls because it opens separately.
* This source snapshot did not include all React components or installed `node_modules`. The user's VM `npm run build` is the authoritative integration check.

## Rollback

The installer prints the backup location under `artifacts/step70/source-backups/<timestamp>/`. Restore those original tracked files into the repository, then redeploy the original `web_console` role via the established playbook/role (this may rebuild its frontend and restart the web service). The separate `kafka_ops70.py` is harmless if no longer imported. Existing Step65–69 functionality is unaffected. Keep the backup out of Git.

## Document lifecycle

Update `README.md` and `docs/PROJECT_GUIDE.md` only at the end of the work session, together with all code and Ansible changes in one Git commit/push.

## Services consistency correction (2026-11-14)

* The previous standalone Kafka sidebar item has been removed.
* `COMPONENTS` registers Kafka KRaft systemd unit `bigtop-kafka-kraft64` on worker1, worker2 and worker3 (these match Step64A's installed unit).
* Existing `/api/status` supplies these three brokers to Home/Services/Grid/Incidents via the standard status collector. No new service API or duplicate status method.
* Kafka operations details remain a read-only panel inside **Services → Kafka**; Kafbat remains a service UI shortcut because it is a Docker container, not the Kafka broker systemd service.
* Start/Stop/Restart are hidden for Kafka service rows, incident and detail actions; backend lifecycle endpoints explicitly reject them with HTTP 403. Existing lifecycle controls for other services remain unchanged.
* The existing Step70 role `apply` rebuilds the frontend, validates the FastAPI template and restarts only the Web Console. `verify` now checks the three Kafka components appear in `/api/status`.

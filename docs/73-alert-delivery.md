# Step73 — End-to-end synthetic alert verification

## Scope and current evidence

Existing worker3 preflight PASS: Prometheus ready, Alertmanager ready, 1 active Alertmanager target, seven Step68 alerts loaded/healthy/inactive, receivers named `default-receiver` and `slack-notifications`. These facts **do not** establish Slack delivery; no synthetic alert was sent in preflight.

This package extends Step73 preflight with **optional live testing**. It does **not** change Step65 Spark/Kafka, Iceberg, checkpoints, Step68 rules, persistent Alertmanager routes, or existing receivers. It creates one expiring, isolated rule file for a uniquely identified test alert, observes Prometheus and Alertmanager, explicitly transitions the alert to resolved, and removes the file. No new services installed.

## Before testing

Extract into project root on worker3 using `unzip -n ... -d .`. The archive updates Step73 files introduced by the previous preflight ZIP. `unzip -n` skips existing paths, so for an update use the **Step73-only overwrite workflow** (see below) rather than extracting with `-n`.

**Recommended safe extract for this package**, which only contains Step73 paths:

```bash
unzip -o step73_alert_delivery_test.zip -d .
```

Inspect existing routes first; output is restricted to route matchers and receiver names. It must not print Webhook URLs or tokens:

```bash
bash scripts/run_step73_delivery.sh inspect
```

Check the actual `route_tree` and receiver types. A receiver named `slack-notifications` is not proof that this alert will match it. If the default severity labels do not match the Slack route, supply the appropriate **non-secret** matching label set with `STEP73_PROBE_LABELS_JSON` (JSON string). Preserve `component=step73` and omit `step73_probe`, `step73_probe_id`, and `alertname` since the script sets these itself.

## Live end-to-end test

**This generates an actual alert and may notify people.** Coordinate with users of the configured receiver before running it. The script will refuse unless explicitly enabled.

```bash
STEP73_ENABLE_LIVE_ALERT=YES \
STEP73_EXPECT_RECEIVER=slack-notifications \
bash scripts/run_step73_delivery.sh test
```

For a special route, use e.g.:

```bash
STEP73_ENABLE_LIVE_ALERT=YES \
STEP73_EXPECT_RECEIVER=slack-notifications \
STEP73_PROBE_LABELS_JSON='{"severity":"warning","component":"step73","team":"data-platform"}' \
bash scripts/run_step73_delivery.sh test
```

**Do not blindly copy `team=data-platform`:** use matchers seen in the actual inspected route. No receiver config or route is altered.

The script:
1. Confirms existing preflight/route without printing secrets.
2. Adds `/etc/prometheus/rules/bigtop-olist-step73-probe.yml` after checking rule glob and `promtool` syntax.
3. Reloads Prometheus; the rule is initially true and expires automatically within 10 minutes even if the script is killed.
4. Waits for Prometheus firing, Alertmanager active, and (if set) routing to the **expected receiver**. Prints `STEP73_FIRING_ROUTE_PASS` only when those conditions hold.
5. Holds the firing alert for 75 seconds by default (`STEP73_HOLD_SECONDS=30..180`) so Alertmanager can group and dispatch. Long `group_wait` configurations may need another test after tuning the hold within these bounds.
6. Changes only the Step73 test expression to an inactive result and reloads Prometheus to send a resolved update; checks `STEP73_RESOLVED_STATE_PASS`.
7. Removes the rule file, reloads Prometheus, prints `STEP73_PROBE_CLEANUP_PASS` and `STEP73_ROUTE_AND_RESOLUTION_PASS`.

`STEP73_ROUTE_AND_RESOLUTION_PASS` proves the alert passed through Prometheus into Alertmanager, matched a receiver, and was resolved. **It does not prove a human received Slack/webhook messages.** Confirm both firing and resolved messages in the destination. Whether a resolved notification is sent depends on receiver `send_resolved` setting and dispatch behavior. Do not report `STEP73_DELIVERY_PASS` without destination or integration-specific proof.

## Failure or cancellation

The shell script automatically attempts cleanup on normal failures/interrupts. If cleanup fails or the process is killed forcefully, manually run:

```bash
bash scripts/run_step73_delivery.sh cleanup
```

This deletes only the owned Step73 synthetic rule and reloads Prometheus. It refuses to delete a file that lacks the Step73 marker. It **does not** change Alertmanager configuration or wipe evidence from operating systems.

Never re-run `test` blindly if `bigtop-olist-step73-probe.yml` remains present. The script refuses to replace a preexisting test file.

## Verification and Git

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/73-alert-delivery.yml \
  -e olist_kafka73_mode=preflight
make validate
```

Store run output in an ignored artifacts directory, if desired. Include Step73 source, role, tests, run scripts and docs in one session-level Git commit after local validation; do not add `artifacts/`, caches, or installed binaries.

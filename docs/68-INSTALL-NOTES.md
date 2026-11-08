# Step68 - Patch installation notes

Review contents before copying into the repository. This archive contains **new files only**:
`playbooks/68-streaming-health.yml`, `roles/olist_kafka68/`, `docs/68-streaming-health.md`.

To inspect and stage from worker3:

```bash
unzip -l /path/to/step68_streaming_health.zip
cd ~/bigtop-cluster-ansible
unzip -n /path/to/step68_streaming_health.zip -d .
# Review docs/68-INSTALL-NOTES.md for validation and rollback notes.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml --syntax-check
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml -e olist_kafka68_mode=preflight
```

Only after `preflight` succeeds:

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml -e olist_kafka68_mode=apply
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/68-streaming-health.yml -e olist_kafka68_mode=verify
```

Existing Grafana provisioning will be used; no dashboard service restart is performed. Review `docs/68-streaming-health.md` for data freshness, alert caveats and rollback.

This patch has not been executed against the user's live VM. Do not treat it as deployed until the actual playbook results and Prometheus rule group are verified. Commit with all other changes of the work session after review, not as a documentation-only commit.

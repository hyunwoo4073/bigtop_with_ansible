SHELL := /bin/bash
.DEFAULT_GOAL := help
.RECIPEPREFIX := >

VENV_DIR ?= .venv
VENV_BIN := $(VENV_DIR)/bin

ANSIBLE ?= $(VENV_BIN)/ansible
ANSIBLE_PLAYBOOK ?= $(VENV_BIN)/ansible-playbook
ANSIBLE_INVENTORY ?= $(VENV_BIN)/ansible-inventory
ANSIBLE_GALAXY ?= $(VENV_BIN)/ansible-galaxy

PYTHON ?= $(VENV_BIN)/python
PIP ?= $(VENV_BIN)/pip
YAMLLINT ?= $(VENV_BIN)/yamllint
ANSIBLE_LINT ?= $(VENV_BIN)/ansible-lint

PLAYBOOK_DIR := playbooks
APP_ID ?=

.PHONY: help
help:
> @echo "Bigtop Cluster Ansible operation commands"
> @echo
> @echo "Dev:"
> @echo "  make venv                      Create Python virtual environment for dev tools"
> @echo "  make collections               Install Ansible Galaxy collections"
> @echo "  make dev-setup                 Create venv and install Ansible collections"
> @echo "  make lint                      Run yamllint and ansible-lint"
> @echo "  make validate                  Run full static validation before commit"
> @echo "  make ci                        Run full CI validation locally"
> @echo "  make fix-newline               Add missing EOF newlines to project text files"
> @echo
> @echo "Basic:"
> @echo "  make inventory                 Show Ansible inventory graph"
> @echo "  make ping                      Ping all nodes"
> @echo "  make syntax                    Run syntax check for playbooks"
> @echo
> @echo "Operations:"
> @echo "  make health                    Run cluster health check"
> @echo "  make recover                   Recover stopped or failed services"
> @echo "  make smoke                     Run cluster smoke test"
> @echo "  make post-reboot               Ping, recover, health check, and smoke test"
> @echo
> @echo "YARN / Spark:"
> @echo "  make diagnostics APP_ID=...    Collect YARN/Spark application diagnostics"
> @echo "  make yarn-log-aggregation      Configure YARN log aggregation directory"
> @echo "  make spark-history-maintenance Check Spark History event log storage"
> @echo "  make nm-storage                Prepare NodeManager local/log directories"
> @echo "  make nm-storage-metrics        Configure NodeManager storage metrics"
> @echo
> @echo "Monitoring:"
> @echo "  make prometheus                Apply Prometheus configuration"
> @echo "  make grafana                   Apply Grafana dashboards"
> @echo "  make alertmanager              Apply Alertmanager configuration"
> @echo "  make monitoring                Apply Prometheus, Grafana, and Alertmanager"
> @echo
> @echo "Full apply helpers:"
> @echo "  make apply-core                Apply common, Java, Bigtop repo, Hadoop, HDFS, YARN, Spark"
> @echo "  make apply-observability       Apply exporters, Prometheus, Grafana, Alertmanager"
> @echo "  make apply-ops                 Apply backup, log aggregation, storage, metrics, and health check"

.PHONY: venv
venv:
> python3 -m venv $(VENV_DIR)
> $(PYTHON) -m pip install --upgrade pip
> $(PIP) install -r requirements-dev.txt

.PHONY: collections
collections:
> $(ANSIBLE_GALAXY) collection install -r collections/requirements.yml

.PHONY: dev-setup
dev-setup: venv collections

.PHONY: fix-newline
fix-newline:
> @python3 - <<'PY'
> from pathlib import Path
> 
> targets = [
>     Path(".yamllint"),
>     Path(".ansible-lint"),
>     Path("requirements-dev.txt"),
>     Path("Makefile"),
>     Path("README.md"),
>     Path("inventory/group_vars/all.yml.example"),
> ]
> 
> for base in [Path("playbooks"), Path("roles"), Path("scripts"), Path("docs")]:
>     if base.exists():
>         for path in base.rglob("*"):
>             if path.is_file() and path.suffix in {".yml", ".yaml", ".j2", ".sh", ".md", ".txt"}:
>                 targets.append(path)
> 
> for file in sorted(set(targets)):
>     if not file.exists():
>         continue
>     data = file.read_bytes()
>     if not data:
>         continue
>     if b"\0" in data:
>         continue
>     if not data.endswith(b"\n"):
>         file.write_bytes(data + b"\n")
>         print(f"fixed newline: {file}")
> PY

.PHONY: lint
lint:
> @test -x "$(YAMLLINT)" || (echo "Missing $(YAMLLINT). Run: make venv"; exit 1)
> @test -x "$(ANSIBLE_LINT)" || (echo "Missing $(ANSIBLE_LINT). Run: make venv"; exit 1)
> $(YAMLLINT) .yamllint .ansible-lint inventory/group_vars/all.yml.example playbooks roles $$(test -d docs && echo docs)
> $(ANSIBLE_LINT)

.PHONY: validate
validate:
> @test -x "$(YAMLLINT)" || (echo "Missing $(YAMLLINT). Run: make venv"; exit 1)
> @test -x "$(ANSIBLE_LINT)" || (echo "Missing $(ANSIBLE_LINT). Run: make venv"; exit 1)
> ./scripts/check-static-validation.sh

.PHONY: ci
ci: venv collections validate

.PHONY: inventory
inventory:
> $(ANSIBLE_INVENTORY) --graph

.PHONY: ping
ping:
> $(ANSIBLE) all -m ping

.PHONY: syntax
syntax:
> @set -euo pipefail; \
> for f in $$(find $(PLAYBOOK_DIR) -maxdepth 1 -type f -name '*.yml' | sort); do \
>   echo; \
>   echo "==> syntax check: $$f"; \
>   $(ANSIBLE_PLAYBOOK) "$$f" --syntax-check; \
> done

.PHONY: health
health:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/20-cluster-health-check.yml

.PHONY: recover
recover:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/21-cluster-recover.yml

.PHONY: smoke
smoke:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/22-cluster-smoke-test.yml

.PHONY: post-reboot
post-reboot: ping recover health smoke

.PHONY: diagnostics
diagnostics:
> @test -n "$(APP_ID)" || (echo "Usage: make diagnostics APP_ID=application_XXXXXXXXXXXX_XXXX"; exit 1)
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/23-yarn-app-diagnostics.yml -e yarn_app_id=$(APP_ID)

.PHONY: yarn-log-aggregation
yarn-log-aggregation:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/24-yarn-log-aggregation.yml

.PHONY: spark-history-maintenance
spark-history-maintenance:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/25-spark-history-maintenance.yml

.PHONY: nm-storage
nm-storage:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/26-yarn-nodemanager-storage.yml

.PHONY: nm-storage-metrics
nm-storage-metrics:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/27-yarn-nodemanager-storage-metrics.yml

.PHONY: prometheus
prometheus:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/14-prometheus.yml

.PHONY: grafana
grafana:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/15-grafana.yml

.PHONY: alertmanager
alertmanager:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/17-alertmanager.yml

.PHONY: monitoring
monitoring: prometheus grafana alertmanager

.PHONY: apply-core
apply-core:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/01-common.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/02-java.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/03-bigtop-repo.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/04-hadoop-install.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/05-hdfs-config.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/06-hdfs-start.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/07-yarn-install.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/08-yarn-config.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/09-yarn-start.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/10-spark-install.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/11-spark-config.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/12-spark-history-start.yml

.PHONY: apply-observability
apply-observability:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/13-node-exporter.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/16-jmx-exporter.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/14-prometheus.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/15-grafana.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/17-alertmanager.yml

.PHONY: apply-ops
apply-ops:
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/18-namenode-backup.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/19-namenode-remote-backup.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/24-yarn-log-aggregation.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/25-spark-history-maintenance.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/26-yarn-nodemanager-storage.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/27-yarn-nodemanager-storage-metrics.yml
> $(ANSIBLE_PLAYBOOK) $(PLAYBOOK_DIR)/20-cluster-health-check.yml
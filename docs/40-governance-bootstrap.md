# Stage 40 — Governance VM Docker bootstrap

## Scope

Target: `governance_nodes` only (`metadata`). No changes to `master`, `worker1`,
`worker2`, `worker3`, any Hadoop service, Web Console, or PostgreSQL deployment.

Installs **Docker Engine and Docker Compose plugin** from Docker's official,
signed Ubuntu package repository; enables Docker at boot; configures
`vm.max_map_count=1048576` for Elasticsearch. It **does not deploy** OpenMetadata
itself, download any OpenMetadata images, launch containers, grant `bae` membership
in the root-equivalent `docker` group, or change the firewall.

## Preconditions

- Inventory contains `[governance_nodes]` with `metadata` at the target IP.
- Ubuntu Server 24.04 on metadata, hostname `metadata`, 4+ vCPU, 6+ GiB RAM.
- SSH works and account `bae` has sudo rights; metadata can reach Docker's APT repo.
- At least 40–60 GB virtual disk strongly recommended.
- Repository conflict guard rejects previously installed Ubuntu Docker/containerd
  instead of automatically uninstalling them.

## Deploy from worker3

```bash
cd ~/bigtop-cluster-ansible
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/40-governance-docker-bootstrap.yml -K
```

If `bae` has passwordless sudo on metadata, omit `-K`.
**Do not** use `-e ansible_connection=local`; this playbook must SSH to metadata.

## Validate

```bash
.venv/bin/ansible -i inventory/hosts.ini governance_nodes -b -K \
  -m ansible.builtin.command -a 'docker compose version'
.venv/bin/ansible -i inventory/hosts.ini governance_nodes -b -K \
  -m ansible.builtin.command -a 'sysctl vm.max_map_count'
```

Next: after Docker is ready, deploy a verified/pinned OpenMetadata release using
an independent Ansible role, with isolated persistent volumes, credentials and
port mapping. Official quickstart runs several memory-intensive services; measure
available VM memory before that deployment. **Never use `docker compose down -v`**
against a data-bearing instance.

Official references:
- https://docs.docker.com/engine/install/ubuntu/
- https://docs.open-metadata.org/v1.12.x/quick-start/local-docker-deployment

# 43. OpenMetadata Governance: classification, glossary, ownership & descriptions

## Purpose

Step 42 has ingested table and column metadata. Step 43 enriches OpenMetadata-only metadata using a **declarative, Ansible-managed policy** and a **manual apply** gate. No database rows, accounts, Hadoop nodes, ingress ports, or container volumes are changed. This is a lab proof-of-concept.

## Workflow

1. Ansible deploys an HTTP API client, declarative YAML, and a disabled-by-default manual oneshot systemd service to worker3.
2. `--plan` reads target table details and existing labels/owners/descriptions; checks all target columns exist. No POST or PATCH.
3. `--apply` creates absent classification, tags, glossary, terms and conservatively appends labels/descriptions to two tables. Owner assignment is opt-in and only if the named team/user exists and the asset has no existing owners.
4. Re-applying is safe: no update if the desired metadata is already present. Existing human-written metadata remains unchanged.

## API and constraints

OpenMetadata API base: `http://metadata:8585/api` from worker3. Bot JWT read from root-only protected file. Named GET followed by optional POST for governance entities and JSON Patch for tables. Table tags are appended, never rewritten; column descriptions are patched by index **after fetching current columns**, not by replacing complete table column arrays.

API client error messages deliberately avoid HTTP response bodies and tokens. On auth failure, check scoped API bot privileges and renew token; never commit it. Step 42 bot may not have governance write permissions.

## Intentional scope exclusions

No automatic PII scans or sensitivity claims, no external data ownership inference, no enforcement of data security policies, no automatic scheduler. Assigning a classification label **does not implement DB access control**. Production environments should replace lab HTTP with TLS and use a separate least-privilege governance bot.

CLI commands and policy example: `README-43-APPLY-KR.md` in the package.

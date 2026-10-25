#!/usr/bin/env python3
"""Synchronize Ansible hosts + Web Console component declarations into PostgreSQL.

Reads a JSON inventory snapshot (non-secret), extracts a literal COMPONENTS list
from the *deployed* FastAPI Python file, and atomically upserts metadata.
Does not alter the source registry, service configuration or operating services.
"""
import argparse
import hashlib
import ast
import json
import os
import re
from pathlib import Path


def read_inventory(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    slug = data.get("cluster_slug")
    if not isinstance(slug, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", slug):
        raise ValueError("Invalid cluster slug (expected lowercase ASCII slug)")
    hosts = data.get("hosts")
    if not isinstance(hosts, list) or not hosts:
        raise ValueError("Inventory snapshot contains no hosts")
    mapped = {}
    for host in hosts:
        name = host.get("name")
        addr = host.get("address")
        memberships = host.get("groups")
        if not isinstance(name, str) or not name.strip() or not isinstance(addr, str) or not addr.strip():
            raise ValueError("Host name/address missing")
        if not isinstance(memberships, list) or any(not isinstance(g, str) for g in memberships):
            raise ValueError("Invalid host group list")
        if name in mapped:
            raise ValueError("Duplicate inventory host: " + name)
        mapped[name] = {"name": name, "address": addr, "groups": sorted(set(memberships))}
    return slug, mapped


def read_services(path, inventory_hosts):
    source = Path(path).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    definitions = [node for node in tree.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "COMPONENTS"
                           for target in node.targets)]
    if len(definitions) != 1:
        raise ValueError("Expected exactly one static COMPONENTS assignment in deployed backend")
    raw = ast.literal_eval(definitions[0].value)
    if not isinstance(raw, list) or not raw:
        raise ValueError("COMPONENTS must be a nonempty static list")
    services = {}
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("Component must be a dictionary")
        host = item.get("host")
        service = item.get("service")
        component = item.get("component")
        group = item.get("group")
        if any(not isinstance(v, str) or not v.strip()
               for v in (host, service, component, group)):
            raise ValueError("Component fields host/service/component/group must be nonempty strings")
        if host not in inventory_hosts:
            raise ValueError("Component references a host absent from inventory: " + host)
        key = (host, service)
        if key in services:
            raise ValueError(f"Duplicate component: {host}::{service}")
        services[key] = (host, service, component, group)
    return services


def sync(conn, ddl, slug, hosts, services):
    # A stable fingerprint means repeated identical deployments do not write again.
    snapshot = {
        "hosts": [hosts[name] for name in sorted(hosts)],
        "services": [services[key] for key in sorted(services)],
    }
    fingerprint = hashlib.sha256(
        json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    # Single transaction: schema + all metadata changes become visible together.
    with conn.transaction():
        with conn.cursor() as cursor:
            for query in ddl.split(";"):
                if query.strip():
                    cursor.execute(query)
            # Prevent two sync processes racing on the same cluster.
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("bigtop-metadata:" + slug,))
            cursor.execute("SELECT snapshot_fingerprint FROM public.platform_clusters WHERE cluster_slug=%s", (slug,))
            previous = cursor.fetchone()
            if previous and previous[0] == fingerprint:
                # Compare actual rows as well: restore missing/edited DB rows on rerun.
                cursor.execute("""SELECT host_name, inventory_address, inventory_groups
                    FROM public.platform_hosts WHERE cluster_slug=%s AND is_present""", (slug,))
                live_hosts = {name: {"name": name, "address": address,
                                     "groups": sorted(group_list)}
                              for name, address, group_list in cursor.fetchall()}
                cursor.execute("""SELECT host_name, service_name, component_name, group_name
                    FROM public.platform_service_instances
                    WHERE cluster_slug=%s AND is_present""", (slug,))
                live_services = {(host, name): (host, name, component, group)
                                 for host, name, component, group in cursor.fetchall()}
                if live_hosts == hosts and live_services == services:
                    return False
            cursor.execute("""INSERT INTO public.platform_clusters(cluster_slug, snapshot_fingerprint)
                VALUES (%s, %s) ON CONFLICT (cluster_slug)
                DO UPDATE SET snapshot_fingerprint=EXCLUDED.snapshot_fingerprint,
                              last_synced_at=now()""", (slug, fingerprint))
            for host in hosts.values():
                cursor.execute("""INSERT INTO public.platform_hosts
                    (cluster_slug, host_name, inventory_address, inventory_groups, is_present)
                    VALUES (%s, %s, %s, %s::jsonb, true)
                    ON CONFLICT (cluster_slug, host_name)
                    DO UPDATE SET inventory_address=EXCLUDED.inventory_address,
                                  inventory_groups=EXCLUDED.inventory_groups,
                                  is_present=true, last_seen_at=now()""",
                               (slug, host["name"], host["address"], json.dumps(host["groups"])))
            for host, service, component, group in services.values():
                cursor.execute("""INSERT INTO public.platform_service_instances
                    (cluster_slug, host_name, service_name, component_name, group_name, is_present)
                    VALUES (%s, %s, %s, %s, %s, true)
                    ON CONFLICT (cluster_slug, host_name, service_name)
                    DO UPDATE SET component_name=EXCLUDED.component_name,
                                  group_name=EXCLUDED.group_name,
                                  is_present=true, last_seen_at=now()""",
                               (slug, host, service, component, group))
            # Removed entries are marked absent, never deleted.
            # Temporary table makes the reconciliation safe for arbitrary text host/service names.
            cursor.execute("""CREATE TEMP TABLE metadata_keys(host_name text, service_name text)
                ON COMMIT DROP""")
            for host, service in services:
                cursor.execute("INSERT INTO metadata_keys VALUES (%s,%s)", (host, service))
            cursor.execute("""UPDATE public.platform_service_instances AS s
                SET is_present=false
                WHERE s.cluster_slug=%s AND s.is_present
                  AND NOT EXISTS(SELECT 1 FROM metadata_keys AS k
                                 WHERE k.host_name=s.host_name AND k.service_name=s.service_name)""", (slug,))
            cursor.execute("""CREATE TEMP TABLE metadata_hosts(host_name text) ON COMMIT DROP""")
            for host in hosts:
                cursor.execute("INSERT INTO metadata_hosts VALUES (%s)", (host,))
            cursor.execute("""UPDATE public.platform_hosts AS h SET is_present=false
                WHERE h.cluster_slug=%s AND h.is_present
                  AND NOT EXISTS(SELECT 1 FROM metadata_hosts AS n WHERE n.host_name=h.host_name)""", (slug,))
            cursor.execute("""INSERT INTO public.platform_metadata_sync_runs
                (cluster_slug, host_count, service_count) VALUES (%s, %s, %s)""",
                           (slug, len(hosts), len(services)))
            return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--inventory", required=True)
    p.add_argument("--app", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--min-hosts", type=int, default=1)
    p.add_argument("--min-services", type=int, default=1)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    slug, hosts = read_inventory(args.inventory)
    services = read_services(args.app, hosts)
    if len(hosts) < args.min_hosts or len(services) < args.min_services:
        raise SystemExit("Refusing to sync: catalog is below configured minimums")
    print(f"Validated: cluster={slug}, hosts={len(hosts)}, services={len(services)}")
    if args.dry_run:
        return
    dsn = os.getenv("WEB_CONSOLE_DB_DSN")
    if not dsn:
        raise SystemExit("Missing WEB_CONSOLE_DB_DSN")
    import psycopg
    ddl = Path(args.schema).read_text(encoding="utf-8")
    with psycopg.connect(dsn, connect_timeout=5) as conn:
        changed = sync(conn, ddl, slug, hosts, services)
    if changed:
        print(f"Synchronized: cluster={slug}, hosts={len(hosts)}, services={len(services)}")
    else:
        print(f"Unchanged: cluster={slug}, hosts={len(hosts)}, services={len(services)}")


if __name__ == "__main__":
    main()

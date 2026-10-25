-- Version 1: metadata is synchronized from Ansible inventory + Web Console COMPONENTS.
-- Idempotent and non-destructive: existing records stay when removed from source.
CREATE TABLE IF NOT EXISTS public.platform_clusters (
    cluster_slug TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    snapshot_fingerprint TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.platform_hosts (
    cluster_slug TEXT NOT NULL REFERENCES public.platform_clusters(cluster_slug),
    host_name TEXT NOT NULL,
    inventory_address TEXT NOT NULL,
    inventory_groups JSONB NOT NULL DEFAULT '[]'::jsonb,
    is_present BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_slug, host_name)
);
CREATE TABLE IF NOT EXISTS public.platform_service_instances (
    cluster_slug TEXT NOT NULL,
    host_name TEXT NOT NULL,
    service_name TEXT NOT NULL,
    component_name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    is_present BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_slug, host_name, service_name),
    FOREIGN KEY (cluster_slug, host_name)
      REFERENCES public.platform_hosts(cluster_slug, host_name)
);
CREATE INDEX IF NOT EXISTS idx_platform_hosts_presence
    ON public.platform_hosts (cluster_slug, is_present);
CREATE INDEX IF NOT EXISTS idx_platform_services_group
    ON public.platform_service_instances (cluster_slug, group_name, is_present);
CREATE TABLE IF NOT EXISTS public.platform_metadata_sync_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cluster_slug TEXT NOT NULL REFERENCES public.platform_clusters(cluster_slug),
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    host_count INTEGER NOT NULL,
    service_count INTEGER NOT NULL,
    source TEXT NOT NULL DEFAULT 'ansible-inventory+web-console-components'
);

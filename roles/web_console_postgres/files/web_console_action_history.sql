-- Safe to execute on every Ansible run. Never drops data.
CREATE TABLE IF NOT EXISTS public.action_history (
    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    recorded_at TEXT NOT NULL,
    action TEXT,
    host TEXT,
    service TEXT,
    ok BOOLEAN,
    blocked BOOLEAN NOT NULL DEFAULT FALSE,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_action_history_host_seq ON public.action_history (host, seq DESC);
CREATE INDEX IF NOT EXISTS idx_action_history_service_seq ON public.action_history (service, seq DESC);
CREATE INDEX IF NOT EXISTS idx_action_history_action_seq ON public.action_history (action, seq DESC);

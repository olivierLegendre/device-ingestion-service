CREATE TABLE IF NOT EXISTS telemetry_events (
  telemetry_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  source TEXT NOT NULL,
  message_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  point_key TEXT NOT NULL,
  value_json JSONB NOT NULL,
  unit TEXT,
  observed_at TIMESTAMPTZ NOT NULL,
  payload_hash TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_site_payload_created
  ON telemetry_events (site_id, payload_hash, created_at DESC);

CREATE TABLE IF NOT EXISTS dead_letters (
  dead_letter_id TEXT PRIMARY KEY,
  organization_id TEXT NOT NULL,
  site_id TEXT NOT NULL,
  protocol TEXT NOT NULL,
  topic TEXT NOT NULL,
  reason TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_scope_created
  ON dead_letters (organization_id, site_id, created_at DESC);

CREATE TABLE IF NOT EXISTS outbox_events (
  event_id TEXT PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
  ON outbox_events (published_at)
  WHERE published_at IS NULL;

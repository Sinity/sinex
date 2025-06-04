-- Migration: Create agent manifests table
-- Up Migration

CREATE TABLE IF NOT EXISTS sinex_schemas.agent_manifests (
    agent_name              TEXT PRIMARY KEY, -- Unique, e.g., "HyprlandIngestor_Rust_v0.3.1"
    description             TEXT,
    version                 TEXT NOT NULL,    -- Semantic version of agent code
    status                  TEXT NOT NULL DEFAULT 'unknown', 
                            -- Ops status: 'running', 'stopped', 'error_state', 'disabled_by_user', 'pending_registration', 'degraded', 'unknown'
    agent_type              TEXT NOT NULL DEFAULT 'generic', 
                            -- e.g., 'ingestor', 'promoter', 'enricher', 'analytical', 'ui_backend', 'system_utility'
    
    config_template_json    JSONB NULLABLE, -- Example JSON/YAML structure of expected config file
    config_schema_id        ULID REFERENCES sinex_schemas.event_payload_schemas(id) NULLABLE, 
                            -- FK to a JSON Schema defining agent's config file structure (Alternative to config_template_json)

    -- Describes events this agent GENERATES
    produces_event_types    JSONB NULLABLE, 
                            -- Example: {"desktop.hyprland.plugin": [{"type": "window_focused", "schema_id_ref": "ULID_schema_A"}, ...]}
                            -- schema_id_ref points to sinex_schemas.event_payload_schemas.id

    -- Describes events this agent CONSUMES (for routing)
    subscribes_to_event_types JSONB NULLABLE, 
                            -- Example: {"raw.events_feed_all": [{"source_filter": "app.neovim.*", "event_type_filter": "file_saved_v1"}]}
                            -- Or: {"sinex.pkm.note_updated": [{"schema_id_expected_ref": "ULID_schema_B"}]}

    required_capabilities   JSONB NULLABLE,   
                            -- e.g., {"filesystem_read": ["/path/to/pkm"], "network_host_allow": ["api.openai.com:443"], "db_tables_rw": ["core.artifacts"]}
    
    llm_dependencies        JSONB NULLABLE,   -- {"models_used": ["ollama/mistral:7b", "openai/gpt-4-turbo"], "required_capabilities": ["function_calling"]}
    
    repo_url                TEXT NULLABLE,    -- Link to agent's source code
    last_heartbeat_ts       TIMESTAMPTZ NULLABLE,
    last_error_ts           TIMESTAMPTZ NULLABLE,
    last_error_summary      TEXT NULLABLE,
    registered_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE sinex_schemas.agent_manifests IS 'Central registry for Sinex agents, their capabilities, configuration, and status.';

-- Indexes
CREATE INDEX IF NOT EXISTS idx_agent_manifests_status_type ON sinex_schemas.agent_manifests (agent_type, status);
CREATE INDEX IF NOT EXISTS idx_agent_manifests_subscribes_gin ON sinex_schemas.agent_manifests USING GIN (subscribes_to_event_types) WHERE subscribes_to_event_types IS NOT NULL;

-- Trigger for updated_at
CREATE TRIGGER trg_agent_manifests_set_updated_at
BEFORE UPDATE ON sinex_schemas.agent_manifests
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();
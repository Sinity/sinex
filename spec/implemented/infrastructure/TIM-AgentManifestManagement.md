# TIM-AgentManifestManagement: Agent Manifest Schema and Management

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 90% (Database schema, registration, heartbeats, CLI fully working, event routing implemented, JSON schema validation pending)
**Dependencies**: PostgreSQL, ULID generation, sinex-collector, agent framework
**Blocks**: Agent discovery, event routing, agent lifecycle management, operational monitoring

## MVP Specification
- Agent manifest database schema with runtime registration
- Agent registration and heartbeat management
- Event routing based on agent capabilities
- Basic agent status tracking and monitoring
- CLI interface for agent management

## Enhanced Features
- Static JSON manifest schema validation
- Bundled manifest files in agent binaries
- Advanced agent capability matching
- Automated agent deployment and scaling
- Comprehensive agent health monitoring
- Agent dependency management

## Implementation Checklist
- [x] Database schema (sinex_schemas.agent_manifests) - `migrations/20250103120007_create_agent_manifests.sql`
- [x] Agent registration with UPSERT logic - `crate/sinex-collector/src/agent.rs:register_agent()`
- [x] Heartbeat and lifecycle management - `crate/sinex-collector/src/agent.rs:AgentLifecycle`
- [x] Event routing to work queue - `migrations/20250103120009_create_event_router.sql`
- [x] CLI agent management commands - `cli/exo.py:agent_list()`, `agent_status()`
- [x] Comprehensive test suite - `test/agent/agent_manifest_tests.rs`
- [ ] JSON schema for static manifests
- [ ] Bundled manifest files in binaries
- [ ] CI validation of manifest schemas

*   **Relevant ADR:** (N/A directly, core infrastructure for agent framework)
*   **Original UG Context:** Section 30
*   **Vision Document Reference:** Part IV.1.2 (Agent Registry)

This TIM details the database schema for `sinex_schemas.agent_manifests`, the JSON schema for agent self-description files, and the processes for agent registration, status updates, and event routing based on manifest capabilities.

## 1. Rationale Summary

A robust agent manifest system is crucial for discovering, managing, and orchestrating Exocortex agents, enabling modularity and extensibility. The database table is the runtime registry, while bundled JSON files allow agent self-description.

## 2. Database Schema (`sinex_schemas.agent_manifests`) [UG Sec 30.1]

*   **DDL (from UG Sec 30.1, Primary Document Appendix A, refined):**
    ```sql
    CREATE SCHEMA IF NOT EXISTS sinex_schemas; -- Ensure schema exists

    CREATE TABLE IF NOT EXISTS sinex_schemas.agent_manifests (
        agent_name              TEXT PRIMARY KEY, -- Unique, e.g., "HyprlandIngestor_Rust_v0.3.1"
        description             TEXT,
        version                 TEXT NOT NULL,    -- Semantic version of agent code
        status                  TEXT NOT NULL DEFAULT 'unknown', 
                                -- Ops status: 'running', 'stopped', 'error_state', 'disabled_by_user', 'pending_registration', 'degraded', 'unknown'
        agent_type              TEXT NOT NULL DEFAULT 'generic', 
                                -- e.g., 'ingestor', 'promoter', 'enricher', 'analytical', 'ui_backend', 'system_utility'
        
        config_template_json    JSONB NULLABLE, -- Example JSON/YAML structure of expected config file
        -- config_schema_id     ULID REFERENCES sinex_schemas.event_payload_schemas(id) NULLABLE, 
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
    CREATE INDEX IF NOT EXISTS idx_agent_manifests_status_type ON sinex_schemas.agent_manifests (agent_type, status);
    CREATE INDEX IF NOT EXISTS idx_agent_manifests_subscribes_gin ON sinex_schemas.agent_manifests USING GIN (subscribes_to_event_types) WHERE subscribes_to_event_types IS NOT NULL;

    -- Trigger for updated_at (using function from TIM-LLMResourceOrchestration.md or define here if first)
    -- CREATE TRIGGER trg_agent_manifests_set_updated_at
    -- BEFORE UPDATE ON sinex_schemas.agent_manifests
    -- FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func(); 
    -- Ensure core.set_updated_at_trigger_func() is defined:
    CREATE OR REPLACE FUNCTION core.set_updated_at_trigger_func_generic()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER trg_agent_manifests_set_updated_at
    BEFORE UPDATE ON sinex_schemas.agent_manifests
    FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();
    ```

## 3. JSON Schema for Agent Self-Description Files (`agent_manifest.json`) [UG Sec 30.2, OR3]

Agents can bundle a static `agent_manifest.json` file. This is used for initial self-registration or can be parsed by deployment tools.
*   **Schema (Conceptual from UG Sec 30.2):**
    ```json
    // {
    //   "$schema": "http://json-schema.org/draft-07/schema#",
    //   "title": "Sinex Agent Static Manifest",
    //   "type": "object",
    //   "required": ["agent_name_base", "version", "description", "agent_type"], // agent_name_base is without version
    //   "properties": {
    //     "agent_name_base": { "type": "string", "description": "Base name, e.g., HyprlandIngestor_Rust. Version added at runtime." },
    //     "version": { "type": "string", "pattern": "^\\d+\\.\\d+\\.\\d+.*$" },
    //     "description": { "type": "string" },
    //     "agent_type": { "type": "string", "enum": ["ingestor", "promoter", "enricher", "analytical", "ui_backend", "system_utility"] },
    //     "config_template_json_example": { "type": "object", "description": "Example structure of agent's config file." },
    //     "produces_event_types_static": { /* As in DB schema */ "type": "object" },
    //     "subscribes_to_event_types_static": { /* As in DB schema */ "type": "object", "nullable": true },
    //     "required_capabilities_static": { /* As in DB schema */ "type": "object", "nullable": true },
    //     "llm_dependencies_static": { /* As in DB schema */ "type": "object", "nullable": true },
    //     "repo_url": { "type": "string", "format": "uri", "nullable": true }
    //   }
    // }
    ```
*   A master JSON Schema for these files should be maintained and used for validation (e.g., in CI).

## 4. Agent Self-Registration and Status Updates [UG Sec 30.3]

*   **Registration (Agent on Startup):**
    1.  Agent reads its version (e.g., from `CARGO_PKG_VERSION` in Rust). Constructs full `agent_name` (e.g., `MyAgent_Rust_v1.2.3`).
    2.  Reads its bundled static manifest or has capabilities compiled in.
    3.  Performs `INSERT INTO sinex_schemas.agent_manifests (...) VALUES (...) ON CONFLICT (agent_name) DO UPDATE SET version = EXCLUDED.version, status = 'running', produces_event_types = EXCLUDED.produces_event_types, ... updated_at = NOW();`
*   **Heartbeats:**
    *   Agents periodically emit `sinex.agent.heartbeat` event to `raw.events`. Payload: `{ "agent_name": "...", "timestamp_iso": "...", "status_reported": "healthy", "metrics_snapshot": {"processed_items": N} }`.
    *   A central "AgentMonitor" agent consumes these heartbeats and updates `agent_manifests.last_heartbeat_ts` and potentially `agent_manifests.status` (e.g., to 'unresponsive' if heartbeats stop).
*   **Status Updates:**
    *   Agents update their `status` in `agent_manifests` on clean shutdown (`'stopped'`), unrecoverable error (`'error_state'`).
*   **Error Reporting:** Significant operational errors logged as `sinex.agent.error` events. AgentMonitor updates `last_error_ts`, `last_error_summary`.

## 5. Routing Logic Based on Manifest Capabilities [UG Sec 30.4, SA4]

An "Event Router" component (PostgreSQL trigger function on `raw.events`, or dedicated agent) populates `sinex_schemas.work_queue`.

*   **SQL Router Function (`sinex_router.route_raw_event_to_work_queue` - from UG Sec 30.4):**
    ```sql
    -- CREATE OR REPLACE FUNCTION sinex_router.route_raw_event_to_work_queue(p_raw_event_id ULID)
    -- RETURNS VOID AS $$
    -- DECLARE
    //     v_event_source TEXT;
    //     v_event_type TEXT;
    //     v_agent_record RECORD;
    // BEGIN
    //     SELECT source, event_type INTO v_event_source, v_event_type
    //     FROM raw.events WHERE id = p_raw_event_id;

    //     IF NOT FOUND THEN RETURN; END IF;

    //     -- Find active agents subscribing to this (source, event_type)
    //     -- This query logic depends heavily on the exact structure of agent_manifests.subscribes_to_event_types JSONB
    //     -- Assuming subscribes_to_event_types is like:
    //     -- { "desktop.hyprland.plugin": ["window_focused", "workspace_activated"], "app.neovim.plugin": ["file_saved"] }
    //     FOR v_agent_record IN
    //         SELECT am.agent_name -- , am.config->>'max_processing_attempts' AS max_attempts_override (if config stored in manifest)
    //         FROM sinex_schemas.agent_manifests am
    //         WHERE am.status = 'running'
    //           AND am.subscribes_to_event_types IS NOT NULL
    //           AND jsonb_path_exists(am.subscribes_to_event_types, format('$.%I[*] ? (@ == %I)', v_event_source, v_event_type)::jsonpath)
    //           -- Or if "source" is a top-level key and "event_type" is in an array value:
    //           -- AND am.subscribes_to_event_types -> v_event_source @> jsonb_build_array(v_event_type)
    //     LOOP
    //         INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name) -- Add max_attempts if configurable
    //         VALUES (p_raw_event_id, v_agent_record.agent_name)
    //         ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING;
    //     END LOOP;
    // END;
    // $$ LANGUAGE plpgsql;

    -- Trigger to call this router function:
    -- CREATE OR REPLACE FUNCTION raw.trigger_router_on_event_insert() RETURNS TRIGGER AS $$
    // BEGIN
    //   PERFORM sinex_router.route_raw_event_to_work_queue(NEW.id);
    //   RETURN NEW;
    // END;
    // $$ LANGUAGE plpgsql;

    -- CREATE TRIGGER trg_raw_events_route_after_insert
    -- AFTER INSERT ON raw.events
    // FOR EACH ROW EXECUTE FUNCTION raw.trigger_router_on_event_insert();
    ```
*   A GIN index on `agent_manifests.subscribes_to_event_types` is beneficial for efficient routing queries.


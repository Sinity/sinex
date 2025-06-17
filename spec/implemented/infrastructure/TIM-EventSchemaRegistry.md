# TIM-EventSchemaRegistry: `sinex_schemas.event_payload_schemas`

*   **Relevant ADR:** (N/A directly, core infrastructure)
*   **Original UG Context:** Section 2.1

This TIM details the implementation and management of the `sinex_schemas.event_payload_schemas` table, which serves as the central registry for JSON Schema definitions describing `raw.events.payload` structures.

## 1. Rationale Summary

A schema registry is crucial for data integrity, documentation, interoperability (e.g., code generation for event types), and managing schema evolution for event payloads. The PostgreSQL-based registry is chosen for simplicity in a single-host MVP.

## 2. DDL for `sinex_schemas.event_payload_schemas`

*   **Schema Definition (from UG Sec 2.1.1, Primary Document Appendix A):**
    ```sql
    CREATE SCHEMA IF NOT EXISTS sinex_schemas;

    CREATE TABLE IF NOT EXISTS sinex_schemas.event_payload_schemas (
        id                      ULID PRIMARY KEY DEFAULT gen_ulid(), -- Using pgx_ulid
        event_source            TEXT NOT NULL,
        event_type              TEXT NOT NULL,
        schema_version          TEXT NOT NULL, -- e.g., "v1.0", "v1.0.1", "v2.0-alpha"
        json_schema_definition  JSONB NOT NULL, -- The actual JSON Schema object
        description             TEXT,
        created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
        is_active               BOOLEAN NOT NULL DEFAULT TRUE, -- Flag for current/active schema version
        UNIQUE (event_source, event_type, schema_version)
    );
    COMMENT ON TABLE sinex_schemas.event_payload_schemas IS 'Registry for JSON Schema definitions of raw.events payloads.';
    COMMENT ON COLUMN sinex_schemas.event_payload_schemas.is_active IS 'Indicates if this schema version is currently active and should be used for new events or validation.';
    ```

## 3. Management Strategy (GitOps-inspired) [Based on UG Sec 2.1.2]

1.  **Schema Source of Truth:** JSON Schema definition files (e.g., `hyprland_window_focused_v1.0.json`) are developed and version-controlled within the main Exocortex Git repository (e.g., under a `/schemas` directory).
2.  **CI/CD for Schema Registration:** A CI/CD pipeline (or a dedicated script/agent like `sinex-schema-manager`):
    *   Validates new/updated JSON Schema files against the JSON Schema meta-schema (e.g., using `ajv-cli` or a language-specific validator).
    *   (Optional) Compares new schema versions against previous active versions for backward compatibility if such policies are enforced (e.g., using a JSON Schema diffing tool).
    *   Idempotently inserts new schema definitions into `sinex_schemas.event_payload_schemas` or updates existing ones (e.g., marking an old version as `is_active = FALSE` and a new one `is_active = TRUE`).
3.  **Eventification of Schema Changes:**
    *   A PostgreSQL trigger on `sinex_schemas.event_payload_schemas` (AFTER INSERT OR UPDATE) logs `sinex.schema.definition_changed` events to `raw.events`.
    *   Payload includes `schema_id`, `event_source`, `event_type`, `new_version`, and type of change (e.g., "created", "activated", "deactivated").

    ```sql
    -- Conceptual Trigger for Schema Change Eventification
    CREATE OR REPLACE FUNCTION sinex_schemas.log_schema_change_trigger_func()
    RETURNS TRIGGER AS $$
    DECLARE
        v_change_type TEXT;
        v_payload JSONB;
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            v_change_type := 'created';
            IF NEW.is_active THEN
                v_change_type := 'created_and_activated';
            END IF;
        ELSIF (TG_OP = 'UPDATE') THEN
            IF OLD.is_active IS DISTINCT FROM NEW.is_active THEN
                v_change_type := CASE WHEN NEW.is_active THEN 'activated' ELSE 'deactivated' END;
            ELSE
                v_change_type := 'updated_metadata'; -- Or more specific if other fields change
            END IF;
        ELSE
            RETURN NULL; -- Should not happen for this trigger configuration
        END IF;

        v_payload := jsonb_build_object(
            'schema_id', NEW.id::text, -- Assuming ULID string representation
            'event_source', NEW.event_source,
            'event_type', NEW.event_type,
            'schema_version', NEW.schema_version,
            'change_type', v_change_type,
            'description', NEW.description,
            '_provenance', jsonb_build_object('correlation_id', current_setting('application_name', true)) -- Example correlation
        );

        INSERT INTO raw.events (source, event_type, host, payload, payload_schema_id)
        VALUES (
            'sinex.schema.registry_monitor',
            'definition_changed',
            inet_client_addr()::text, -- Or a fixed host ID for system events
            v_payload,
            NULL -- Or schema_id for this meta-event itself if defined
        );
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    CREATE TRIGGER trg_event_payload_schemas_after_insert_update
    AFTER INSERT OR UPDATE ON sinex_schemas.event_payload_schemas
    FOR EACH ROW
    EXECUTE FUNCTION sinex_schemas.log_schema_change_trigger_func();
    ```

## 4. Linking `raw.events` to Schemas

The `raw.events.payload_schema_id` column is a foreign key to `sinex_schemas.event_payload_schemas.id`.

```sql
-- Ensure FK is set up (from UG Sec 2.2)
-- This should be run after both tables are created.
ALTER TABLE raw.events
DROP CONSTRAINT IF EXISTS fk_raw_events_payload_schema; -- Drop if exists to avoid error on re-run

ALTER TABLE raw.events
ADD CONSTRAINT fk_raw_events_payload_schema
FOREIGN KEY (payload_schema_id) REFERENCES sinex_schemas.event_payload_schemas(id)
ON DELETE SET NULL        -- If a schema definition is deleted, nullify references
ON UPDATE CASCADE;       -- If a schema ULID changes (unlikely), cascade
```

Ingestors are responsible for looking up the correct `id` from `sinex_schemas.event_payload_schemas` based on the `(event_source, event_type, schema_version)` they are producing and populating `raw.events.payload_schema_id` accordingly. This lookup can be cached by ingestors for performance.


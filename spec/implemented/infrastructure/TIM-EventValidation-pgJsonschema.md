# TIM-EventValidation-pgJsonschema: In-Database JSON Schema Validation

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 80% (Extension and validation infrastructure complete, monitoring pending)
**Dependencies**: pg_jsonschema PostgreSQL extension, schema registry, event ingestion pipeline
**Blocks**: Data integrity enforcement, payload validation, schema compliance

## MVP Specification
- pg_jsonschema extension setup and configuration
- Basic JSON Schema validation functions
- Integration with event ingestion pipeline
- Validation error handling and reporting
- Schema-based payload filtering

## Enhanced Features
- Advanced validation rules and constraints
- Custom validation functions
- Performance optimization for high-volume validation
- Validation metrics and monitoring
- Schema evolution and migration support
- Custom error reporting and diagnostics

## Implementation Checklist
- [x] pg_jsonschema extension installation
- [x] Database validation functions setup
- [x] Integration with schema registry
- [x] Validation trigger implementation
- [x] Error handling and logging
- [ ] Performance benchmarking
- [ ] Validation rule configuration
- [ ] Monitoring and metrics
- [ ] Documentation and best practices

*   **Relevant ADR:** (N/A directly, supports data integrity principle)
*   **Original UG Context:** Section 2.2

This TIM details the use of the `pg_jsonschema` PostgreSQL extension for validating `raw.events.payload` JSONB data against schemas registered in `sinex_schemas.event_payload_schemas`, directly within the database.

## 1. Rationale Summary

In-database validation using `pg_jsonschema` provides a performant (C-based extension, much faster than PL/pgSQL UDFs [CR3]) and consistent way to enforce data integrity for event payloads at the point of ingestion or during processing.

## 2. `pg_jsonschema` Extension Setup

*   **NixOS:** Ensure the `pg_jsonschema` package is available for the PostgreSQL version used.
    ```nix
    # Example: in configuration.nix
    # services.postgresql = {
    #   enable = true;
    #   package = pkgs.postgresql_16;
    //   extraPlugins = [ pkgs.postgresql_16Packages.pg_jsonschema ]; // Check correct package name
    // };
    ```
*   **Database Activation:**
    ```sql
    CREATE EXTENSION IF NOT EXISTS pg_jsonschema;
    ```

## 3. Validation `CHECK` Constraint on `raw.events`

A `CHECK` constraint on `raw.events` enforces that if a `payload_schema_id` is provided and the referenced schema is active, the `payload` must conform to it.

*   **DDL (from UG Sec 2.2.3, refined):**
    ```sql
    -- Ensure this runs after raw.events and sinex_schemas.event_payload_schemas tables are created
    -- and after the FK from raw.events.payload_schema_id to event_payload_schemas.id is established.

    ALTER TABLE raw.events
    DROP CONSTRAINT IF EXISTS chk_payload_conforms_to_schema; -- Drop if exists for idempotency

    ALTER TABLE raw.events
    ADD CONSTRAINT chk_payload_conforms_to_schema
    CHECK (
        payload_schema_id IS NULL OR -- If no schema is specified, validation is skipped
        (
            -- Fetch the schema definition and active status in a subquery
            WITH schema_info AS (
                SELECT ps.json_schema_definition, ps.is_active
                FROM sinex_schemas.event_payload_schemas ps
                WHERE ps.id = raw.events.payload_schema_id
            )
            -- Only attempt validation if the schema was found and is active
            (SELECT si.is_active FROM schema_info si) = TRUE
            AND
            -- Perform the actual JSON Schema validation
            jsonb_matches_schema(
               (SELECT si.json_schema_definition FROM schema_info si),
               payload
            )
        )
    );
    COMMENT ON CONSTRAINT chk_payload_conforms_to_schema ON raw.events
        IS 'Ensures that raw.events.payload conforms to the JSON schema specified by payload_schema_id, if that schema is active.';
    ```
*   **Performance Note on Subselects in `CHECK`:** For extremely high ingestion rates, the subselects within the `CHECK` constraint could introduce latency. If this becomes a bottleneck:
    1.  **Asynchronous Validation:** Remove the `CHECK` constraint. Implement validation in an early-stage promotion agent. Non-compliant events are flagged or quarantined.
    2.  **Trigger-Based Validation:** Use a `BEFORE INSERT OR UPDATE` trigger. More complex logic but might offer different performance characteristics.
    Given `pg_jsonschema`'s C-based speed [CR3: ~15x faster than PL/pgSQL], synchronous `CHECK` is the preferred starting point for strong immediate integrity.

## 4. Example JSON Schema Definitions

Refer to **Primary Document Appendix B** (or a future dedicated `TIM-CanonicalEventSchemas.md`) for illustrative JSON Schema examples for key event payloads like:
*   `desktop.hyprland.plugin/window_focused`
*   `app.terminal.kitty_rc/command_executed`
*   `sinex.pkm.sync_agent/note_updated`
*   `user.meta.friction_log/entry_created`

These schemas detail expected properties, data types, `required` fields, and descriptions for the `payload` JSONB of each event.


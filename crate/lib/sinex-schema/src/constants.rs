//! Database constants for table names and common SQL patterns
//!
//! This module centralizes all database-related constants to avoid hardcoding
//! table names and other database strings throughout the codebase.

/// Database schema names
pub mod schemas {
    pub const CORE: &str = "core";
    pub const RAW: &str = "raw";
    pub const PUBLIC: &str = "public";
}

/// Database table names
pub mod tables {
    // Core schema tables
    pub const EVENTS: &str = "core.events";
    pub const PROCESSOR_CHECKPOINTS: &str = "core.processor_checkpoints";
    pub const SOURCE_MATERIAL_REGISTRY: &str = "raw.source_material_registry";
    pub const ANNOTATIONS: &str = "core.annotations";
    pub const EVENT_BLOBS: &str = "core.event_blobs";

    // Raw schema tables
    pub const RAW_EVENTS: &str = "raw.events";

    // Public schema tables
    pub const MIGRATIONS: &str = "public._sqlx_migrations";

    // System tables
    pub const PG_EXTENSION: &str = "pg_extension";
    pub const PG_AVAILABLE_EXTENSIONS: &str = "pg_available_extensions";
}

/// Common SQL patterns and functions
pub mod sql {
    // PostgreSQL functions
    pub const GEN_RANDOM_UUID: &str = "gen_random_uuid()";
    pub const GEN_ULID: &str = "gen_ulid()";
    pub const ULID_GENERATE: &str = "ulid_generate()";
    pub const JSON_MATCHES_SCHEMA: &str = "json_matches_schema";
    pub const JSONB_MATCHES_SCHEMA: &str = "jsonb_matches_schema";

    // Common patterns
    pub const NOW: &str = "NOW()";
    pub const COUNT_ALL: &str = "COUNT(*)";
}

/// Database extension names
pub mod extensions {
    pub const TIMESCALEDB: &str = "timescaledb";
    pub const UUID_OSSP: &str = "uuid-ossp";
    pub const PG_JSONSCHEMA: &str = "pg_jsonschema";
}

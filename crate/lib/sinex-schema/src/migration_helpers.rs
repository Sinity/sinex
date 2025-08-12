//! Schema migration helper utilities
//!
//! This module provides utilities for generating and validating database schema migrations.
//! These are pure schema-level operations that don't depend on domain logic.

/// Generate SQL to create all schemas (namespaces)
pub fn create_schemas() -> Vec<String> {
    vec![
        "CREATE SCHEMA IF NOT EXISTS core".to_string(),
        "CREATE SCHEMA IF NOT EXISTS raw".to_string(),
        "CREATE SCHEMA IF NOT EXISTS audit".to_string(),
        "CREATE SCHEMA IF NOT EXISTS sinex_schemas".to_string(),
    ]
}

/// Generate SQL to check if a table exists
pub fn table_exists_query(schema: &str, table: &str) -> String {
    format!(
        "SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{}' 
            AND table_name = '{}'
        )",
        schema, table
    )
}

/// Generate SQL to check if an index exists
pub fn index_exists_query(schema: &str, index: &str) -> String {
    format!(
        "SELECT EXISTS (
            SELECT FROM pg_indexes 
            WHERE schemaname = '{}' 
            AND indexname = '{}'
        )",
        schema, index
    )
}

/// Generate SQL to check if a schema exists
pub fn schema_exists_query(schema: &str) -> String {
    format!(
        "SELECT EXISTS (
            SELECT FROM information_schema.schemata
            WHERE schema_name = '{}'
        )",
        schema
    )
}

/// Generate SQL to check if an extension exists
pub fn extension_exists_query(extension: &str) -> String {
    format!(
        "SELECT EXISTS (
            SELECT FROM pg_extension
            WHERE extname = '{}'
        )",
        extension
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_schemas() {
        let schemas = create_schemas();
        assert_eq!(schemas.len(), 4);
        assert!(schemas[0].contains("core"));
        assert!(schemas[1].contains("raw"));
        assert!(schemas[2].contains("audit"));
        assert!(schemas[3].contains("sinex_schemas"));
    }

    #[test]
    fn test_existence_queries() {
        let table_query = table_exists_query("core", "events");
        assert!(table_query.contains("information_schema.tables"));
        assert!(table_query.contains("table_schema = 'core'"));
        assert!(table_query.contains("table_name = 'events'"));

        let index_query = index_exists_query("core", "idx_events_source_type");
        assert!(index_query.contains("pg_indexes"));
        assert!(index_query.contains("schemaname = 'core'"));
        assert!(index_query.contains("indexname = 'idx_events_source_type'"));

        let schema_query = schema_exists_query("core");
        assert!(schema_query.contains("information_schema.schemata"));
        assert!(schema_query.contains("schema_name = 'core'"));

        let extension_query = extension_exists_query("pg_jsonschema");
        assert!(extension_query.contains("pg_extension"));
        assert!(extension_query.contains("extname = 'pg_jsonschema'"));
    }
}

//! Declarative configuration for database cleanup strategies.
//!
//! This module centralizes all table cleanup logic, making it explicit which tables
//! need special handling (trigger disabling, skip cleanup, etc.) instead of scattering
//! this knowledge across multiple cleanup functions.

/// Configuration for database cleanup operations.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// List of tables and their cleanup strategies
    pub tables: Vec<TableCleanupStrategy>,
}

/// Strategy for cleaning up a specific table.
#[derive(Debug, Clone)]
pub struct TableCleanupStrategy {
    /// Fully qualified table name (e.g., "core.events", "raw.temporal_ledger")
    pub table_name: &'static str,
    /// How to clean this table
    pub method: CleanupMethod,
    /// Whether to disable triggers before cleanup
    pub disable_triggers: bool,
    /// Reason for special handling (for documentation)
    pub reason: Option<&'static str>,
}

/// Method used to clean a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMethod {
    /// Use TRUNCATE (fast, but doesn't work with hypertables)
    Truncate,
    /// Use DELETE (slower, but works with hypertables and triggers)
    Delete,
    /// Skip cleanup entirely (for append-only reference data)
    Skip,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            tables: vec![
                // Append-only tables that need trigger disabling
                TableCleanupStrategy {
                    table_name: "raw.temporal_ledger",
                    method: CleanupMethod::Delete,
                    disable_triggers: true,
                    reason: Some("Append-only constraint enforced by BEFORE trigger"),
                },
                TableCleanupStrategy {
                    table_name: "core.events",
                    method: CleanupMethod::Delete,
                    disable_triggers: true,
                    reason: Some("Archive trigger must be bypassed during test cleanup"),
                },

                // Regular tables (can use TRUNCATE)
                TableCleanupStrategy {
                    table_name: "core.event_annotations",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_relations",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_cluster_members",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_embeddings",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.entity_relations",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.revisions",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.processor_manifests",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "sinex_schemas.event_payload_schemas",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.processor_checkpoints",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.operations_log",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.transactional_outbox",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.tags",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.tagged_items",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.blobs",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.entities",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_clusters",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "raw.source_material_registry",
                    method: CleanupMethod::Delete,
                    disable_triggers: false,
                    reason: None,
                },
            ],
        }
    }
}

impl CleanupConfig {
    /// Returns tables that require triggers to be disabled.
    pub fn tables_requiring_trigger_disable(&self) -> impl Iterator<Item = &TableCleanupStrategy> {
        self.tables.iter().filter(|t| t.disable_triggers)
    }

    /// Returns tables that should be cleaned (not skipped).
    pub fn tables_to_clean(&self) -> impl Iterator<Item = &TableCleanupStrategy> {
        self.tables
            .iter()
            .filter(|t| t.method != CleanupMethod::Skip)
    }

    /// Returns tables that can use TRUNCATE.
    pub fn truncatable_tables(&self) -> impl Iterator<Item = &TableCleanupStrategy> {
        self.tables
            .iter()
            .filter(|t| t.method == CleanupMethod::Truncate)
    }

    /// Returns tables that need DELETE.
    pub fn delete_only_tables(&self) -> impl Iterator<Item = &TableCleanupStrategy> {
        self.tables
            .iter()
            .filter(|t| t.method == CleanupMethod::Delete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_ledger_has_trigger_disable() {
        let config = CleanupConfig::default();
        let temporal_ledger = config
            .tables
            .iter()
            .find(|t| t.table_name == "raw.temporal_ledger")
            .expect("temporal_ledger should be in config");

        assert!(
            temporal_ledger.disable_triggers,
            "temporal_ledger must have triggers disabled"
        );
        assert_eq!(temporal_ledger.method, CleanupMethod::Delete);
    }

    #[test]
    fn core_events_has_trigger_disable() {
        let config = CleanupConfig::default();
        let events = config
            .tables
            .iter()
            .find(|t| t.table_name == "core.events")
            .expect("core.events should be in config");

        assert!(
            events.disable_triggers,
            "core.events must have triggers disabled"
        );
    }

    #[test]
    fn no_duplicate_tables() {
        let config = CleanupConfig::default();
        let mut seen = std::collections::HashSet::new();

        for table in &config.tables {
            assert!(
                seen.insert(table.table_name),
                "Duplicate table in config: {}",
                table.table_name
            );
        }
    }

    #[test]
    fn all_tables_have_valid_names() {
        let config = CleanupConfig::default();

        for table in &config.tables {
            assert!(
                table.table_name.contains('.'),
                "Table name should be fully qualified (schema.table): {}",
                table.table_name
            );
        }
    }
}

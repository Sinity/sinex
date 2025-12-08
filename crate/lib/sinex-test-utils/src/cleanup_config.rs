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
    /// Whether this table must never be truncated or deleted (safety valve)
    pub protected: bool,
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
                    protected: false,
                    reason: Some("Append-only constraint enforced by BEFORE trigger"),
                },
                TableCleanupStrategy {
                    table_name: "core.events",
                    method: CleanupMethod::Delete,
                    disable_triggers: true,
                    protected: false,
                    reason: Some("Archive trigger must be bypassed during test cleanup"),
                },
                // Regular tables (can use TRUNCATE for speed)
                TableCleanupStrategy {
                    table_name: "core.event_annotations",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_relations",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_cluster_members",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_embeddings",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.entity_relations",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.revisions",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.processor_manifests",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "sinex_schemas.event_payload_schemas",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.processor_checkpoints",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.operations_log",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.transactional_outbox",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.tags",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.tagged_items",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.blobs",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.entities",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_clusters",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "raw.source_material_registry",
                    method: CleanupMethod::Truncate,
                    disable_triggers: false,
                    protected: false,
                    reason: None,
                },
                // Protected/internal tables (never touch)
                TableCleanupStrategy {
                    table_name: "public.seaql_migrations",
                    method: CleanupMethod::Skip,
                    disable_triggers: false,
                    protected: true,
                    reason: Some("Migration history table; must never be cleaned"),
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
            .filter(|t| t.method != CleanupMethod::Skip && !t.protected)
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

    /// Ordered list for FK-safe cleanup; unknown tables are appended in config order.
    pub fn ordered_tables(&self) -> Vec<&TableCleanupStrategy> {
        // Child-to-parent ordering to minimize FK contention.
        const ORDER: &[&str] = &[
            "core.event_annotations",
            "core.event_relations",
            "core.event_cluster_members",
            "core.event_embeddings",
            "core.entity_relations",
            "core.revisions",
            "core.processor_manifests",
            "sinex_schemas.event_payload_schemas",
            "core.processor_checkpoints",
            "core.operations_log",
            "core.transactional_outbox",
            "core.tags",
            "core.tagged_items",
            "core.blobs",
            "core.event_clusters",
            "core.entities",
            "raw.source_material_registry",
            "raw.temporal_ledger",
            "core.events",
        ];

        let mut seen = std::collections::HashSet::new();
        let mut ordered = Vec::new();

        for name in ORDER {
            if let Some(t) = self.tables.iter().find(|t| t.table_name == *name) {
                ordered.push(t);
                seen.insert(*name);
            }
        }

        // Append any remaining tables in config order.
        for t in &self.tables {
            if !seen.contains(t.table_name) {
                ordered.push(t);
            }
        }

        ordered
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
                !table.protected || table.method == CleanupMethod::Skip,
                "Protected tables must be marked Skip: {}",
                table.table_name
            );
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

    #[test]
    fn ordered_tables_cover_all_entries() {
        let config = CleanupConfig::default();
        let ordered = config.ordered_tables();

        assert_eq!(
            ordered.len(),
            config.tables.len(),
            "ordered_tables should include every configured table"
        );

        let mut seen = std::collections::HashSet::new();
        for t in ordered {
            assert!(
                seen.insert(t.table_name),
                "ordered_tables contains duplicate: {}",
                t.table_name
            );
        }
    }
}

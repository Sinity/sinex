//! Declarative configuration for database cleanup strategies.
//!
//! All tables use TRUNCATE (batched into a single statement for speed).
//! Skip-marked tables are never cleaned (migration history, deployed schemas).

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
    /// Whether this table must never be truncated or deleted (safety valve)
    pub protected: bool,
    /// Reason for special handling (for documentation)
    pub reason: Option<&'static str>,
}

/// Method used to clean a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMethod {
    /// Use TRUNCATE — fast, batched, doesn't fire row-level triggers.
    /// Works on hypertables (TimescaleDB 2.x+), bypasses archive/append-only triggers.
    Truncate,
    /// Skip cleanup entirely (for infrastructure reference data)
    Skip,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            tables: vec![
                // Append-only / archive tables — TRUNCATE bypasses row-level triggers.
                // TimescaleDB 2.x+ supports TRUNCATE on hypertables (drops chunks).
                // CASCADE propagates to dependent tables (event_annotations, etc.).
                TableCleanupStrategy {
                    table_name: "raw.temporal_ledger",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some(
                        "Append-only constraint enforced by BEFORE trigger; TRUNCATE bypasses it",
                    ),
                },
                TableCleanupStrategy {
                    table_name: "core.events",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some(
                        "TimescaleDB hypertable; TRUNCATE drops chunks, doesn't fire archive trigger",
                    ),
                },
                TableCleanupStrategy {
                    table_name: "core.event_tombstones",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some("Tombstone tier of principled forgetting; no outbound FKs"),
                },
                TableCleanupStrategy {
                    table_name: "audit.archived_events",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some("Archive tier of principled forgetting"),
                },
                // Regular tables
                TableCleanupStrategy {
                    table_name: "core.event_annotations",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_cluster_members",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_embeddings",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.embedding_cache",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some("Embedding cache; FK to embedding_models, can grow unbounded"),
                },
                TableCleanupStrategy {
                    table_name: "core.embedding_models",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some(
                        "Reference data for embedding providers; parent of cache + embeddings",
                    ),
                },
                TableCleanupStrategy {
                    table_name: "core.entity_relations",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.node_manifests",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "sinex_schemas.event_payload_schemas",
                    method: CleanupMethod::Skip,
                    protected: true,
                    reason: Some(
                        "Infrastructure reference data deployed by contracts preflight; preserved across tests like migrations",
                    ),
                },
                TableCleanupStrategy {
                    table_name: "sinex_schemas.validation_cache",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some("Composite FK to events + schemas; can grow unbounded"),
                },
                TableCleanupStrategy {
                    table_name: "sinex_schemas.gitops_schema_sources",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: Some("GitOps schema sync config; has updated_at trigger"),
                },
                TableCleanupStrategy {
                    table_name: "core.operations_log",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.tags",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.tagged_items",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.blobs",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.entities",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "core.event_clusters",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                TableCleanupStrategy {
                    table_name: "raw.source_material_registry",
                    method: CleanupMethod::Truncate,
                    protected: false,
                    reason: None,
                },
                // Protected/internal tables (never touch)
                TableCleanupStrategy {
                    table_name: "public.seaql_migrations",
                    method: CleanupMethod::Skip,
                    protected: true,
                    reason: Some("Migration history table; must never be cleaned"),
                },
            ],
        }
    }
}

impl CleanupConfig {
    /// Returns tables that should be cleaned (not skipped).
    pub fn tables_to_clean(&self) -> impl Iterator<Item = &TableCleanupStrategy> {
        self.tables
            .iter()
            .filter(|t| t.method != CleanupMethod::Skip && !t.protected)
    }

    /// Returns tables that can use TRUNCATE (all non-skip tables).
    pub fn truncatable_tables(&self) -> impl Iterator<Item = &TableCleanupStrategy> {
        self.tables
            .iter()
            .filter(|t| t.method == CleanupMethod::Truncate)
    }

    /// Ordered list for FK-safe cleanup; unknown tables are appended in config order.
    #[must_use]
    pub fn ordered_tables(&self) -> Vec<&TableCleanupStrategy> {
        // Child-to-parent ordering to minimize FK contention.
        const ORDER: &[&str] = &[
            // Children first (FK dependents)
            "core.event_tombstones",
            "core.event_annotations",
            "core.event_cluster_members",
            "core.embedding_cache",
            "core.event_embeddings",
            "core.entity_relations",
            "core.embedding_models",
            "core.node_manifests",
            "sinex_schemas.validation_cache",
            "sinex_schemas.event_payload_schemas",
            "sinex_schemas.gitops_schema_sources",
            "core.operations_log",
            "core.tags",
            "core.tagged_items",
            "core.blobs",
            "core.event_clusters",
            "core.entities",
            // Archive + parent tables last
            "audit.archived_events",
            "raw.temporal_ledger",
            "core.events",
            "raw.source_material_registry",
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
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn temporal_ledger_uses_truncate() -> ::xtask::sandbox::TestResult<()> {
        let config = CleanupConfig::default();
        let temporal_ledger = config
            .tables
            .iter()
            .find(|t| t.table_name == "raw.temporal_ledger")
            .expect("temporal_ledger should be in config");

        assert_eq!(
            temporal_ledger.method,
            CleanupMethod::Truncate,
            "temporal_ledger should use TRUNCATE (bypasses append-only trigger)"
        );
        Ok(())
    }

    #[sinex_test]
    async fn core_events_uses_truncate() -> ::xtask::sandbox::TestResult<()> {
        let config = CleanupConfig::default();
        let events = config
            .tables
            .iter()
            .find(|t| t.table_name == "core.events")
            .expect("core.events should be in config");

        assert_eq!(
            events.method,
            CleanupMethod::Truncate,
            "core.events should use TRUNCATE (TimescaleDB 2.x+ supports it)"
        );
        Ok(())
    }

    #[sinex_test]
    async fn no_duplicate_tables() -> ::xtask::sandbox::TestResult<()> {
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
        Ok(())
    }

    #[sinex_test]
    async fn all_tables_have_valid_names() -> ::xtask::sandbox::TestResult<()> {
        let config = CleanupConfig::default();

        for table in &config.tables {
            assert!(
                table.table_name.contains('.'),
                "Table name should be fully qualified (schema.table): {}",
                table.table_name
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn ordered_tables_cover_all_entries() -> ::xtask::sandbox::TestResult<()> {
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
        Ok(())
    }
}

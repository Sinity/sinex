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
            tables: sinex_schema::schema::all_tables()
                .iter()
                .map(|table| TableCleanupStrategy {
                    table_name: table.qualified_name,
                    method: if table.cleanup_protected {
                        CleanupMethod::Skip
                    } else {
                        CleanupMethod::Truncate
                    },
                    protected: table.cleanup_protected,
                    reason: None,
                })
                .collect(),
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
        self.tables.iter().collect()
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

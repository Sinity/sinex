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
            tables: sinex_db::schema::defs::all_tables()
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
#[path = "cleanup_config_test.rs"]
mod tests;

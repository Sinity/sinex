//! RAII guards for PostgreSQL session state.
//!
//! These guards follow the same pattern as `OperationIdGuard`: explicit restore
//! via consuming `restore()` method. Drop is not implemented because guards are
//! consumed by restore() calls.

use sqlx::{pool::PoolConnection, Postgres};

use crate::Result;

/// Guard that temporarily sets `session_replication_role = 'replica'`.
pub struct ReplicationRoleGuard {
    was_set: bool,
}

impl ReplicationRoleGuard {
    /// Attempt to set session_replication_role to 'replica' for cleanup.
    pub async fn disable_for_cleanup(conn: &mut PoolConnection<Postgres>) -> Result<Self> {
        let was_set = sqlx::query("SET session_replication_role = 'replica'")
            .execute(conn.as_mut())
            .await
            .is_ok();

        if !was_set {
            tracing::warn!(
                "Unable to set session_replication_role = 'replica' (permission denied); \
                 cleanup may be limited by FK constraints"
            );
        }

        Ok(Self { was_set })
    }

    /// Restore session_replication_role to 'origin'.
    pub async fn restore(self, conn: &mut PoolConnection<Postgres>) -> Result<()> {
        if self.was_set {
            if let Err(e) = sqlx::query("SET session_replication_role = 'origin'")
                .execute(conn.as_mut())
                .await
            {
                tracing::warn!(
                    error = %e,
                    "Failed to restore session_replication_role to origin after cleanup"
                );
            }
        }
        Ok(())
    }
}

/// Guard that temporarily disables row-level security.
pub struct RowSecurityGuard {
    was_disabled: bool,
}

impl RowSecurityGuard {
    /// Disable RLS for cleanup operations.
    pub async fn disable_for_cleanup(conn: &mut PoolConnection<Postgres>) -> Result<Self> {
        let was_disabled = sqlx::query("SET row_security = off")
            .execute(conn.as_mut())
            .await
            .is_ok();

        if !was_disabled {
            tracing::warn!("Failed to disable row_security (permission denied)");
        }

        Ok(Self { was_disabled })
    }

    /// Restore row security to ON.
    pub async fn restore(self, conn: &mut PoolConnection<Postgres>) -> Result<()> {
        if self.was_disabled {
            if let Err(e) = sqlx::query("SET row_security = on")
                .execute(conn.as_mut())
                .await
            {
                tracing::warn!(error = %e, "Failed to re-enable row_security after cleanup");
            }
        }
        Ok(())
    }
}

/// Guard that temporarily disables triggers on specific tables.
pub struct TriggersGuard {
    tables: Vec<String>,
}

impl TriggersGuard {
    /// Disable triggers on the given tables.
    pub async fn disable_for_cleanup(
        conn: &mut PoolConnection<Postgres>,
        tables: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self> {
        let mut disabled_tables = Vec::new();

        for table in tables {
            let table_name = table.as_ref();
            let query = format!("ALTER TABLE {} DISABLE TRIGGER ALL", table_name);

            if sqlx::query(&query).execute(conn.as_mut()).await.is_ok() {
                disabled_tables.push(table_name.to_string());
            } else {
                tracing::warn!(table = %table_name, "Failed to disable triggers on table");
            }
        }

        Ok(Self {
            tables: disabled_tables,
        })
    }

    /// Re-enable triggers on all tables where they were disabled.
    pub async fn restore(self, conn: &mut PoolConnection<Postgres>) -> Result<()> {
        for table in &self.tables {
            let query = format!("ALTER TABLE {} ENABLE TRIGGER ALL", table);
            if let Err(e) = sqlx::query(&query).execute(conn.as_mut()).await {
                tracing::warn!(
                    error = %e,
                    table = %table,
                    "Failed to re-enable triggers after cleanup"
                );
            }
        }
        Ok(())
    }
}

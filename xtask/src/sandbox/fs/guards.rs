//! RAII guards for `PostgreSQL` session state.
//!
//! These guards follow the same pattern as `OperationIdGuard`: explicit restore
//! via consuming `restore()` method. Drop is not implemented because guards are
//! consumed by `restore()` calls.

use crate::sandbox::prelude::*;
use sqlx::error::DatabaseError;
use sqlx::pool::PoolConnection;
use std::ffi::OsStr;
use std::sync::{Mutex, MutexGuard};

static ENV_MUTEX: std::sync::LazyLock<Mutex<()>> = std::sync::LazyLock::new(|| Mutex::new(()));

/// Guard for serializing environment-variable mutations inside tests.
///
/// Tests frequently need to toggle gateway and RPC settings via env vars and those
/// mutations can leak across parallel test execution. `EnvGuard` acquires a
/// process-wide mutex and tracks the previous values for any keys it touches.
/// Once dropped, every recorded variable is restored to its original state,
/// ensuring deterministic behavior even under `cargo test -- --test-threads=N`.
pub struct EnvGuard {
    lock: Option<MutexGuard<'static, ()>>,
    original: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    /// Acquire the global environment mutex and prepare to record changes.
    pub fn new() -> Self {
        let lock = ENV_MUTEX
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            lock: Some(lock),
            original: Vec::new(),
        }
    }

    fn remember_original(&mut self, key: &str) {
        if self.original.iter().any(|(existing, _)| existing == key) {
            return;
        }
        let previous = std::env::var(key).ok();
        self.original.push((key.to_string(), previous));
    }

    /// Set an environment variable while remembering the prior value.
    pub fn set(&mut self, key: &str, value: impl AsRef<OsStr>) {
        self.remember_original(key);
        unsafe { std::env::set_var(key, value) };
    }

    /// Remove an environment variable for the duration of the guard.
    pub fn clear(&mut self, key: &str) {
        self.remember_original(key);
        unsafe { std::env::remove_var(key) };
    }

    /// Convenience helper for optional values (None => clear, Some => set).
    pub fn set_optional(&mut self, key: &str, value: Option<&str>) {
        match value {
            Some(v) => self.set(key, v),
            None => self.clear(key),
        }
    }
}

impl Default for EnvGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, previous) in self.original.drain(..).rev() {
            unsafe {
                if let Some(value) = previous {
                    std::env::set_var(&key, value);
                } else {
                    std::env::remove_var(&key);
                }
            }
        }
        // Explicitly drop the mutex guard last so other tests can proceed.
        self.lock.take();
    }
}

fn is_hypertable_trigger_toggle_error(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .and_then(|db_err| db_err.code())
        .is_some_and(|code| code.as_ref() == "0A000")
}

/// Guard that temporarily sets `session_replication_role = 'replica'`.
pub struct ReplicationRoleGuard {
    was_set: bool,
}

impl ReplicationRoleGuard {
    /// Attempt to set `session_replication_role` to 'replica' for cleanup.
    pub async fn disable_for_cleanup(conn: &mut PoolConnection<Postgres>) -> Result<Self> {
        sqlx::query("SET session_replication_role = 'replica'")
            .execute(conn.as_mut())
            .await
            .map_err(|e| SinexError::database(e.to_string()))?;

        Ok(Self { was_set: true })
    }

    /// Restore `session_replication_role` to 'origin'.
    pub async fn restore(self, conn: &mut PoolConnection<Postgres>) -> Result<()> {
        if self.was_set
            && let Err(e) = sqlx::query("SET session_replication_role = 'origin'")
                .execute(conn.as_mut())
                .await
        {
            tracing::warn!(
                error = %e,
                "Failed to restore session_replication_role to origin after cleanup"
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn was_set(&self) -> bool {
        self.was_set
    }
}

/// Guard that temporarily disables row-level security.
pub struct RowSecurityGuard {
    was_disabled: bool,
}

impl RowSecurityGuard {
    /// Disable RLS for cleanup operations.
    pub async fn disable_for_cleanup(conn: &mut PoolConnection<Postgres>) -> Result<Self> {
        sqlx::query("SET row_security = off")
            .execute(conn.as_mut())
            .await
            .map_err(|e| SinexError::database(e.to_string()))?;

        Ok(Self { was_disabled: true })
    }

    /// Restore row security to ON.
    pub async fn restore(self, conn: &mut PoolConnection<Postgres>) -> Result<()> {
        if self.was_disabled
            && let Err(e) = sqlx::query("SET row_security = on")
                .execute(conn.as_mut())
                .await
        {
            tracing::warn!(error = %e, "Failed to re-enable row_security after cleanup");
        }
        Ok(())
    }

    #[must_use]
    pub fn was_disabled(&self) -> bool {
        self.was_disabled
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
            let query = format!("ALTER TABLE {table_name} DISABLE TRIGGER ALL");

            match sqlx::query(&query).execute(conn.as_mut()).await {
                Ok(_) => disabled_tables.push(table_name.to_string()),
                Err(err) if is_hypertable_trigger_toggle_error(&err) => {
                    tracing::warn!(
                        table = %table_name,
                        "Skipping trigger disable for hypertable during cleanup"
                    );
                }
                Err(err) => {
                    return Err(SinexError::database(err.to_string()).into());
                }
            }
        }

        Ok(Self {
            tables: disabled_tables,
        })
    }

    /// Re-enable triggers on all tables where they were disabled.
    pub async fn restore(self, conn: &mut PoolConnection<Postgres>) -> Result<()> {
        for table in &self.tables {
            let query = format!("ALTER TABLE {table} ENABLE TRIGGER ALL");
            if let Err(e) = sqlx::query(&query).execute(conn.as_mut()).await {
                if is_hypertable_trigger_toggle_error(&e) {
                    tracing::warn!(
                        table = %table,
                        "Skipping trigger enable for hypertable after cleanup"
                    );
                } else {
                    tracing::warn!(
                        error = %e,
                        table = %table,
                        "Failed to re-enable triggers after cleanup"
                    );
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn disabled_tables(&self) -> &[String] {
        &self.tables
    }
}

//! GitOps schema source repository
//!
//! CRUD operations for `sinex_schemas.gitops_schema_sources`.
//! Used by both the gateway (RPC handlers) and ingestd (sync service).

use super::common::{DbResult, Repository, db_error};
use sinex_primitives::Uuid;
use sinex_primitives::error::SinexError;
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;

/// A row from `sinex_schemas.gitops_schema_sources`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GitOpsSourceRecord {
    pub id: Uuid,
    pub repository_url: String,
    pub branch: String,
    pub path_pattern: String,
    pub sync_enabled: bool,
    pub last_sync_at: Option<Timestamp>,
    pub last_sync_commit: Option<String>,
    pub sync_frequency_minutes: i32,
}

pub struct GitOpsRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for GitOpsRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl GitOpsRepository<'_> {
    /// List all gitops sources, optionally including disabled ones.
    pub async fn list_sources(&self, include_disabled: bool) -> DbResult<Vec<GitOpsSourceRecord>> {
        sqlx::query_as!(
            GitOpsSourceRecord,
            r#"
            SELECT
                id as "id!: Uuid",
                repository_url,
                branch,
                path_pattern,
                sync_enabled,
                last_sync_at as "last_sync_at: Timestamp",
                last_sync_commit,
                sync_frequency_minutes
            FROM sinex_schemas.gitops_schema_sources
            WHERE ($1 OR sync_enabled = true)
            ORDER BY repository_url, branch
            "#,
            include_disabled
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list gitops sources"))
    }

    /// Create a new gitops source configuration.
    ///
    /// Validates inputs before inserting. Returns the generated UUIDv7.
    pub async fn create_source(
        &self,
        repository_url: &str,
        branch: &str,
        path_pattern: &str,
        sync_frequency_minutes: i32,
    ) -> DbResult<Uuid> {
        if repository_url.starts_with("file://") {
            return Err(SinexError::validation(
                "file:// URLs are not allowed for gitops sources",
            ));
        }
        if repository_url.is_empty() {
            return Err(SinexError::validation("Repository URL cannot be empty"));
        }
        if sync_frequency_minutes < 1 {
            return Err(SinexError::validation(
                "Sync frequency must be at least 1 minute",
            ));
        }

        let id = Uuid::now_v7();

        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.gitops_schema_sources (
                id, repository_url, branch, path_pattern,
                sync_enabled, sync_frequency_minutes
            ) VALUES (
                $1::uuid, $2, $3, $4, true, $5
            )
            "#,
            id,
            repository_url,
            branch,
            path_pattern,
            sync_frequency_minutes,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "create gitops source"))?;

        Ok(id)
    }

    /// Delete a gitops source. Returns true if a row was deleted.
    pub async fn delete_source(&self, id: &Uuid) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            DELETE FROM sinex_schemas.gitops_schema_sources
            WHERE id = $1::uuid
            "#,
            id
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "delete gitops source"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Trigger an immediate sync by resetting `last_sync_at` to NULL.
    ///
    /// Only affects enabled sources. Returns true if a row was updated.
    pub async fn trigger_sync(&self, id: &Uuid) -> DbResult<bool> {
        let result = sqlx::query!(
            r#"
            UPDATE sinex_schemas.gitops_schema_sources
            SET last_sync_at = NULL
            WHERE id = $1::uuid AND sync_enabled = true
            "#,
            id
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "trigger gitops sync"))?;

        Ok(result.rows_affected() > 0)
    }

    /// Update a source's sync state after a successful sync.
    pub async fn update_sync_state(&self, id: &Uuid, commit_sha: &str) -> DbResult<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.gitops_schema_sources
            SET last_sync_at = NOW(),
                last_sync_commit = $1
            WHERE id = $2::uuid
            "#,
            commit_sha,
            id
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update gitops source sync state"))?;

        Ok(())
    }
}

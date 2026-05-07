//! Source binding repository for managing source-material acquisition declarations.
//!
//! CRUD operations for `raw.source_bindings` and `raw.source_binding_resolution_log`.

use super::common::{DbResult, Repository, db_error};
use sinex_primitives::error::SinexError;
use sinex_primitives::temporal::Timestamp;
use sqlx::PgPool;
use uuid::Uuid;

/// A row from `raw.source_bindings`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SourceBindingRow {
    pub id: Uuid,
    pub name: String,
    pub source_family: String,
    pub binding_mode: String,
    pub resolver_preset: Option<String>,
    pub locator: serde_json::Value,
    pub input_shape_kind: String,
    pub material_format_hint: Option<String>,
    pub parser_id: Option<String>,
    pub source_unit_id: Option<String>,
    pub privacy_policy_id: String,
    pub raw_material_policy: serde_json::Value,
    pub watch_policy: serde_json::Value,
    pub host_scope: Option<String>,
    pub user_scope: Option<String>,
    pub enabled: bool,
    pub status: String,
    pub last_resolved: Option<serde_json::Value>,
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// A row from `raw.source_binding_resolution_log`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SourceBindingResolutionLogRow {
    pub id: Uuid,
    pub source_binding_id: Uuid,
    pub resolved_at: Timestamp,
    pub candidate_count: i32,
    pub selected_locator: Option<serde_json::Value>,
    pub evidence: serde_json::Value,
    pub status: String,
    pub error_summary: Option<String>,
}

pub struct SourceBindingRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for SourceBindingRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl SourceBindingRepository<'_> {
    /// List all source bindings, optionally filtered by source family.
    pub async fn list_bindings(
        &self,
        source_family: Option<&str>,
        include_disabled: bool,
    ) -> DbResult<Vec<SourceBindingRow>> {
        sqlx::query_as::<_, SourceBindingRow>(
            r#"
            SELECT
                id,
                name,
                source_family,
                binding_mode,
                resolver_preset,
                locator,
                input_shape_kind,
                material_format_hint,
                parser_id,
                source_unit_id,
                privacy_policy_id,
                raw_material_policy,
                watch_policy,
                host_scope,
                user_scope,
                enabled,
                status,
                last_resolved,
                last_error,
                created_at,
                updated_at
            FROM raw.source_bindings
            WHERE ($1::text IS NULL OR source_family = $1)
              AND ($2 OR enabled = true)
            ORDER BY source_family, name
            "#,
        )
        .bind(source_family)
        .bind(include_disabled)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list source bindings"))
    }

    /// Get a single binding by name.
    pub async fn get_binding_by_name(&self, name: &str) -> DbResult<Option<SourceBindingRow>> {
        sqlx::query_as::<_, SourceBindingRow>(
            r#"
            SELECT
                id,
                name,
                source_family,
                binding_mode,
                resolver_preset,
                locator,
                input_shape_kind,
                material_format_hint,
                parser_id,
                source_unit_id,
                privacy_policy_id,
                raw_material_policy,
                watch_policy,
                host_scope,
                user_scope,
                enabled,
                status,
                last_resolved,
                last_error,
                created_at,
                updated_at
            FROM raw.source_bindings
            WHERE name = $1
            "#,
        )
        .bind(name)
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "get source binding by name"))
    }

    /// Create a new source binding.
    ///
    /// Returns the generated UUID.
    pub async fn create_binding(
        &self,
        name: &str,
        source_family: &str,
        binding_mode: &str,
        input_shape_kind: &str,
        resolver_preset: Option<&str>,
        locator: &serde_json::Value,
        material_format_hint: Option<&str>,
        privacy_policy_id: &str,
        raw_material_policy: &serde_json::Value,
        enabled: bool,
    ) -> DbResult<Uuid> {
        let row: (Uuid,) = sqlx::query_as(
            r#"
            INSERT INTO raw.source_bindings
                (name, source_family, binding_mode, input_shape_kind,
                 resolver_preset, locator, material_format_hint,
                 privacy_policy_id, raw_material_policy, enabled)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
            "#,
        )
        .bind(name)
        .bind(source_family)
        .bind(binding_mode)
        .bind(input_shape_kind)
        .bind(resolver_preset)
        .bind(locator)
        .bind(material_format_hint)
        .bind(privacy_policy_id)
        .bind(raw_material_policy)
        .bind(enabled)
        .fetch_one(self.pool)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("unique") || msg.contains("uk_") || msg.contains("uq_") {
                SinexError::validation(format!("Source binding already exists: {name}"))
            } else {
                db_error(e, "create source binding")
            }
        })?;

        Ok(row.0)
    }

    /// Update the status and error fields of a binding.
    pub async fn update_binding_status(
        &self,
        name: &str,
        status: &str,
        last_error: Option<&str>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE raw.source_bindings
            SET status = $2,
                last_error = $3,
                updated_at = now()
            WHERE name = $1
            "#,
        )
        .bind(name)
        .bind(status)
        .bind(last_error)
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update binding status"))?;

        Ok(())
    }

    /// Record a resolution attempt for a binding.
    pub async fn log_resolution(
        &self,
        source_binding_id: Uuid,
        candidate_count: i32,
        selected_locator: Option<&serde_json::Value>,
        evidence: &serde_json::Value,
        status: &str,
        error_summary: Option<&str>,
    ) -> DbResult<Uuid> {
        let row: (Uuid,) = sqlx::query_as(
            r#"
            INSERT INTO raw.source_binding_resolution_log
                (source_binding_id, candidate_count, selected_locator, evidence, status, error_summary)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            "#,
        )
        .bind(source_binding_id)
        .bind(candidate_count)
        .bind(selected_locator)
        .bind(evidence)
        .bind(status)
        .bind(error_summary)
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "log binding resolution"))?;

        Ok(row.0)
    }

    /// List resolution log entries for a binding.
    pub async fn list_resolution_log(
        &self,
        source_binding_id: Uuid,
    ) -> DbResult<Vec<SourceBindingResolutionLogRow>> {
        sqlx::query_as::<_, SourceBindingResolutionLogRow>(
            r#"
            SELECT
                id,
                source_binding_id,
                resolved_at,
                candidate_count,
                selected_locator,
                evidence,
                status,
                error_summary
            FROM raw.source_binding_resolution_log
            WHERE source_binding_id = $1
            ORDER BY resolved_at DESC
            "#,
        )
        .bind(source_binding_id)
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "list binding resolution log"))
    }
}

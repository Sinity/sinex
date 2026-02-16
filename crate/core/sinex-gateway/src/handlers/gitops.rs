//! GitOps schema source management handlers
//!
//! Provides CRUD operations for managing Git repository sources used by the
//! GitOps schema sync service in `sinex-ingestd`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{SinexError, Ulid};
use sqlx::PgPool;
use tracing::info;

type Result<T> = std::result::Result<T, SinexError>;

// ─── Request/Response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct GitOpsListSourcesRequest {
    #[serde(default)]
    pub include_disabled: bool,
}

#[derive(Debug, Serialize)]
pub struct GitOpsListSourcesResponse {
    pub sources: Vec<GitOpsSourceInfo>,
}

#[derive(Debug, Serialize)]
pub struct GitOpsSourceInfo {
    pub id: Ulid,
    pub repository_url: String,
    pub branch: String,
    pub path_pattern: String,
    pub sync_enabled: bool,
    pub last_sync_at: Option<Timestamp>,
    pub last_sync_commit: Option<String>,
    pub sync_frequency_minutes: i32,
}

#[derive(Debug, Deserialize)]
pub struct GitOpsCreateSourceRequest {
    pub repository_url: String,
    #[serde(default = "default_branch")]
    pub branch: String,
    #[serde(default = "default_path_pattern")]
    pub path_pattern: String,
    #[serde(default = "default_sync_frequency")]
    pub sync_frequency_minutes: i32,
}

fn default_branch() -> String {
    "main".to_string()
}

fn default_path_pattern() -> String {
    "schemas/**/*.json".to_string()
}

fn default_sync_frequency() -> i32 {
    60
}

#[derive(Debug, Serialize)]
pub struct GitOpsCreateSourceResponse {
    pub id: Ulid,
    pub repository_url: String,
    pub branch: String,
    pub path_pattern: String,
}

#[derive(Debug, Deserialize)]
pub struct GitOpsDeleteSourceRequest {
    pub id: Ulid,
}

#[derive(Debug, Serialize)]
pub struct GitOpsDeleteSourceResponse {
    pub deleted: bool,
}

#[derive(Debug, Deserialize)]
pub struct GitOpsTriggerSyncRequest {
    pub id: Ulid,
}

#[derive(Debug, Serialize)]
pub struct GitOpsTriggerSyncResponse {
    pub triggered: bool,
    pub message: String,
}

// ─── Handlers ───────────────────────────────────────────────────────────

/// List all configured gitops sources.
pub async fn handle_gitops_list_sources(pool: &PgPool, params: Value) -> Result<Value> {
    let request: GitOpsListSourcesRequest =
        serde_json::from_value(params).unwrap_or(GitOpsListSourcesRequest {
            include_disabled: false,
        });

    let rows = sqlx::query!(
        r#"
        SELECT
            id::uuid as "id!: Ulid",
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
        request.include_disabled
    )
    .fetch_all(pool)
    .await
    .map_err(|e| SinexError::database("Failed to list gitops sources").with_std_error(&e))?;

    let sources: Vec<GitOpsSourceInfo> = rows
        .into_iter()
        .map(|row| GitOpsSourceInfo {
            id: row.id,
            repository_url: row.repository_url,
            branch: row.branch,
            path_pattern: row.path_pattern,
            sync_enabled: row.sync_enabled,
            last_sync_at: row.last_sync_at,
            last_sync_commit: row.last_sync_commit,
            sync_frequency_minutes: row.sync_frequency_minutes,
        })
        .collect();

    let response = GitOpsListSourcesResponse { sources };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize gitops sources response").with_std_error(&e)
    })
}

/// Create a new gitops source configuration.
pub async fn handle_gitops_create_source(pool: &PgPool, params: Value) -> Result<Value> {
    let request: GitOpsCreateSourceRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid gitops create source request").with_std_error(&e)
    })?;

    // Validate URL: reject file:// scheme
    if request.repository_url.starts_with("file://") {
        return Err(SinexError::validation(
            "file:// URLs are not allowed for gitops sources",
        ));
    }

    if request.repository_url.is_empty() {
        return Err(SinexError::validation("Repository URL cannot be empty"));
    }

    if request.sync_frequency_minutes < 1 {
        return Err(SinexError::validation(
            "Sync frequency must be at least 1 minute",
        ));
    }

    let id = Ulid::new();

    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.gitops_schema_sources (
            id, repository_url, branch, path_pattern,
            sync_enabled, sync_frequency_minutes
        ) VALUES (
            $1::uuid::ulid, $2, $3, $4, true, $5
        )
        "#,
        id.as_uuid(),
        request.repository_url.as_str(),
        request.branch.as_str(),
        request.path_pattern.as_str(),
        request.sync_frequency_minutes,
    )
    .execute(pool)
    .await
    .map_err(|e| SinexError::database("Failed to create gitops source").with_std_error(&e))?;

    info!(
        id = %id,
        url = %request.repository_url,
        branch = %request.branch,
        pattern = %request.path_pattern,
        "Created gitops schema source"
    );

    let response = GitOpsCreateSourceResponse {
        id,
        repository_url: request.repository_url,
        branch: request.branch,
        path_pattern: request.path_pattern,
    };

    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize create source response").with_std_error(&e)
    })
}

/// Delete a gitops source configuration.
pub async fn handle_gitops_delete_source(pool: &PgPool, params: Value) -> Result<Value> {
    let request: GitOpsDeleteSourceRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid gitops delete source request").with_std_error(&e)
    })?;

    let result = sqlx::query!(
        r#"
        DELETE FROM sinex_schemas.gitops_schema_sources
        WHERE id = $1::uuid::ulid
        "#,
        request.id.as_uuid()
    )
    .execute(pool)
    .await
    .map_err(|e| SinexError::database("Failed to delete gitops source").with_std_error(&e))?;

    let deleted = result.rows_affected() > 0;

    if deleted {
        info!(id = %request.id, "Deleted gitops schema source");
    } else {
        return Err(SinexError::not_found(format!(
            "GitOps source not found: {}",
            request.id
        )));
    }

    let response = GitOpsDeleteSourceResponse { deleted };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize delete source response").with_std_error(&e)
    })
}

/// Trigger an immediate sync for a specific source by resetting its last_sync_at.
///
/// The actual sync is performed by the background service in ingestd. This handler
/// just resets the timing so the next poll cycle will pick it up.
pub async fn handle_gitops_trigger_sync(pool: &PgPool, params: Value) -> Result<Value> {
    let request: GitOpsTriggerSyncRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid gitops trigger sync request").with_std_error(&e)
    })?;

    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.gitops_schema_sources
        SET last_sync_at = NULL
        WHERE id = $1::uuid::ulid AND sync_enabled = true
        "#,
        request.id.as_uuid()
    )
    .execute(pool)
    .await
    .map_err(|e| SinexError::database("Failed to trigger gitops sync").with_std_error(&e))?;

    let triggered = result.rows_affected() > 0;
    let message = if triggered {
        info!(id = %request.id, "Triggered immediate gitops sync");
        "Sync triggered — source will be synced on next poll cycle".to_string()
    } else {
        "Source not found or not enabled".to_string()
    };

    let response = GitOpsTriggerSyncResponse { triggered, message };
    serde_json::to_value(response).map_err(|e| {
        SinexError::serialization("Failed to serialize trigger sync response").with_std_error(&e)
    })
}

//! `GitOps` schema source management handlers
//!
//! Provides CRUD operations for managing Git repository sources used by the
//! `GitOps` schema sync service in `sinex-ingestd`.

use serde_json::Value;
use sinex_db::DbPoolExt;
use sinex_primitives::rpc::gitops::{
    GitOpsCreateSourceRequest, GitOpsCreateSourceResponse, GitOpsDeleteSourceRequest,
    GitOpsDeleteSourceResponse, GitOpsListSourcesRequest, GitOpsListSourcesResponse,
    GitOpsSourceInfo, GitOpsTriggerSyncRequest, GitOpsTriggerSyncResponse,
};
use sinex_primitives::SinexError;
use sqlx::PgPool;
use tracing::info;

type Result<T> = std::result::Result<T, SinexError>;

// ─── Handlers ───────────────────────────────────────────────────────────

/// List all configured gitops sources.
pub async fn handle_gitops_list_sources(pool: &PgPool, params: Value) -> Result<Value> {
    let request: GitOpsListSourcesRequest =
        serde_json::from_value(params).unwrap_or(GitOpsListSourcesRequest {
            include_disabled: false,
        });

    let records = pool
        .gitops()
        .list_sources(request.include_disabled)
        .await?;

    let sources: Vec<GitOpsSourceInfo> = records
        .into_iter()
        .map(|r| GitOpsSourceInfo {
            id: r.id,
            repository_url: r.repository_url,
            branch: r.branch,
            path_pattern: r.path_pattern,
            sync_enabled: r.sync_enabled,
            last_sync_at: r.last_sync_at,
            last_sync_commit: r.last_sync_commit,
            sync_frequency_minutes: r.sync_frequency_minutes,
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

    // Validation is handled by the repository
    let id = pool
        .gitops()
        .create_source(
            &request.repository_url,
            &request.branch,
            &request.path_pattern,
            request.sync_frequency_minutes,
        )
        .await?;

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

    let deleted = pool.gitops().delete_source(&request.id).await?;

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

/// Trigger an immediate sync for a specific source by resetting its `last_sync_at`.
///
/// The actual sync is performed by the background service in ingestd. This handler
/// just resets the timing so the next poll cycle will pick it up.
pub async fn handle_gitops_trigger_sync(pool: &PgPool, params: Value) -> Result<Value> {
    let request: GitOpsTriggerSyncRequest = serde_json::from_value(params).map_err(|e| {
        SinexError::serialization("Invalid gitops trigger sync request").with_std_error(&e)
    })?;

    let triggered = pool.gitops().trigger_sync(&request.id).await?;

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

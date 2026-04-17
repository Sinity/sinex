//! Tests for `GitOps` schema source CRUD handlers
//!
//! Validates:
//! - Full CRUD lifecycle (create, list, delete)
//! - Input validation (file:// URL rejection, empty URL, invalid sync frequency)
//! - Trigger sync resets `last_sync_at`

use serde_json::json;
use sinex_gateway::handlers::gitops::{
    handle_gitops_create_source, handle_gitops_delete_source, handle_gitops_list_sources,
    handle_gitops_trigger_sync,
};
use sinex_primitives::rpc::gitops::{
    DEFAULT_GITOPS_PATH_PATTERN, GitOpsCreateSourceResponse, GitOpsListSourcesResponse,
    GitOpsTriggerSyncResponse,
};
use xtask::sandbox::prelude::*;

// ─── CRUD lifecycle ─────────────────────────────────────────────────────

#[sinex_test]
async fn gitops_create_list_delete_lifecycle(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // 1. Create a source
    let create_params = json!({
        "repository_url": "https://github.com/example/schemas.git",
        "branch": "main",
        "path_pattern": DEFAULT_GITOPS_PATH_PATTERN,
        "sync_frequency_minutes": 30,
    });
    let create_result = handle_gitops_create_source(pool, create_params).await?;
    let created: GitOpsCreateSourceResponse = serde_json::from_value(create_result)?;
    assert_eq!(
        created.repository_url,
        "https://github.com/example/schemas.git"
    );
    assert_eq!(created.branch, "main");
    assert_eq!(created.path_pattern, DEFAULT_GITOPS_PATH_PATTERN);

    // 2. List sources and verify the created source is present
    let list_result = handle_gitops_list_sources(pool, json!({})).await?;
    let list: GitOpsListSourcesResponse = serde_json::from_value(list_result)?;
    let found = list.sources.iter().any(|s| s.id == created.id);
    assert!(found, "Created source must appear in list");

    // Verify the source has the correct sync_frequency
    let source = list.sources.iter().find(|s| s.id == created.id).unwrap();
    assert_eq!(source.sync_frequency_minutes, 30);
    assert!(
        source.sync_enabled,
        "Newly created source should be enabled"
    );

    // 3. Delete the source
    let delete_result =
        handle_gitops_delete_source(pool, json!({ "id": created.id.to_string() })).await?;
    let deleted_val = delete_result
        .get("deleted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    assert!(deleted_val, "Delete should return deleted=true");

    // 4. List again and verify it's gone
    let list_after = handle_gitops_list_sources(pool, json!({})).await?;
    let list_after: GitOpsListSourcesResponse = serde_json::from_value(list_after)?;
    let still_found = list_after.sources.iter().any(|s| s.id == created.id);
    assert!(!still_found, "Deleted source must not appear in list");

    Ok(())
}

// ─── Validation: file:// URL rejection ──────────────────────────────────

#[sinex_test]
async fn gitops_rejects_file_url(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let params = json!({
        "repository_url": "file:///etc/passwd",
        "branch": "main",
        "path_pattern": "**/*.json",
        "sync_frequency_minutes": 60,
    });

    let result = handle_gitops_create_source(pool, params).await;
    assert!(result.is_err(), "file:// URLs must be rejected");

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("file://"),
        "Error message should mention file:// rejection, got: {err_msg}"
    );

    Ok(())
}

// ─── Validation: empty URL rejection ────────────────────────────────────

#[sinex_test]
async fn gitops_rejects_empty_url(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let params = json!({
        "repository_url": "",
        "branch": "main",
        "path_pattern": "**/*.json",
        "sync_frequency_minutes": 60,
    });

    let result = handle_gitops_create_source(pool, params).await;
    assert!(result.is_err(), "Empty URL must be rejected");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("empty"),
        "Error should mention empty URL, got: {err_msg}"
    );

    Ok(())
}

// ─── Validation: invalid sync frequency ─────────────────────────────────

#[sinex_test]
async fn gitops_rejects_zero_sync_frequency(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let params = json!({
        "repository_url": "https://github.com/example/schemas.git",
        "branch": "main",
        "path_pattern": "**/*.json",
        "sync_frequency_minutes": 0,
    });

    let result = handle_gitops_create_source(pool, params).await;
    assert!(result.is_err(), "Zero sync frequency must be rejected");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("at least 1"),
        "Error should mention minimum frequency, got: {err_msg}"
    );

    Ok(())
}

#[sinex_test]
async fn gitops_rejects_negative_sync_frequency(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let params = json!({
        "repository_url": "https://github.com/example/schemas.git",
        "branch": "main",
        "path_pattern": "**/*.json",
        "sync_frequency_minutes": -5,
    });

    let result = handle_gitops_create_source(pool, params).await;
    assert!(result.is_err(), "Negative sync frequency must be rejected");

    Ok(())
}

// ─── Trigger sync ───────────────────────────────────────────────────────

#[sinex_test]
async fn gitops_trigger_sync_resets_last_sync_at(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Create a source first
    let create_params = json!({
        "repository_url": "https://github.com/example/trigger-test.git",
        "branch": "main",
        "path_pattern": "**/*.json",
        "sync_frequency_minutes": 60,
    });
    let create_result = handle_gitops_create_source(pool, create_params).await?;
    let created: GitOpsCreateSourceResponse = serde_json::from_value(create_result)?;

    // Trigger sync
    let trigger_result =
        handle_gitops_trigger_sync(pool, json!({ "id": created.id.to_string() })).await?;
    let trigger: GitOpsTriggerSyncResponse = serde_json::from_value(trigger_result)?;
    assert!(
        trigger.triggered,
        "Trigger should succeed for enabled source"
    );
    assert!(
        trigger.message.contains("triggered"),
        "Message should confirm trigger"
    );

    // Verify that last_sync_at is NULL (indicating it needs to sync)
    let list_result = handle_gitops_list_sources(pool, json!({})).await?;
    let list: GitOpsListSourcesResponse = serde_json::from_value(list_result)?;
    let source = list.sources.iter().find(|s| s.id == created.id).unwrap();
    assert!(
        source.last_sync_at.is_none(),
        "After trigger_sync, last_sync_at should be NULL"
    );

    Ok(())
}

// ─── Delete non-existent source ─────────────────────────────────────────

#[sinex_test]
async fn gitops_delete_nonexistent_returns_not_found(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let fake_id = Uuid::now_v7();
    let result = handle_gitops_delete_source(pool, json!({ "id": fake_id.to_string() })).await;

    assert!(result.is_err(), "Deleting non-existent source should fail");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("not found"),
        "Error should indicate not found, got: {err_msg}"
    );

    Ok(())
}

// ─── Trigger sync on non-existent source ────────────────────────────────

#[sinex_test]
async fn gitops_trigger_sync_nonexistent_source(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let fake_id = Uuid::now_v7();
    let trigger_result =
        handle_gitops_trigger_sync(pool, json!({ "id": fake_id.to_string() })).await?;
    let trigger: GitOpsTriggerSyncResponse = serde_json::from_value(trigger_result)?;

    assert!(
        !trigger.triggered,
        "Trigger on non-existent source should not succeed"
    );
    assert!(
        trigger.message.contains("not found") || trigger.message.contains("not enabled"),
        "Message should indicate source not found or not enabled, got: {}",
        trigger.message
    );

    Ok(())
}

// ─── List with include_disabled ─────────────────────────────────────────

#[sinex_test]
async fn gitops_list_sources_defaults_to_enabled_only(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Create a source (will be enabled by default)
    let create_params = json!({
        "repository_url": "https://github.com/example/enabled-test.git",
        "branch": "main",
        "path_pattern": "**/*.json",
        "sync_frequency_minutes": 60,
    });
    handle_gitops_create_source(pool, create_params).await?;

    // Default list (include_disabled=false) should show enabled sources
    let list_result = handle_gitops_list_sources(pool, json!({})).await?;
    let list: GitOpsListSourcesResponse = serde_json::from_value(list_result)?;
    assert!(
        !list.sources.is_empty(),
        "Should list at least the enabled source we just created"
    );

    // All listed sources should be enabled
    for source in &list.sources {
        assert!(
            source.sync_enabled,
            "Default list should only contain enabled sources"
        );
    }

    Ok(())
}

#[sinex_test]
async fn gitops_list_sources_rejects_malformed_params(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    let result = handle_gitops_list_sources(pool, json!({ "include_disabled": "yes" })).await;
    assert!(result.is_err(), "malformed list params must fail");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid gitops list sources request")
    );

    Ok(())
}

// ─── Create with defaults ───────────────────────────────────────────────

#[sinex_test]
async fn gitops_create_source_uses_defaults(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Only provide repository_url, let other fields default
    let params = json!({
        "repository_url": "https://github.com/example/defaults.git",
    });
    let result = handle_gitops_create_source(pool, params).await?;
    let created: GitOpsCreateSourceResponse = serde_json::from_value(result)?;

    assert_eq!(created.branch, "main", "Default branch should be 'main'");
    assert_eq!(
        created.path_pattern, DEFAULT_GITOPS_PATH_PATTERN,
        "Default path pattern should be '{DEFAULT_GITOPS_PATH_PATTERN}'"
    );

    // Verify default sync frequency via list
    let list_result = handle_gitops_list_sources(pool, json!({})).await?;
    let list: GitOpsListSourcesResponse = serde_json::from_value(list_result)?;
    let source = list.sources.iter().find(|s| s.id == created.id).unwrap();
    assert_eq!(
        source.sync_frequency_minutes, 60,
        "Default sync frequency should be 60 minutes"
    );

    Ok(())
}

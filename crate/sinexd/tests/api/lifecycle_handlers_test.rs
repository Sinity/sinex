//! Lifecycle handler regression coverage for persisted audit state and tombstone execution.

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as SourceMaterialRegistration;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::rpc::audit::AuditGetRequest;
use sinex_primitives::rpc::audit::AuditGetResponse;
use sinex_primitives::rpc::lifecycle::{
    LifecycleArchiveRequest, LifecycleArchiveResponse, LifecycleRestoreRequest,
    LifecycleRestoreResponse, TombstoneApproveRequest, TombstoneApproveResponse,
    TombstoneCancelRequest, TombstoneCancelResponse, TombstoneCreateRequest,
    TombstoneCreateResponse, TombstoneListRequest, TombstoneListResponse, TombstoneOperationState,
    TombstoneStatusRequest, TombstoneStatusResponse,
};
use sinexd::api::handlers::{
    handle_audit_get as handle_audit_get_typed,
    handle_lifecycle_archive as handle_lifecycle_archive_typed,
    handle_lifecycle_restore as handle_lifecycle_restore_typed,
    handle_tombstone_approve as handle_tombstone_approve_typed,
    handle_tombstone_cancel as handle_tombstone_cancel_typed,
    handle_tombstone_create as handle_tombstone_create_typed,
    handle_tombstone_list as handle_tombstone_list_typed,
    handle_tombstone_status as handle_tombstone_status_typed,
};
use sinexd::api::rpc_server::RpcAuthContext;
use sinexd::api::service_container::ServiceContainer;
use xtask::sandbox::prelude::*;

async fn handle_audit_get(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
) -> TestResult<serde_json::Value> {
    let request: AuditGetRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_audit_get_typed(pool, request).await?,
    )?)
}

async fn handle_lifecycle_archive(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: LifecycleArchiveRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_lifecycle_archive_typed(pool, request, auth).await?,
    )?)
}

async fn handle_lifecycle_restore(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: LifecycleRestoreRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_lifecycle_restore_typed(pool, request, auth).await?,
    )?)
}

async fn handle_tombstone_create(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: TombstoneCreateRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_tombstone_create_typed(pool, request, auth).await?,
    )?)
}

async fn handle_tombstone_approve(
    params: serde_json::Value,
    services: &ServiceContainer,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: TombstoneApproveRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_tombstone_approve_typed(services, request, auth).await?,
    )?)
}

async fn handle_tombstone_cancel(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: TombstoneCancelRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_tombstone_cancel_typed(pool, request, auth).await?,
    )?)
}

async fn handle_tombstone_list(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: TombstoneListRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_tombstone_list_typed(pool, request, auth).await?,
    )?)
}

async fn handle_tombstone_status(
    pool: &sqlx::PgPool,
    params: serde_json::Value,
    auth: &RpcAuthContext,
) -> TestResult<serde_json::Value> {
    let request: TombstoneStatusRequest = serde_json::from_value(params)?;
    Ok(serde_json::to_value(
        handle_tombstone_status_typed(pool, request, auth).await?,
    )?)
}

async fn publish_event(
    ctx: &TestContext,
    source: &str,
    sequence: i64,
) -> TestResult<sinex_primitives::events::Event<serde_json::Value>> {
    let material_id = ctx.create_source_material(Some(source)).await?;
    Ok(ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(source, "test.lifecycle", json!({ "sequence": sequence }))
                .from_material(material_id)
                .build()?,
        )
        .await?)
}

async fn archived_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint as "count!" FROM audit.archived_events WHERE id = $1::uuid"#,
        event_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?)
}

async fn tombstone_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint as "count!" FROM core.event_tombstones WHERE id = $1::uuid"#,
        event_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?)
}

async fn archived_annotation_count(ctx: &TestContext, event_id: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint as "count!" FROM audit.archived_annotations WHERE event_id = $1::uuid"#,
        event_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?)
}

/// sinex-kwwt: `execute_cascade_tombstone` (apply.rs) inserts a tombstone
/// skeleton row and deletes from `audit.archived_events`, but never touches
/// `audit.archived_annotations`, `audit.archived_embeddings`, or
/// `audit.archived_tagged_items` -- unlike its non-destructive sibling
/// `execute_cascade_restore`, which cleans up all three. A permanent purge
/// (the exact operation an operator uses to remove sensitive data) leaves
/// operator-authored annotation content behind forever, orphaned and
/// unreachable by any later purge attempt.
#[sinex_test]
async fn tombstone_approve_purges_archived_annotation_content(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let source = "test.lifecycle.tombstone-annotation-leak";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    // An operator-curated annotation on the live event, before it is
    // archived+tombstoned.
    sqlx::query!(
        r#"
        INSERT INTO core.event_annotations (event_id, annotation_type, content, created_by)
        VALUES ($1::uuid, 'note', 'sensitive operator note that must be purgeable', 'test:operator')
        "#,
        event_id.parse::<uuid::Uuid>()?,
    )
    .execute(ctx.pool())
    .await?;

    handle_lifecycle_archive(
        ctx.pool(),
        json!({
            "event_ids": [event_id.clone()],
            "dry_run": false,
            "reason": "prepare tombstone with an annotated event",
        }),
        &auth,
    )
    .await?;
    // Archiving copies the annotation into audit.archived_annotations.
    assert_eq!(archived_annotation_count(&ctx, &event_id).await?, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "preview annotated event for permanent purge",
            }),
            &auth,
        )
        .await?,
    )?;

    handle_tombstone_approve(
        json!({
            "operation_id": create.operation.operation_id,
            "yes_i_understand_data_is_gone": true,
        }),
        &services,
        &auth,
    )
    .await?;

    assert_eq!(
        archived_annotation_count(&ctx, &event_id).await?,
        0,
        "sinex-kwwt: a permanent purge (tombstone approve) must remove the archived annotation \
         content too, not just the archived_events skeleton row -- otherwise operator-curated \
         content the purge was specifically meant to remove survives forever, orphaned"
    );

    Ok(())
}

#[sinex_test]
async fn archive_and_restore_operations_are_persisted_and_auditable(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let event = publish_event(&ctx, "test.lifecycle.archive", 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive_value = handle_lifecycle_archive(
        ctx.pool(),
        json!({
            "event_ids": [event_id.clone()],
            "dry_run": false,
            "reason": "archive regression test",
        }),
        &auth,
    )
    .await?;
    let archive: LifecycleArchiveResponse = serde_json::from_value(archive_value)?;
    assert_eq!(archive.archived_count, 1);

    let archive_audit: AuditGetResponse = serde_json::from_value(
        handle_audit_get(ctx.pool(), json!({ "operation_id": archive.operation_id })).await?,
    )?;
    assert_eq!(archive_audit.event_count, 1);
    assert_eq!(
        archive_audit.audit_trail.operation.operator,
        auth.actor_id()
    );
    assert_eq!(
        archive_audit.audit_trail.affected_events[0].id.to_string(),
        event_id
    );
    assert_eq!(archived_count(&ctx, &event_id).await?, 1);

    let restore_value = handle_lifecycle_restore(
        ctx.pool(),
        json!({
            "event_ids": [event_id.clone()],
            "dry_run": false,
        }),
        &auth,
    )
    .await?;
    let restore: LifecycleRestoreResponse = serde_json::from_value(restore_value)?;
    assert_eq!(restore.restored_count, 1);

    let restore_audit: AuditGetResponse = serde_json::from_value(
        handle_audit_get(ctx.pool(), json!({ "operation_id": restore.operation_id })).await?,
    )?;
    assert_eq!(restore_audit.event_count, 1);
    assert_eq!(
        restore_audit.audit_trail.operation.operator,
        auth.actor_id()
    );
    assert_eq!(
        restore_audit.audit_trail.affected_events[0].id.to_string(),
        event_id
    );
    assert_eq!(archived_count(&ctx, &event_id).await?, 0);

    Ok(())
}

#[sinex_test]
async fn tombstone_approve_uses_previewed_event_set_and_audits_tombstones(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let source = "test.lifecycle.tombstone";
    let first = publish_event(&ctx, source, 1).await?;
    let first_id = first
        .id
        .expect("published first event should have an id")
        .to_string();

    let archive_first: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [first_id.clone()],
                "dry_run": false,
                "reason": "prepare tombstone preview",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive_first.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "preview exact archived set",
            }),
            &auth,
        )
        .await?,
    )?;

    let second = publish_event(&ctx, source, 2).await?;
    let second_id = second
        .id
        .expect("published second event should have an id")
        .to_string();
    let archive_second: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [second_id.clone()],
                "dry_run": false,
                "reason": "introduce later archived sibling",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive_second.archived_count, 1);

    let approve: TombstoneApproveResponse = serde_json::from_value(
        handle_tombstone_approve(
            json!({
                "operation_id": create.operation.operation_id,
                "yes_i_understand_data_is_gone": true,
            }),
            &services,
            &auth,
        )
        .await?,
    )?;
    assert_eq!(approve.operation.tombstoned_count, Some(1));
    assert_eq!(approve.operation.created_by, auth.actor_id());
    assert_eq!(
        approve.operation.approved_by.as_deref(),
        Some(auth.actor_id())
    );

    let audit: AuditGetResponse = serde_json::from_value(
        handle_audit_get(
            ctx.pool(),
            json!({ "operation_id": approve.operation.operation_id }),
        )
        .await?,
    )?;
    assert_eq!(audit.event_count, 1);
    assert_eq!(
        audit.audit_trail.affected_events[0].id.to_string(),
        first_id
    );
    assert_eq!(archived_count(&ctx, &first_id).await?, 0);
    assert_eq!(tombstone_count(&ctx, &first_id).await?, 1);
    assert_eq!(archived_count(&ctx, &second_id).await?, 1);
    assert_eq!(tombstone_count(&ctx, &second_id).await?, 0);

    Ok(())
}

/// Verify the #987 delete-on-tombstone path end-to-end:
///
/// 1. Create a source material with no events.
/// 2. Publish an event referencing the material (live).
/// 3. Confirm the material is NOT orphan (live event references it).
/// 4. Archive the event.
/// 5. Confirm the material is NOT orphan (archived event still references it).
/// 6. Tombstone via handle_tombstone_approve.
/// 7. Confirm the material registry row is gone (delete-on-tombstone fired
///    because there are no remaining references in core.events or
///    audit.archived_events).
///
/// This exercises the full delete-on-tombstone wiring: the cleanup block in
/// handle_tombstone_approve, the new repository methods
/// (material_ids_for_archived_events, find_orphan_materials, delete_material),
/// and the orphan-detection SQL.
#[sinex_test]
async fn tombstone_approve_deletes_orphan_source_material(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let source = "test.lifecycle.tombstone.delete-on-tombstone";

    // Stage 1+2: Create material + publish event referencing it.
    let material_id = ctx.create_source_material(Some(source)).await?;
    let event = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(source, "test.lifecycle.dot", json!({ "kind": "fixture" }))
                .from_material(material_id)
                .build()?,
        )
        .await?;
    let event_id = event.id.expect("inserted event must have id").to_string();

    // Stage 3: Material is NOT orphan while the event is live.
    let materials = ctx.pool().source_materials();
    let live_orphans = materials
        .find_orphan_materials(&[material_id.to_uuid()])
        .await?;
    assert!(
        live_orphans.is_empty(),
        "material with a live event must not be reported as orphan"
    );

    // Stage 4: Archive the event.
    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id.clone()],
                "dry_run": false,
                "reason": "delete-on-tombstone test: archive before tombstone",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    // Stage 5: Material is still NOT orphan — archived event references it.
    let archived_orphans = materials
        .find_orphan_materials(&[material_id.to_uuid()])
        .await?;
    assert!(
        archived_orphans.is_empty(),
        "material with an archived event must not be reported as orphan"
    );

    // Create a tombstone preview that includes our archived event.
    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "delete-on-tombstone test: preview",
            }),
            &auth,
        )
        .await?,
    )?;

    // Sanity: the material registry row exists right before tombstone.
    let row_before = materials
        .get_by_id(sinex_primitives::Id::from_uuid(material_id.to_uuid()))
        .await?;
    assert!(
        row_before.is_some(),
        "material registry row must exist before tombstone"
    );

    // Stage 6: Approve the tombstone — this triggers delete-on-tombstone.
    let approve: TombstoneApproveResponse = serde_json::from_value(
        handle_tombstone_approve(
            json!({
                "operation_id": create.operation.operation_id,
                "yes_i_understand_data_is_gone": true,
            }),
            &services,
            &auth,
        )
        .await?,
    )?;
    assert_eq!(
        approve.operation.tombstoned_count,
        Some(1),
        "exactly one event tombstoned"
    );

    // Stage 7: The material registry row is gone.
    let row_after = materials
        .get_by_id(sinex_primitives::Id::from_uuid(material_id.to_uuid()))
        .await?;
    assert!(
        row_after.is_none(),
        "material registry row must be deleted by delete-on-tombstone path"
    );

    // And event_tombstones records the deletion (sanity check on the cascade).
    assert_eq!(tombstone_count(&ctx, &event_id).await?, 1);

    Ok(())
}

/// Companion test: when an event references material that is ALSO referenced
/// by a separate live event, tombstoning the first event must NOT delete
/// the material — the second event still depends on it.
#[sinex_test]
async fn tombstone_approve_preserves_material_with_other_references(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let source = "test.lifecycle.tombstone.preserve-shared";

    let material_id = ctx.create_source_material(Some(source)).await?;

    let first = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(source, "test.lifecycle.share", json!({ "n": 1 }))
                .from_material(material_id)
                .build()?,
        )
        .await?;
    let first_id = first.id.expect("first event id").to_string();

    let _second = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(source, "test.lifecycle.share", json!({ "n": 2 }))
                .from_material(material_id)
                .build()?,
        )
        .await?;

    // Archive only the first event.
    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [first_id.clone()],
                "dry_run": false,
                "reason": "preserve-shared test: archive first only",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "preserve-shared test: preview",
            }),
            &auth,
        )
        .await?,
    )?;

    let approve: TombstoneApproveResponse = serde_json::from_value(
        handle_tombstone_approve(
            json!({
                "operation_id": create.operation.operation_id,
                "yes_i_understand_data_is_gone": true,
            }),
            &services,
            &auth,
        )
        .await?,
    )?;
    assert_eq!(approve.operation.tombstoned_count, Some(1));

    // Material registry row MUST still exist — the second live event references it.
    let materials = ctx.pool().source_materials();
    let row = materials
        .get_by_id(sinex_primitives::Id::from_uuid(material_id.to_uuid()))
        .await?;
    assert!(
        row.is_some(),
        "material registry row must survive tombstone when other events still reference it"
    );

    Ok(())
}

/// Regression test for sinex-audit-cas-shared-blob-delete: content-addressed
/// dedup means two DIFFERENT `raw.source_material_registry` rows can point at
/// the same `core.blobs` row (no UNIQUE constraint on `optional_blob_id`; this
/// is exactly what `sources.stage --with-bytes` produces when re-staging
/// identical content -- a new material row every time, but one shared blob).
///
/// Tombstone material A's only event (orphaning material A) while material B
/// -- sharing the same blob -- still has a live event. Before the fix,
/// delete-on-tombstone dropped the CAS content unconditionally once it found
/// ANY orphaned material pointing at the blob, destroying content material B
/// still depends on. After the fix, the blob is only dropped once its
/// reference count is genuinely zero.
#[sinex_test]
async fn tombstone_approve_preserves_shared_blob_content_for_live_sibling_material(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let source = "test.lifecycle.tombstone.shared-blob-survives";

    // Ingest once: this is the one core.blobs row two independently-staged
    // materials will both reference (mirrors what content-addressed dedup
    // does across two separate `sources.stage --with-bytes` calls).
    let content_store = services.content.content_store();
    let payload = b"shared-blob-survives-sibling regression payload";
    let blob = content_store
        .ingest_from_bytes(payload, "shared-blob.bin", "application/octet-stream")
        .await?;
    let content_key = blob.content_key();

    let materials = ctx.pool().source_materials();
    let material_a = materials
        .register_material(
            SourceMaterialRegistration::blob_binary("shared-blob-a.bin").with_blob_id(blob.id),
        )
        .await?;
    let material_b = materials
        .register_material(
            SourceMaterialRegistration::blob_binary("shared-blob-b.bin").with_blob_id(blob.id),
        )
        .await?;
    let material_a_id = sinex_primitives::Id::from_uuid(material_a.id);
    let material_b_id = sinex_primitives::Id::from_uuid(material_b.id);

    // A live event for each material -- both are legitimately reachable.
    let event_a = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(
                source,
                "test.lifecycle.shared-blob.a",
                json!({ "which": "a" }),
            )
            .from_material(material_a_id)
            .build()?,
        )
        .await?;
    let event_a_id = event_a.id.expect("inserted event must have id").to_string();

    let _event_b = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(
                source,
                "test.lifecycle.shared-blob.b",
                json!({ "which": "b" }),
            )
            .from_material(material_b_id)
            .build()?,
        )
        .await?;

    // Archive + tombstone ONLY event A, so material A (but not material B)
    // becomes an orphan candidate.
    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_a_id.clone()],
                "dry_run": false,
                "reason": "shared-blob test: archive A only",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "shared-blob test: preview",
            }),
            &auth,
        )
        .await?,
    )?;

    let approve: TombstoneApproveResponse = serde_json::from_value(
        handle_tombstone_approve(
            json!({
                "operation_id": create.operation.operation_id,
                "yes_i_understand_data_is_gone": true,
            }),
            &services,
            &auth,
        )
        .await?,
    )?;
    assert_eq!(approve.operation.tombstoned_count, Some(1));

    // Material A is gone (delete-on-tombstone still fires for the genuinely
    // orphaned material row).
    let row_a_after = materials
        .get_by_id(sinex_primitives::Id::from_uuid(material_a.id))
        .await?;
    assert!(
        row_a_after.is_none(),
        "orphaned material A's registry row must still be deleted"
    );

    // Material B is untouched -- its own event is still live.
    let row_b_after = materials
        .get_by_id(sinex_primitives::Id::from_uuid(material_b.id))
        .await?;
    assert!(
        row_b_after.is_some(),
        "material B must survive: its own event is still live"
    );

    // The shared blob row must survive -- material B still references it.
    let blob_row_after = ctx.pool().blobs().get_by_id(blob.id).await?;
    assert!(
        blob_row_after.is_some(),
        "shared blob row must NOT be deleted while material B still references it"
    );

    // And the CAS content itself must survive and remain retrievable --
    // this is the actual data-loss surface: material B's future
    // retrieve_content/replay must not fail with content missing.
    let retrieved = content_store.retrieve_content(&content_key).await?;
    assert_eq!(
        retrieved, payload,
        "shared CAS content must survive delete-on-tombstone while a live sibling references it"
    );

    Ok(())
}

/// Companion to the shared-blob-survives test above: once the LAST reference
/// to a blob is genuinely gone (no sibling material, no live/archived event),
/// delete-on-tombstone must actually remove both the CAS content and the
/// `core.blobs` row -- otherwise the row survives forever as a zombie that
/// later fools dedup on re-ingestion (sinex-audit-cas-zombie-blob-rows).
#[sinex_test]
async fn tombstone_approve_deletes_blob_row_once_last_reference_is_gone(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let source = "test.lifecycle.tombstone.blob-row-cleanup";

    let content_store = services.content.content_store();
    let payload = b"blob-row-cleanup-on-last-reference regression payload";
    let blob = content_store
        .ingest_from_bytes(payload, "cleanup-blob.bin", "application/octet-stream")
        .await?;
    let content_key = blob.content_key();

    let materials = ctx.pool().source_materials();
    let material = materials
        .register_material(
            SourceMaterialRegistration::blob_binary("cleanup-blob.bin").with_blob_id(blob.id),
        )
        .await?;
    let material_id = sinex_primitives::Id::from_uuid(material.id);

    let event = ctx
        .pool()
        .events()
        .insert(
            DynamicPayload::new(source, "test.lifecycle.blob-cleanup", json!({ "n": 1 }))
                .from_material(material_id)
                .build()?,
        )
        .await?;
    let event_id = event.id.expect("inserted event must have id").to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "blob-row-cleanup test: archive",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let create: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "blob-row-cleanup test: preview",
            }),
            &auth,
        )
        .await?,
    )?;

    let approve: TombstoneApproveResponse = serde_json::from_value(
        handle_tombstone_approve(
            json!({
                "operation_id": create.operation.operation_id,
                "yes_i_understand_data_is_gone": true,
            }),
            &services,
            &auth,
        )
        .await?,
    )?;
    assert_eq!(approve.operation.tombstoned_count, Some(1));

    // Material row is gone (unchanged behavior).
    let row_after = materials
        .get_by_id(sinex_primitives::Id::from_uuid(material.id))
        .await?;
    assert!(row_after.is_none(), "orphaned material row must be deleted");

    // The blob row itself must ALSO be gone now — it has zero remaining
    // references. Before the fix, core.blobs rows were never deleted
    // anywhere, leaving a permanent zombie row behind.
    let blob_row_after = ctx.pool().blobs().get_by_id(blob.id).await?;
    assert!(
        blob_row_after.is_none(),
        "dereferenced blob row must be deleted, not left as a zombie"
    );

    // And the CAS content is actually gone from disk.
    let retrieve_result = content_store.retrieve_content(&content_key).await;
    assert!(
        retrieve_result.is_err(),
        "CAS content must be genuinely dropped once the last reference is gone"
    );

    Ok(())
}

#[sinex_test]
async fn tombstone_cancel_persists_terminal_metadata(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.cancel";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "cancel me",
            }),
            &auth,
        )
        .await?,
    )?;

    let cancelled: TombstoneCancelResponse = serde_json::from_value(
        handle_tombstone_cancel(
            ctx.pool(),
            json!({
                "operation_id": created.operation.operation_id,
                "reason": "operator requested stop",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(cancelled.status, "cancelled");

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": created.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Cancelled);
    assert_eq!(status.operation.created_by, auth.actor_id());
    assert!(status.operation.finished_at.is_some());
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Cancelled by system:local: operator requested stop")
    );

    let persisted_duration_ms: i32 = sqlx::query_scalar!(
        r#"SELECT duration_ms as "duration_ms!" FROM core.operations_log WHERE id = $1::uuid"#,
        created.operation.operation_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?;
    assert!(persisted_duration_ms >= 0);

    Ok(())
}

#[sinex_test]
async fn tombstone_expiry_persists_terminal_metadata(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.expiry";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone expiry",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "expire me",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(scope, '{expires_at}', to_jsonb($2::text), false)
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": created.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Expired);
    assert!(status.operation.finished_at.is_some());
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Expired before approval")
    );

    let persisted_duration_ms: i32 = sqlx::query_scalar!(
        r#"SELECT duration_ms as "duration_ms!" FROM core.operations_log WHERE id = $1::uuid"#,
        created.operation.operation_id.parse::<uuid::Uuid>()?
    )
    .fetch_one(ctx.pool())
    .await?;
    assert!(persisted_duration_ms >= 0);

    Ok(())
}

/// sinex-9djc: `handle_tombstone_approve`'s completion write (step 3) is a
/// separate DB write made AFTER the irreversible deletion (step 2, inside
/// `execute_cascade_tombstone`) has already committed. If step 3 never lands
/// (crash, pool exhaustion, network blip between steps 2 and 3), the
/// operation is stuck at `Executing` forever -- `is_terminal()` is false and
/// there is no retry/cancel path for that state. Once the fixed 1-hour TTL
/// set at preview time elapses, `reconcile_tombstone_expiry` silently
/// relabels the stuck row `Expired` with `error_details: "Expired before
/// approval"` -- a factually false statement, since the deletion already
/// happened.
///
/// This test reproduces the stuck end-state directly: it runs the real
/// approve flow to a genuine, verified completion (so the deletion is real,
/// not simulated), then rewrites the persisted operation back to the
/// `Executing`/lapsed-TTL shape step 3's failure would have left behind --
/// the same "rewrite `operations_log.scope` via SQL to force a lapsed TTL"
/// technique `tombstone_expiry_persists_terminal_metadata` above uses for
/// the "never approved" case.
///
/// Expected to FAIL against current code: `reconcile_tombstone_expiry` has
/// no way to distinguish "never started" from "deletion committed,
/// completion write lost" and unconditionally relabels any lapsed-TTL
/// non-terminal operation as Expired/"Expired before approval".
#[sinex_test]
#[ignore = "sinex-9djc open: reconcile_tombstone_expiry has no way to distinguish \
'never started' from 'deletion committed, completion write lost' -- fails until fixed"]
async fn tombstone_status_does_not_mislabel_completed_deletion_as_expired(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let services = ServiceContainer::from_database_url(ctx.database_url().to_string()).await?;
    let source = "test.lifecycle.tombstone.stuck-executing";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare stuck-executing repro",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "stuck-executing repro",
            }),
            &auth,
        )
        .await?,
    )?;

    // Run the real approve flow to genuine completion: step 2
    // (execute_cascade_tombstone) truly commits the deletion here, exactly
    // as it would in the production failure this bead describes.
    let approved: TombstoneApproveResponse = serde_json::from_value(
        handle_tombstone_approve(
            json!({
                "operation_id": created.operation.operation_id,
                "yes_i_understand_data_is_gone": true,
            }),
            &services,
            &auth,
        )
        .await?,
    )?;
    assert_eq!(approved.operation.tombstoned_count, Some(1));
    assert_eq!(
        tombstone_count(&ctx, &event_id).await?,
        1,
        "deletion (step 2) must have genuinely committed for this repro to be meaningful"
    );

    // Rewrite the persisted operation back to the stuck-Executing shape
    // step 3's failure would have left behind: phase=executing, no
    // finished_at/error_details, TTL lapsed.
    //
    // operation_record_to_tombstone() treats `phase` as the canonical
    // field and overwrites `state` from it on every read
    // (`operation.state = operation.phase.into()`), so mutating `state`
    // alone is a no-op against the real read path -- `phase` is what
    // must be rewritten to reach reconcile_tombstone_expiry's
    // `!operation.state.is_terminal()` guard with Executing.
    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(
            jsonb_set(
                jsonb_set(
                    jsonb_set(scope, '{phase}', to_jsonb('executing'::text), false),
                    '{state}', to_jsonb('executing'::text), false
                ),
                '{expires_at}', to_jsonb($2::text), false
            ),
            '{finished_at}', 'null'::jsonb, false
        )
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": created.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;

    // The deletion already happened -- status must not claim it "expired
    // before approval". This is the single most destructive path in the
    // system; telling an operator the destructive op never went through
    // when it actually did is worse than saying nothing.
    assert_ne!(
        status.operation.error_details.as_deref(),
        Some("Expired before approval"),
        "deletion already committed; status must not claim the operation never happened"
    );

    Ok(())
}

#[sinex_test]
async fn tombstone_cancel_rejects_expired_operation_and_keeps_expired_state(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.cancel-expired";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare expired cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "expire before cancel",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(scope, '{expires_at}', to_jsonb($2::text), false)
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let error = handle_tombstone_cancel(
        ctx.pool(),
        json!({
            "operation_id": created.operation.operation_id,
            "reason": "too late",
        }),
        &auth,
    )
    .await
    .expect_err("expired tombstone operation should not be cancellable");
    assert!(
        error.to_string().contains("has expired"),
        "unexpected error: {error}"
    );

    let status: TombstoneStatusResponse = serde_json::from_value(
        handle_tombstone_status(
            ctx.pool(),
            json!({ "operation_id": created.operation.operation_id }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(status.operation.state, TombstoneOperationState::Expired);
    assert_eq!(
        status.operation.error_details.as_deref(),
        Some("Expired before approval")
    );

    Ok(())
}

#[sinex_test]
async fn tombstone_cancel_rejects_invalid_created_at_metadata(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.cancel-invalid-created-at";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare invalid created_at cancel",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "cancel with corrupt metadata",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(scope, '{created_at}', to_jsonb($2::text), false)
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "not-a-timestamp"
    )
    .execute(ctx.pool())
    .await?;

    let error = handle_tombstone_cancel(
        ctx.pool(),
        json!({
            "operation_id": created.operation.operation_id,
            "reason": "operator requested stop",
        }),
        &auth,
    )
    .await
    .expect_err("invalid created_at should fail honestly");
    assert!(error.to_string().contains("invalid created_at"));

    Ok(())
}

#[sinex_test]
async fn tombstone_status_rejects_invalid_created_at_during_expiry(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.expiry-invalid-created-at";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare invalid created_at expiry",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let created: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "reason": "expire with corrupt metadata",
            }),
            &auth,
        )
        .await?,
    )?;

    sqlx::query!(
        r#"
        UPDATE core.operations_log
        SET scope = jsonb_set(
                jsonb_set(scope, '{created_at}', to_jsonb($2::text), false),
                '{expires_at}',
                to_jsonb($3::text),
                false
            )
        WHERE id = $1::uuid
        "#,
        created.operation.operation_id.parse::<uuid::Uuid>()?,
        "not-a-timestamp",
        "2000-01-01T00:00:00Z"
    )
    .execute(ctx.pool())
    .await?;

    let error = handle_tombstone_status(
        ctx.pool(),
        json!({ "operation_id": created.operation.operation_id }),
        &auth,
    )
    .await
    .expect_err("invalid created_at should fail honestly during expiry reconciliation");
    assert!(error.to_string().contains("invalid created_at"));

    Ok(())
}

#[sinex_test]
async fn tombstone_list_state_filter_applies_before_limit(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = "test.lifecycle.tombstone.list";
    let event = publish_event(&ctx, source, 1).await?;
    let event_id = event
        .id
        .expect("published event should have an id")
        .to_string();

    let archive: LifecycleArchiveResponse = serde_json::from_value(
        handle_lifecycle_archive(
            ctx.pool(),
            json!({
                "event_ids": [event_id],
                "dry_run": false,
                "reason": "prepare tombstone list regression",
            }),
            &auth,
        )
        .await?,
    )?;
    assert_eq!(archive.archived_count, 1);

    let cancelled: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "cancelled tombstone operation",
            }),
            &auth,
        )
        .await?,
    )?;
    let _: TombstoneCancelResponse = serde_json::from_value(
        handle_tombstone_cancel(
            ctx.pool(),
            json!({
                "operation_id": cancelled.operation.operation_id,
                "reason": "regression filter target",
            }),
            &auth,
        )
        .await?,
    )?;

    let previewed: TombstoneCreateResponse = serde_json::from_value(
        handle_tombstone_create(
            ctx.pool(),
            json!({
                "source": source,
                "limit": 1,
                "reason": "newer previewed tombstone operation",
            }),
            &auth,
        )
        .await?,
    )?;

    let listed: TombstoneListResponse = serde_json::from_value(
        handle_tombstone_list(
            ctx.pool(),
            json!({
                "state": "cancelled",
                "limit": 1,
            }),
            &auth,
        )
        .await?,
    )?;

    assert_eq!(listed.operations.len(), 1);
    assert_eq!(
        listed.operations[0].operation_id, cancelled.operation.operation_id,
        "state filter should be applied before the result limit"
    );
    assert_eq!(
        listed.operations[0].state,
        TombstoneOperationState::Cancelled
    );
    assert_ne!(
        listed.operations[0].operation_id, previewed.operation.operation_id,
        "newer previewed rows must not hide older cancelled rows"
    );

    Ok(())
}

#[sinex_test]
async fn tombstone_list_fails_on_malformed_persisted_scope(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    ctx.pool()
        .state()
        .start_operation(
            "tombstone",
            "tester",
            json!({ "not": "a tombstone operation" }),
        )
        .await?;

    let error = handle_tombstone_list(ctx.pool(), json!({ "limit": 10 }), &auth)
        .await
        .expect_err("malformed tombstone rows must fail loudly");
    assert!(
        error.to_string().contains("malformed scope"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[sinex_test]
async fn lifecycle_archive_rejects_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_lifecycle_archive(
        ctx.pool(),
        json!({
            "source": "test.lifecycle.invalid-limit",
            "limit": 0,
            "dry_run": true,
        }),
        &auth,
    )
    .await
    .expect_err("archive should reject non-positive limits");
    assert!(
        error
            .to_string()
            .contains("lifecycle.archive limit must be positive")
    );
    Ok(())
}

#[sinex_test]
async fn lifecycle_archive_rejects_conflicting_explicit_event_filters(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_lifecycle_archive(
        ctx.pool(),
        json!({
            "event_ids": ["00000000-0000-0000-0000-000000000001"],
            "source": "test.lifecycle.conflict",
            "before": "30d",
            "dry_run": true,
            "reason": "conflicting archive filters",
        }),
        &auth,
    )
    .await
    .expect_err("archive should reject conflicting explicit event-id filters");
    assert!(
        error
            .to_string()
            .contains("does not allow `event_ids` together with `source` or `before`")
    );
    Ok(())
}

#[sinex_test]
async fn tombstone_create_rejects_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_tombstone_create(
        ctx.pool(),
        json!({
            "source": "test.lifecycle.invalid-limit",
            "limit": -1,
            "reason": "reject invalid limit",
        }),
        &auth,
    )
    .await
    .expect_err("tombstone create should reject non-positive limits");
    assert!(
        error
            .to_string()
            .contains("lifecycle.tombstone.create limit must be positive")
    );
    Ok(())
}

#[sinex_test]
async fn tombstone_create_rejects_conflicting_explicit_event_filters(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_tombstone_create(
        ctx.pool(),
        json!({
            "event_ids": ["00000000-0000-0000-0000-000000000001"],
            "source": "test.lifecycle.conflict",
            "before": "30d",
            "reason": "conflicting tombstone filters",
        }),
        &auth,
    )
    .await
    .expect_err("tombstone create should reject conflicting explicit event-id filters");
    assert!(
        error
            .to_string()
            .contains("does not allow `event_ids` together with `source` or `before`")
    );
    Ok(())
}

#[sinex_test]
async fn tombstone_list_rejects_non_positive_limits(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let error = handle_tombstone_list(ctx.pool(), json!({ "limit": 0 }), &auth)
        .await
        .expect_err("tombstone list should reject non-positive limits");
    assert!(
        error
            .to_string()
            .contains("lifecycle.tombstone.list limit must be positive")
    );
    Ok(())
}

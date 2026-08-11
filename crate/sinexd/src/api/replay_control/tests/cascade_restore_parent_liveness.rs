//! Regression coverage for the derived-row parent-liveness guard in
//! `core.execute_cascade_restore` (sinex-79is).
//!
//! Before this fix, the occurrence-safety guard added for #2194 F2 only
//! covered material rows: a material root whose occurrence
//! `(source_material_id, anchor_byte)` was re-emitted (new id, same
//! occurrence) before an archived cascade got restored correctly stayed
//! archived. But a DERIVED child of that same material, archived in the
//! same cascade, had no equivalent check and was restored unconditionally
//! -- producing a live derived event whose only parent exists solely in
//! `audit.archived_events`. Because the restored derived row still carries
//! its original `equivalence_key`, the automaton's correct fresh
//! recomputation from the re-emitted material parent gets rejected at
//! admission as `OccurrenceDuplicate`, permanently letting the stale
//! derivation win.
use super::*;

/// Exercises `EventRepository::execute_cascade_restore` directly (the same
/// method the replay engine's cancel/failure compensation paths call),
/// without the full replay-control/NATS apparatus.
#[sinex_test]
async fn cascade_restore_skips_derived_row_whose_parent_was_reemitted(
    ctx: TestContext,
) -> Result<()> {
    let material_id = ctx
        .create_source_material(Some("cascade-restore-parent-liveness"))
        .await?;

    // m1: the original material root, archived alongside its derived child.
    let m1 = DynamicPayload::new(
        "cascade-restore-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/cascade-restore-m1.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let m1_inserted = ctx.pool.events().insert(m1).await?;
    let m1_id = m1_inserted.id.expect("m1 must have id").to_uuid();

    let product_class = DerivedProductClass::CanonicalDerivedEvent;
    seed_product_declaration(
        &ctx.pool,
        "sinex.test.cascade_restore_parent_liveness",
        product_class,
        "cascade-restore-derived-test",
        "analytics.summary",
    )
    .await?;

    // d1: a derived child of m1, archived in the same cascade.
    let mut d1 = DynamicPayload::new(
        "cascade-restore-derived-test",
        "analytics.summary",
        json!({ "path": "/tmp/cascade-restore-d1.txt" }),
    )
    .from_parents([m1_inserted.id.expect("m1 must have id")])?
    .build()?;
    d1.product_class = Some(product_class);
    d1.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    d1.derivation_declaration_id = Some("sinex.test.cascade_restore_parent_liveness".to_string());
    let d1_inserted = ctx.pool.events().insert(d1).await?;
    let d1_id = d1_inserted.id.expect("d1 must have id").to_uuid();

    // Archive both -- mirrors what a replay's archive step does before
    // dispatching the scan/parse.
    let archived_count = ctx
        .pool
        .events()
        .execute_cascade_archive(
            &[m1_id, d1_id],
            "test archive for parent-liveness regression",
            "cascade-restore-parent-liveness-op",
            "test:archiver",
        )
        .await?;
    assert_eq!(archived_count, 2);

    // Simulate the source re-emitting a FRESH interpretation of the same
    // occurrence before restore runs: m2 shares (source_material_id,
    // anchor_byte) with m1 but has a different id.
    let m2 = DynamicPayload::new(
        "cascade-restore-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/cascade-restore-m2.txt" }),
    )
    .from_material(material_id) // anchor_byte 0, same occurrence as m1
    .build()?;
    let m2_inserted = ctx.pool.events().insert(m2).await?;
    let m2_id = m2_inserted.id.expect("m2 must have id").to_uuid();
    assert_ne!(m2_id, m1_id, "re-emitted material must mint a fresh id");

    // Attempt to restore both m1 and d1.
    let restored_count = ctx
        .pool
        .events()
        .execute_cascade_restore(&[m1_id, d1_id], "cascade-restore-parent-liveness-op")
        .await?;

    assert_eq!(
        restored_count, 0,
        "neither the occurrence-blocked material nor its derived child should restore"
    );

    let m1_live: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
        .bind(m1_id)
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(m1_live, 0, "m1 must stay archived (existing occurrence-safety guard)");

    let d1_live: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
        .bind(d1_id)
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(
        d1_live, 0,
        "d1 must stay archived: its only parent (m1) is not live -- this is the sinex-79is regression"
    );

    let d1_archived: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM audit.archived_events WHERE id = $1::uuid",
    )
    .bind(d1_id)
    .fetch_one(&ctx.pool)
    .await?;
    assert_eq!(d1_archived, 1, "d1 must remain in the archive, not be lost");

    // Sanity: m2 (the fresh re-emission) is unaffected by any of this.
    let m2_live: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
        .bind(m2_id)
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(m2_live, 1);

    Ok(())
}

/// Positive case: when a derived row's parent IS live (the ordinary
/// non-conflicted restore path -- e.g. a plain operator Cancel with no
/// concurrent re-emission), the derived row restores normally. Guards
/// against an overly aggressive fix that would block all derived-row
/// restores rather than only the genuinely orphaned ones.
#[sinex_test]
async fn cascade_restore_restores_derived_row_whose_parent_is_live(ctx: TestContext) -> Result<()> {
    let material_id = ctx
        .create_source_material(Some("cascade-restore-parent-liveness-ok"))
        .await?;

    let m1 = DynamicPayload::new(
        "cascade-restore-ok-test",
        FileCreatedPayload::EVENT_TYPE.as_static_str(),
        json!({ "path": "/tmp/cascade-restore-ok-m1.txt" }),
    )
    .from_material(material_id)
    .build()?;
    let m1_inserted = ctx.pool.events().insert(m1).await?;
    let m1_id = m1_inserted.id.expect("m1 must have id").to_uuid();

    let product_class = DerivedProductClass::CanonicalDerivedEvent;
    seed_product_declaration(
        &ctx.pool,
        "sinex.test.cascade_restore_parent_liveness_ok",
        product_class,
        "cascade-restore-ok-derived-test",
        "analytics.summary",
    )
    .await?;

    let mut d1 = DynamicPayload::new(
        "cascade-restore-ok-derived-test",
        "analytics.summary",
        json!({ "path": "/tmp/cascade-restore-ok-d1.txt" }),
    )
    .from_parents([m1_inserted.id.expect("m1 must have id")])?
    .build()?;
    d1.product_class = Some(product_class);
    d1.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    d1.derivation_declaration_id =
        Some("sinex.test.cascade_restore_parent_liveness_ok".to_string());
    let d1_inserted = ctx.pool.events().insert(d1).await?;
    let d1_id = d1_inserted.id.expect("d1 must have id").to_uuid();

    let archived_count = ctx
        .pool
        .events()
        .execute_cascade_archive(
            &[m1_id, d1_id],
            "test archive for parent-liveness ok regression",
            "cascade-restore-parent-liveness-ok-op",
            "test:archiver",
        )
        .await?;
    assert_eq!(archived_count, 2);

    // No re-emission this time -- restore both, unconflicted.
    let restored_count = ctx
        .pool
        .events()
        .execute_cascade_restore(&[m1_id, d1_id], "cascade-restore-parent-liveness-ok-op")
        .await?;
    assert_eq!(restored_count, 2);

    let m1_live: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
        .bind(m1_id)
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(m1_live, 1);

    let d1_live: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid")
        .bind(d1_id)
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(
        d1_live, 1,
        "d1 should restore normally when its parent is also being restored"
    );

    Ok(())
}

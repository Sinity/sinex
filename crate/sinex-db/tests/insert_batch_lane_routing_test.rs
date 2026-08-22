//! sinex-kgp4 regression coverage: `EventRepository::insert_batch`'s
//! multi-element path must route each event to its correct storage lane
//! (`core.events` for Activity, `reflection.events` for Reflection) by
//! `source_role(event.source)`, the same way the single-element path already
//! does via `insert_with_tx`.
//!
//! Production dependency exercised: `EventRepository::insert_batch`. The
//! mixed batch below forces its multi-element path and verifies physical
//! placement directly in both event tables.

use sinex_db::DbPoolExt;
use sinex_primitives::Id;
use sinex_primitives::derivation::DerivedProductClass;
use sinex_primitives::events::EventId;
use sinex_primitives::events::payload::DynamicPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn insert_batch_routes_reflection_event_out_of_multi_element_batch(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("kgp4-lane-routing-material"))
        .await?;

    // A reflection-source event (source_role() classifies any "sinex."/
    // "sinexd."-prefixed source as Reflection) sharing a batch with an
    // ordinary activity-source event.
    let reflection_event = DynamicPayload::new(
        "sinex.selftest",
        "kgp4.reflection.probe",
        serde_json::json!({"probe": "kgp4"}),
    )
    .from_material(material_id)
    .build()?;
    let activity_event = DynamicPayload::new(
        "kgp4-activity-source",
        "kgp4.activity.probe",
        serde_json::json!({"probe": "kgp4"}),
    )
    .from_material(material_id)
    .build()?;
    // len() == 2 forces the multi-element path (insert_batch_unnest_in_tx),
    // not the len()==1 single-element delegation to insert_with_tx.
    let inserted = ctx
        .pool()
        .events()
        .insert_batch(vec![reflection_event, activity_event])
        .await?;
    assert_eq!(
        inserted.len(),
        2,
        "both events in the batch should be inserted"
    );
    let reflection_event_id = *inserted
        .iter()
        .find(|event| event.source.as_str() == "sinex.selftest")
        .expect("inserted batch should retain the reflection event")
        .id
        .as_ref()
        .expect("inserted event should have an id")
        .as_uuid();

    let in_reflection: i64 =
        sqlx::query_scalar("SELECT count(*) FROM reflection.events WHERE id = $1")
            .bind(reflection_event_id)
            .fetch_one(ctx.pool())
            .await?;
    let in_core: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE id = $1")
        .bind(reflection_event_id)
        .fetch_one(ctx.pool())
        .await?;

    assert_eq!(
        (in_reflection, in_core),
        (1, 0),
        "reflection-source event sharing a multi-element insert_batch call must land in \
         reflection.events, not core.events -- sinex-kgp4: the multi-element path currently \
         hardcodes core.events regardless of source_role"
    );

    Ok(())
}

#[sinex_test]
async fn insert_batch_keeps_cross_chunk_reflection_parent_live(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("kgp4-cross-chunk-parent-material"))
        .await?;
    let declaration_id = "insert-batch-kgp4-reflection-child";
    sqlx::query(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            output_source, output_event_type, semantics_version,
            input_eligibility, default_claim_support, verification_command
        )
        VALUES (
            $1, 'kgp4-test', 'canonical_derived_event',
            'derived_output', 'sinex.child', 'kgp4.reflection.child', 'v1',
            'default_canonical_input', '{}'::jsonb, 'true'
        )
        ON CONFLICT (declaration_id) DO NOTHING
        "#,
    )
    .bind(declaration_id)
    .execute(ctx.pool())
    .await?;
    let parent_id = Id::new();
    let mut parent = DynamicPayload::new(
        "sinex.parent",
        "kgp4.reflection.parent",
        serde_json::json!({"role": "reflection-parent"}),
    )
    .from_material(material_id)
    .build()?;
    parent.id = Some(parent_id);

    let mut child = DynamicPayload::new(
        "sinex.child",
        "kgp4.reflection.child",
        serde_json::json!({"role": "reflection-child"}),
    )
    .from_parents(vec![EventId::from_uuid(*parent_id.as_uuid())])?
    .build()?;
    child.product_class = Some(DerivedProductClass::CanonicalDerivedEvent);
    child.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    child.derivation_declaration_id = Some(declaration_id.to_string());

    // Put the child in chunk one and its parent in chunk two. The production
    // insert path must validate parent liveness against the full batch, not
    // only the current 50-row chunk.
    let mut batch = vec![child];
    for index in 0..49 {
        batch.push(
            DynamicPayload::new(
                "kgp4-cross-chunk-activity",
                "kgp4.activity.filler",
                serde_json::json!({"index": index}),
            )
            .from_material(material_id)
            .build()?,
        );
    }
    batch.push(parent);

    let inserted = ctx.pool().events().insert_batch(batch).await?;
    assert_eq!(inserted.len(), 51);
    let child_id = inserted
        .iter()
        .find(|event| event.event_type.as_str() == "kgp4.reflection.child")
        .and_then(|event| event.id.as_ref())
        .expect("cross-chunk child should be inserted");
    let parent_id = inserted
        .iter()
        .find(|event| event.event_type.as_str() == "kgp4.reflection.parent")
        .and_then(|event| event.id.as_ref())
        .expect("cross-chunk parent should be inserted");

    let child_lanes: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM reflection.events WHERE id = $1),\
                (SELECT count(*) FROM core.events WHERE id = $1)",
    )
    .bind(child_id.as_uuid())
    .fetch_one(ctx.pool())
    .await?;
    let parent_lanes: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM reflection.events WHERE id = $1),\
                (SELECT count(*) FROM core.events WHERE id = $1)",
    )
    .bind(parent_id.as_uuid())
    .fetch_one(ctx.pool())
    .await?;
    assert_eq!(child_lanes, (1, 0));
    assert_eq!(parent_lanes, (1, 0));
    Ok(())
}

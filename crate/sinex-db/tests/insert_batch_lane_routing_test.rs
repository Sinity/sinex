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
    assert_eq!(inserted.len(), 2, "both events in the batch should be inserted");
    let reflection_event_id = *inserted
        .iter()
        .find(|event| event.source.as_str() == "sinex.selftest")
        .expect("inserted batch should retain the reflection event")
        .id
        .as_ref()
        .expect("inserted event should have an id")
        .as_uuid();

    let in_reflection: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM reflection.events WHERE id = $1",
    )
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

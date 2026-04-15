use sinex_db::DbPoolExt;
use sinex_db::replay::{ReplayScope, ReplayStateMachine};
use sinex_primitives::Timestamp;
use sinex_primitives::domain::RecordedPath;
use sinex_primitives::events::{Event, EventPayload, payloads::FileCreatedPayload};
use std::collections::HashMap;
use xtask::sandbox::prelude::*;

fn test_file_payload(path: &str) -> TestResult<FileCreatedPayload> {
    Ok(FileCreatedPayload::test_default(
        RecordedPath::from_observed(path).map_err(|e| color_eyre::eyre::eyre!(e))?,
    ))
}

#[sinex_test]
async fn replay_preview_nulls_cascade_impact_when_metadata_queries_fail(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("replay-preview-cascade-metadata"))
        .await?;
    let root = ctx
        .pool()
        .events()
        .insert(
            FileCreatedPayload::test_default(
                RecordedPath::from_observed("/tmp/replay-preview-root.txt")
                    .map_err(|e| color_eyre::eyre::eyre!(e))?,
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let root_id = root.id.expect("inserted root event should have an id");

    let mut derived = Event::builder(test_file_payload("/tmp/replay-preview-derived.txt")?)
        .from_parents(vec![root_id])?
        .build()?;
    derived.scope_key = Some("scope:replay-preview".to_string());
    ctx.pool().events().insert(derived).await?;

    sqlx::query!("ALTER TABLE core.events RENAME COLUMN scope_key TO scope_key_broken")
        .execute(ctx.pool())
        .await?;

    let machine = ReplayStateMachine::new(ctx.pool().clone());
    let preview = machine
        .generate_preview_summary(&ReplayScope {
            node_id: root.source.to_string(),
            time_window: Some((
                Timestamp::now() - time::Duration::minutes(5),
                Timestamp::now() + time::Duration::minutes(5),
            )),
            material_filter: None,
            filters: HashMap::new(),
        })
        .await?;

    assert!(
        preview["cascade_impact"].is_null(),
        "metadata query failures must invalidate cascade impact instead of synthesizing empty metadata"
    );
    assert_eq!(
        preview["root_event_ids"],
        serde_json::json!([root_id.to_uuid()]),
        "preview summaries must carry root ids for downstream replay execution"
    );
    Ok(())
}

#[sinex_test]
async fn replay_preview_maps_watcher_node_ids_to_emitted_event_sources(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("replay-preview-source-alias"))
        .await?;
    let root = ctx
        .pool()
        .events()
        .insert(
            FileCreatedPayload::test_default(
                RecordedPath::from_observed("/tmp/replay-preview-source-alias.txt")
                    .map_err(|e| color_eyre::eyre::eyre!(e))?,
            )
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let root_id = root.id.expect("inserted root event should have an id");

    let machine = ReplayStateMachine::new(ctx.pool().clone());
    let preview = machine
        .generate_preview_summary(&ReplayScope {
            node_id: "filesystem-watcher".to_string(),
            time_window: Some((
                root_id.timestamp() - time::Duration::minutes(1),
                root_id.timestamp() + time::Duration::minutes(1),
            )),
            material_filter: None,
            filters: HashMap::new(),
        })
        .await?;

    assert_eq!(
        preview["total_events"],
        serde_json::json!(1),
        "watcher node ids should match the emitted fs-watcher event source during replay preview"
    );
    assert_eq!(
        preview["root_event_ids"],
        serde_json::json!([root_id.to_uuid()]),
        "preview summaries must keep the matched replay roots after source alias expansion"
    );
    Ok(())
}

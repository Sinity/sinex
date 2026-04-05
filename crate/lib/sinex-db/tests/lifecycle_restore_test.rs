use sinex_db::DbPoolExt;
use sinex_primitives::domain::{DerivedNodeModel, RecordedPath, SyntheticTemporalPolicy};
use sinex_primitives::events::{Event, EventPayload, payloads::FileCreatedPayload};
use sinex_primitives::{Timestamp, Uuid};
use xtask::sandbox::prelude::*;

fn test_file_payload(path: &str) -> TestResult<FileCreatedPayload> {
    Ok(FileCreatedPayload::test_default(
        RecordedPath::from_observed(path).map_err(|e| color_eyre::eyre::eyre!(e))?,
    ))
}

#[sinex_test]
async fn cascade_restore_preserves_synthetic_metadata_columns(ctx: TestContext) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("lifecycle-restore-parent"))
        .await?;
    let parent = ctx
        .pool()
        .events()
        .insert(
            FileCreatedPayload {
                path: "/tmp/lifecycle-parent.txt".into(),
                size: 128,
                created_at: Timestamp::now(),
                permissions: None,
            }
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let parent_id = parent.id.expect("published parent should have an id");

    let mut derived = Event::builder(test_file_payload("/tmp/lifecycle-derived.txt")?)
        .from_parents(vec![parent_id])?
        .build()?;
    let replay_operation_id = Uuid::now_v7();
    derived.temporal_policy = Some(SyntheticTemporalPolicy::LatestInput);
    derived.semantics_version = Some("v9.9.9".to_string());
    derived.scope_key = Some("scope:lifecycle-restore".to_string());
    derived.equivalence_key = Some("equiv:lifecycle-restore".to_string());
    derived.created_by_operation_id = Some(replay_operation_id);
    derived.node_model = Some(DerivedNodeModel::ScopeReconciler);

    let inserted = ctx.pool().events().insert(derived).await?;
    let event_id = inserted
        .id
        .expect("inserted derived event should have an id");

    let archive_operation_id = Uuid::now_v7().to_string();
    ctx.pool()
        .events()
        .execute_cascade_archive(
            &[*event_id.as_uuid()],
            "archive before restore metadata regression",
            &archive_operation_id,
            "test",
        )
        .await?;

    let restore_operation_id = Uuid::now_v7().to_string();
    ctx.pool()
        .events()
        .execute_cascade_restore(&[*event_id.as_uuid()], &restore_operation_id)
        .await?;

    let restored = ctx
        .pool()
        .events()
        .get_by_id(event_id)
        .await?
        .expect("restored event should be present in live tier");

    assert_eq!(
        restored.temporal_policy,
        Some(SyntheticTemporalPolicy::LatestInput)
    );
    assert_eq!(restored.semantics_version.as_deref(), Some("v9.9.9"));
    assert_eq!(
        restored.scope_key.as_deref(),
        Some("scope:lifecycle-restore")
    );
    assert_eq!(
        restored.equivalence_key.as_deref(),
        Some("equiv:lifecycle-restore")
    );
    assert_eq!(restored.created_by_operation_id, Some(replay_operation_id));
    assert_eq!(restored.node_model, Some(DerivedNodeModel::ScopeReconciler));

    Ok(())
}

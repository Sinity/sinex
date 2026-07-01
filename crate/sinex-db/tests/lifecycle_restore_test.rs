use sinex_db::DbPoolExt;
use sinex_primitives::domain::{AutomatonModel, RecordedPath, SyntheticTemporalPolicy};
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
    derived.automaton_model = Some(AutomatonModel::ScopeReconciler);

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
    assert_eq!(
        restored.automaton_model,
        Some(AutomatonModel::ScopeReconciler)
    );

    Ok(())
}

/// #2194 F2: a cascade restore must not re-create a material interpretation
/// whose occurrence (source_material_id, anchor_byte) is already live again.
/// During a crashed replay the source may re-emit a fresh material event (new
/// id, same occurrence) before the crash; restoring the archived twin by PK
/// would leave two live interpretations for one occurrence. The occurrence
/// guard must skip the archived row instead.
#[sinex_test]
async fn cascade_restore_skips_material_occurrence_already_live(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("occurrence-guard-material"))
        .await?;

    // Original interpretation at occurrence (material_id, anchor_byte=0).
    let original = ctx
        .pool()
        .events()
        .insert(
            FileCreatedPayload {
                path: "/tmp/occurrence-guard.txt".into(),
                size: 64,
                created_at: Timestamp::now(),
                permissions: None,
            }
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let original_id = original.id.expect("original should have an id");
    let original_anchor = original
        .provenance()
        .anchor_byte()
        .expect("material event should have an anchor byte");

    // Archive it (the replay archive step).
    let archive_operation_id = Uuid::now_v7().to_string();
    ctx.pool()
        .events()
        .execute_cascade_archive(
            &[*original_id.as_uuid()],
            "archive before re-emission",
            &archive_operation_id,
            "test",
        )
        .await?;

    // Re-emission: a fresh interpretation (new id) for the SAME occurrence.
    let reemitted = ctx
        .pool()
        .events()
        .insert(
            FileCreatedPayload {
                path: "/tmp/occurrence-guard.txt".into(),
                size: 64,
                created_at: Timestamp::now(),
                permissions: None,
            }
            .from_material(material_id)
            .build()?,
        )
        .await?;
    let reemitted_id = reemitted.id.expect("re-emitted should have an id");
    assert_ne!(original_id, reemitted_id, "re-emission must mint a new id");
    assert_eq!(
        reemitted
            .provenance()
            .anchor_byte()
            .expect("material event should have an anchor byte"),
        original_anchor,
        "re-emission must reuse the same occurrence anchor"
    );

    // Crash-recovery restore of the archived cascade must NOT resurrect the
    // archived twin, because its occurrence is already live again.
    let restore_operation_id = Uuid::now_v7().to_string();
    let restored = ctx
        .pool()
        .events()
        .execute_cascade_restore(&[*original_id.as_uuid()], &restore_operation_id)
        .await?;
    assert_eq!(restored, 0, "occurrence already live → nothing restored");

    assert!(
        ctx.pool().events().get_by_id(original_id).await?.is_none(),
        "archived original must stay archived (occurrence guard)"
    );
    assert!(
        ctx.pool().events().get_by_id(reemitted_id).await?.is_some(),
        "re-emitted interpretation remains the single live row"
    );

    Ok(())
}

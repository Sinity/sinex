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

/// Registers a `derivation.product_declarations` row for
/// `fs-watcher`/`file.created` so this file's derived-event fixture (built
/// via `.from_parents(..)`) satisfies the `events_derived_requires_product_class`
/// CHECK constraint and the `enforce_event_product_declaration` trigger
/// (sinex-0vx.4 / sinex-8cr.2 derivation control plane, landed after this
/// fixture was written — see sinex-94mh). Idempotent via `ON CONFLICT`.
async fn ensure_replay_preview_test_declaration(pool: &sqlx::PgPool) -> color_eyre::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            output_source, output_event_type, semantics_version,
            input_eligibility, default_claim_support, verification_command
        )
        VALUES (
            'replay-preview-test-decl', 'test-owner', 'canonical_derived_event',
            'derived_output', 'fs-watcher', 'file.created', 'v1',
            'default_canonical_input', '{}'::jsonb, 'true'
        )
        ON CONFLICT (declaration_id) DO NOTHING
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
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

    ensure_replay_preview_test_declaration(ctx.pool()).await?;
    let mut derived = Event::builder(test_file_payload("/tmp/replay-preview-derived.txt")?)
        .from_parents(vec![root_id])?
        .build()?;
    derived.scope_key = Some("scope:replay-preview".to_string());
    derived.product_class =
        Some(sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent);
    derived.claim_support = Some(sinex_primitives::derivation::ClaimSupport::unknown());
    derived.derivation_declaration_id = Some("replay-preview-test-decl".to_string());
    ctx.pool().events().insert(derived).await?;

    sqlx::query!("ALTER TABLE core.events RENAME COLUMN scope_key TO scope_key_broken")
        .execute(ctx.pool())
        .await?;

    let machine = ReplayStateMachine::new(ctx.pool().clone());
    let preview = machine
        .generate_preview_summary(&ReplayScope {
            source_name: root.source.to_string(),
            time_window: Some((
                Timestamp::now() - time::Duration::minutes(5),
                Timestamp::now() + time::Duration::minutes(5),
            )),
            material_filter: None,
            filters: HashMap::new(),
            ..Default::default()
        })
        .await?;

    assert!(
        preview["cascade_impact"].is_null(),
        "metadata query failures must invalidate cascade impact instead of synthesizing empty metadata"
    );
    assert_eq!(
        preview["root_event_id_sample"],
        serde_json::json!([root_id.to_uuid()]),
        "preview summaries must carry a bounded root-id sample"
    );
    assert!(
        preview["anchor_churn_pct"].is_null(),
        "unmeasured anchor churn must not be reported as zero"
    );
    assert_eq!(preview["anchor_churn_status"], "not_measured");
    assert!(
        preview["time_quality_flip_pct"].is_null(),
        "unmeasured time-quality flips must not be reported as zero"
    );
    assert_eq!(preview["time_quality_flip_status"], "not_measured");
    Ok(())
}

#[sinex_test]
async fn replay_preview_maps_watcher_source_ids_to_emitted_event_sources(
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
            source_name: "filesystem-watcher".to_string(),
            time_window: Some((
                root_id.timestamp().expect("test ID must be UUIDv7") - time::Duration::minutes(1),
                root_id.timestamp().expect("test ID must be UUIDv7") + time::Duration::minutes(1),
            )),
            material_filter: None,
            filters: HashMap::new(),
            ..Default::default()
        })
        .await?;

    assert_eq!(
        preview["total_events"],
        serde_json::json!(1),
        "watcher source names should match the emitted fs-watcher event source during replay preview"
    );
    assert_eq!(
        preview["root_event_id_sample"],
        serde_json::json!([root_id.to_uuid()]),
        "preview summaries must keep sampled replay roots after source alias expansion"
    );
    Ok(())
}

#[sinex_test]
async fn replay_preview_bounds_root_identity_at_the_keyset_page_boundary(
    ctx: TestContext,
) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("replay-preview-root-boundary"))
        .await?;
    let now = Timestamp::now();

    for anchor in 0..=10_000_i64 {
        ctx.pool()
            .events()
            .insert(
                FileCreatedPayload::test_default(
                    RecordedPath::from_observed("/tmp/replay-preview-boundary.txt")
                        .map_err(|e| color_eyre::eyre::eyre!(e))?,
                )
                .from_material_at(material_id, anchor)
                .build()?,
            )
            .await?;
    }

    let preview = ReplayStateMachine::new(ctx.pool().clone())
        .generate_preview_summary(&ReplayScope {
            source_name: "fs-watcher".to_string(),
            time_window: Some((
                now - time::Duration::minutes(1),
                Timestamp::now() + time::Duration::minutes(1),
            )),
            material_filter: Some(vec![*material_id.as_uuid()]),
            ..Default::default()
        })
        .await?;

    assert_eq!(preview["total_events"], serde_json::json!(10_001));
    assert_eq!(preview["root_event_count"], serde_json::json!(10_001));
    assert_eq!(
        preview["root_event_id_sample"]
            .as_array()
            .map(Vec::len),
        Some(100),
        "the persisted preview must stay bounded after the 10,000-root query page"
    );
    assert!(preview.get("root_event_ids").is_none());
    assert!(preview["root_event_id_fingerprint"]
        .as_str()
        .is_some_and(|fingerprint| !fingerprint.is_empty()));
    Ok(())
}

use chrono::{Duration, Utc};
use serde_json::json;
use sinex_core::db::integrity::{
    CheckpointInconsistencyType, CheckpointKind, CheckpointSnapshot, IntegrityTestConfig,
    IntegrityTester,
};
use sinex_core::repositories::DbPoolExt;
use sinex_core::types::ulid::Ulid as CoreUlid;
use xtask::sandbox::{sinex_test, TestContext};

#[sinex_test]
async fn windowing_limits_event_counts(ctx: TestContext) -> color_eyre::Result<()> {
    let pool = ctx.pool.clone();

    // Seed manifest for the synthetic processor
    sqlx::query!(
        r#"
        INSERT INTO core.processor_manifests (processor_name, node_type, version, description, anchor_rule_version)
        VALUES ($1, 'automaton', '1.0.0', NULL, 1)
        "#,
        "integrity.proc"
    )
    .execute(&pool)
    .await?;

    let snapshots = vec![CheckpointSnapshot {
        processor_name: "integrity.proc".to_string(),
        consumer_group: "default".to_string(),
        consumer_name: "default".to_string(),
        checkpoint_kind: CheckpointKind::None,
        last_processed_id: None,
        processed_count: 0,
        last_activity: Utc::now(),
    }];

    // Old event outside the window
    let old_event_id = CoreUlid::from_datetime(Utc::now() - Duration::hours(48));
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_material_id,
            anchor_byte,
            offset_kind
        ) VALUES (
            $1::uuid::ulid,
            $2,
            $3,
            'localhost',
            $4,
            $5,
            $6::uuid::ulid,
            0,
            'byte'
        )
        "#,
        old_event_id.as_uuid(),
        "integrity.source",
        "integrity.event",
        json!({"age": "old"}),
        Utc::now() - Duration::hours(48),
        CoreUlid::new().as_uuid()
    )
    .execute(&pool)
    .await?;

    // Fresh event inside window
    let fresh_event_id = CoreUlid::new();
    sqlx::query!(
        r#"
        INSERT INTO core.events (
            id,
            source,
            event_type,
            host,
            payload,
            ts_orig,
            source_material_id,
            anchor_byte,
            offset_kind
        ) VALUES (
            $1::uuid::ulid,
            $2,
            $3,
            'localhost',
            $4,
            $5,
            $6::uuid::ulid,
            0,
            'byte'
        )
        "#,
        fresh_event_id.as_uuid(),
        "integrity.source",
        "integrity.event",
        json!({"age": "new"}),
        Utc::now(),
        CoreUlid::new().as_uuid()
    )
    .execute(&pool)
    .await?;

    let tester = IntegrityTester::new(&pool).await?;
    let report = tester
        .run_integrity_tests(IntegrityTestConfig {
            max_events_to_check: 10,
            check_window_hours: 24,
            include_deep_validation: false,
            validate_checkpoints: true,
            validate_ulid_ordering: false,
            validate_schemas: false,
            checkpoint_snapshots: Some(snapshots),
        })
        .await?;

    let issues = report.check_report.checkpoint_inconsistencies;
    let behind = issues
        .iter()
        .find(|issue| issue.inconsistency_type == CheckpointInconsistencyType::CheckpointBehindEvents)
        .expect("expected checkpoint lag issue");
    assert_eq!(behind.events_potentially_missed, 1);
    Ok(())
}

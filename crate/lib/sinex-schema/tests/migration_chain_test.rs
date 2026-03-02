//! Migration chain invariant tests.
//!
//! Validates that the migration registry in `Migrator::migrations()` maintains
//! structural invariants: strictly increasing sequence numbers, no gaps, and
//! unique names. Also tests the operation_id security trigger that guards
//! `core.events` against accidental deletes.

use sea_orm_migration::prelude::*;
use sinex_schema::Migrator;
use std::collections::HashSet;
use xtask::sandbox::prelude::*;

/// Extract the sequence number from a migration name.
///
/// Migration names follow the pattern `mYYYYMMDD_NNNNNN_description`.
/// Returns the `NNNNNN` portion parsed as u32.
fn extract_sequence_number(name: &str) -> Option<u32> {
    // Format: m<date>_<seq>_<rest>
    // Split on '_' and take the second segment (index 1).
    let parts: Vec<&str> = name.splitn(3, '_').collect();
    if parts.len() >= 2 {
        parts[1].parse::<u32>().ok()
    } else {
        None
    }
}

// ─── Migration ordering invariants ──────────────────────────────────────

#[sinex_test]
async fn migration_sequence_numbers_are_strictly_increasing() -> TestResult<()> {
    let migrations = <Migrator as MigratorTrait>::migrations();
    assert!(
        !migrations.is_empty(),
        "Migrator should have at least one migration"
    );

    let mut prev_seq: Option<u32> = None;
    for migration in &migrations {
        let name = migration.name();
        let seq = extract_sequence_number(name).unwrap_or_else(|| {
            panic!("Migration name '{name}' does not contain a valid sequence number")
        });

        if let Some(prev) = prev_seq {
            assert!(
                seq > prev,
                "Migration '{name}' has sequence {seq} which is not strictly greater than previous {prev}"
            );
        }
        prev_seq = Some(seq);
    }

    Ok(())
}

#[sinex_test]
async fn migration_sequence_numbers_have_no_gaps() -> TestResult<()> {
    let migrations = <Migrator as MigratorTrait>::migrations();
    let sequences: Vec<u32> = migrations
        .iter()
        .map(|m| {
            extract_sequence_number(m.name())
                .unwrap_or_else(|| panic!("Bad migration name: {}", m.name()))
        })
        .collect();

    for window in sequences.windows(2) {
        let (prev, next) = (window[0], window[1]);
        assert_eq!(
            next,
            prev + 1,
            "Gap in migration sequence: {prev} -> {next}. Expected {prev} -> {}",
            prev + 1
        );
    }

    Ok(())
}

#[sinex_test]
async fn migration_names_are_unique() -> TestResult<()> {
    let migrations = <Migrator as MigratorTrait>::migrations();
    let mut seen = HashSet::new();

    for migration in &migrations {
        let name = migration.name();
        assert!(
            seen.insert(name.to_string()),
            "Duplicate migration name: '{name}'"
        );
    }

    assert_eq!(seen.len(), migrations.len());
    Ok(())
}

#[sinex_test]
async fn migration_sequence_starts_at_one() -> TestResult<()> {
    let migrations = <Migrator as MigratorTrait>::migrations();
    let first = migrations.first().expect("at least one migration");
    let seq = extract_sequence_number(first.name())
        .expect("first migration should have valid sequence number");
    assert_eq!(seq, 1, "Migration sequence should start at 1, got {seq}");

    Ok(())
}

#[sinex_test]
async fn migration_count_matches_expected() -> TestResult<()> {
    let migrations = <Migrator as MigratorTrait>::migrations();
    // The last migration has sequence 25.
    let last = migrations.last().expect("at least one migration");
    let last_seq = extract_sequence_number(last.name()).expect("valid sequence");

    assert_eq!(
        migrations.len(),
        last_seq as usize,
        "Number of registered migrations ({}) should equal the last sequence number ({last_seq})",
        migrations.len()
    );

    Ok(())
}

// ─── Operation ID security trigger ──────────────────────────────────────

/// Helper: insert a test event directly via SQL, bypassing the NATS pipeline.
/// sinex-schema tests don't have an ingest daemon, so ctx.publish() would time out.
async fn insert_test_event(
    pool: &sqlx::PgPool,
    ctx: &TestContext,
    source: &str,
) -> TestResult<sinex_primitives::Id<sinex_primitives::Event<serde_json::Value>>> {
    let event_id = sinex_primitives::Id::<sinex_primitives::Event<serde_json::Value>>::new();
    let material_id = ctx.create_source_material(Some(source)).await?;

    sqlx::query(
        r#"
        INSERT INTO core.events (id, source, event_type, payload, ts_orig, host, node_version, source_material_id)
        VALUES ($1::uuid::ulid, $2, $3, $4::jsonb, NOW(), $5, $6, $7::uuid::ulid)
        "#,
    )
    .bind(event_id.to_uuid())
    .bind(source)
    .bind("test.security")
    .bind(serde_json::json!({"test": "operation_id_guard"}))
    .bind("test-host")
    .bind("test-v1")
    .bind(material_id.to_uuid())
    .execute(pool)
    .await?;

    Ok(event_id)
}

#[sinex_test]
async fn delete_without_operation_id_is_rejected(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let event_id = insert_test_event(pool, &ctx, "migration-test-guard").await?;

    // Attempt DELETE without setting sinex.operation_id — trigger should reject.
    let result = sqlx::query("DELETE FROM core.events WHERE id = $1::uuid::ulid")
        .bind(event_id.to_uuid())
        .execute(pool)
        .await;

    assert!(
        result.is_err(),
        "DELETE without sinex.operation_id should be rejected by the archive trigger"
    );

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("sinex.operation_id"),
        "Error message should mention sinex.operation_id, got: {err_msg}"
    );

    // Verify the event still exists.
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid::ulid")
            .bind(event_id.to_uuid())
            .fetch_one(pool)
            .await?;
    assert_eq!(count.0, 1, "Event should still exist after rejected delete");

    Ok(())
}

#[sinex_test]
async fn delete_with_operation_id_succeeds(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let event_id = insert_test_event(pool, &ctx, "migration-test-allowed").await?;

    // Set sinex.operation_id and delete — should succeed.
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT set_config('sinex.operation_id', $1, true)")
        .bind("test-migration-chain-delete")
        .execute(&mut *tx)
        .await?;

    let result = sqlx::query("DELETE FROM core.events WHERE id = $1::uuid::ulid")
        .bind(event_id.to_uuid())
        .execute(&mut *tx)
        .await;

    assert!(
        result.is_ok(),
        "DELETE with sinex.operation_id set should succeed, got: {:?}",
        result.err()
    );

    tx.commit().await?;

    // Verify the event is gone from core.events.
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::bigint FROM core.events WHERE id = $1::uuid::ulid")
            .bind(event_id.to_uuid())
            .fetch_one(pool)
            .await?;
    assert_eq!(count.0, 0, "Event should be deleted from core.events");

    Ok(())
}

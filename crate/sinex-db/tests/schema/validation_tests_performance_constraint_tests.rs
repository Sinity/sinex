use super::constraint_validation_tests::setup_test_tables;
use super::*;

#[sinex_serial_test]
#[ignore = "long: constraint performance fixture, run via xtask test --heavy"]
async fn test_constraint_check_performance() -> TestResult<()> {
    let ctx = prepare_constraint_context().await?;
    let pool = &ctx.pool;
    constraint_validation_tests::setup_test_tables(pool).await;
    let mut conn = pool.acquire().await?;

    let material_id = Uuid::now_v7();
    sqlx::query!(
        r#"
        INSERT INTO raw.source_material_registry (id, material_kind, source_identifier, status, timing_info_type, metadata)
        VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', '{}'::jsonb)
        ON CONFLICT (id) DO NOTHING
        "#,
        material_id,
        format!("bulk-material-{material_id}")
    )
    .execute(conn.as_mut())
    .await?;

    let start = std::time::Instant::now();
    let inserts = 4;
    for i in 0..inserts {
        let event_id = Uuid::now_v7();
        let mut attempts = 0;
        loop {
            match sqlx::query!(
                "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
                event_id,
                "bulk-source",
                "bulk-event",
                "test-host",
                serde_json::json!({"index": i}),
                *Timestamp::now(),
                material_id,
                i64::from(i)
            )
            .execute(conn.as_mut())
            .await
            {
                Ok(_) => break,
                Err(err) if attempts < 2 => {
                    attempts += 1;
                    if let Some(code) = err.as_database_error().and_then(sqlx::error::DatabaseError::code)
                        && code.as_ref() == "57P01" {
                            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            continue;
                        }
                    return Err(err.into());
                }
                Err(err) => return Err(err.into())}
        }
    }

    let duration = start.elapsed();
    let per_insert = duration / inserts as u32;
    println!(
        "Inserted {inserts} events with constraints in {duration:?} ({per_insert:?} per insert)"
    );

    // Constraint checking should not significantly slow down inserts.
    assert!(
        per_insert.as_millis() < 1500,
        "Constraint checking per insert should remain well under 1.5s (observed {per_insert:?})"
    );
    finalize_constraint_context(&ctx).await?;
    Ok(())
}

#[sinex_serial_test]
async fn test_index_constraint_interaction() -> TestResult<()> {
    let ctx = prepare_constraint_context().await?;
    let pool = &ctx.pool;
    setup_test_tables(pool).await;

    for index_stmt in Events::create_indexes() {
        let sql = index_stmt.to_string(PostgresQueryBuilder);
        let _ = sqlx::query(&sql).execute(pool).await; // May fail if already exists
    }
    sqlx::query("CREATE INDEX IF NOT EXISTS ux_events_material_anchor_id ON core.events (source_material_id, anchor_byte)")
        .execute(pool)
        .await?;

    // Create source material
    sqlx::query(
        &SourceMaterialRegistry::create_table_statement().to_string(PostgresQueryBuilder),
    )
    .execute(pool)
    .await
    .unwrap();

    let material = insert_sample_material(&ctx).await?;

    // Test that constraints work correctly with indexes present
    let event_id1 = Uuid::now_v7();
    sqlx::query!(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
        event_id1,
        "indexed-source",
        "indexed-event",
        "test-host",
        serde_json::json!({}),
        *Timestamp::now(),
        material.id,
        42
    ).execute(pool).await.unwrap();

    // Verify the storage-level index exists as expected
    let index_exists = sqlx::query_scalar!(
        "SELECT COUNT(*)::BIGINT FROM pg_indexes WHERE schemaname = 'core' AND tablename = 'events' AND indexname = 'ux_events_material_anchor_id'"
    )
    .fetch_one(pool)
    .await?;
    assert!(
        index_exists.expect("COUNT(*) should always return one row") >= 1,
        "expected anchor index to exist"
    );

    // Duplicate inserts currently succeed due to TimescaleDB's requirement that
    // unique indexes include the hypertable partition key. The ingest layer is
    // responsible for enforcing anchor uniqueness prior to insert.
    let event_id2 = Uuid::now_v7();
    let mut inserted = false;
    for attempt in 0..3 {
        let result = sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::uuid, $8)",
            event_id2,
            "indexed-source",
            "indexed-event-2",
            "test-host",
            serde_json::json!({}),
            *Timestamp::now(),
            material.id,
            42
        )
        .execute(pool)
        .await;
        match result {
            Ok(res) => {
                assert_eq!(
                    res.rows_affected(),
                    1,
                    "duplicate insert should succeed at SQL layer"
                );
                inserted = true;
                break;
            }
            Err(err) if attempt < 2 => {
                if let Some(code) = err
                    .as_database_error()
                    .and_then(sqlx::error::DatabaseError::code)
                    && code.as_ref() == "40P01"
                {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
            Err(err) => return Err(err.into()),
        }
    }
    assert!(inserted, "failed to insert second event after retries");

    let duplicate_count = sqlx::query_scalar!(
        "SELECT COUNT(*)::BIGINT FROM core.events WHERE source_material_id = $1::uuid AND anchor_byte = $2",
        material.id,
        42
    )
    .fetch_one(pool)
    .await?;
    assert!(
        duplicate_count.expect("COUNT(*) should always return one row") >= 2,
        "expected at least two events sharing anchor byte"
    );
    // Clean state before finalize to avoid residual rows.
    sqlx::query("TRUNCATE core.events CASCADE")
        .execute(pool)
        .await?;
    sqlx::query("TRUNCATE raw.source_material_registry CASCADE")
        .execute(pool)
        .await?;
    finalize_constraint_context(&ctx).await?;
    Ok(())
}

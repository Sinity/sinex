use super::*;
use crate::sandbox::EnvGuard;
use crate::sandbox::sinex_test;

/// Drop (if present), wait for absence, then recreate a pool database.
///
/// Shared setup step in tests that need a clean slot database with current
/// schema before running the actual assertion.
async fn reset_slot_database_for_test(
    admin_conn: &mut sqlx::postgres::PgConnection,
    db_name: &str,
    slot_url: &str,
) -> TestResult<()> {
    drop_database_if_exists_admin(admin_conn, db_name).await?;
    wait_for_database_absence_admin(admin_conn, db_name).await?;
    recreate_pool_database(db_name, slot_url).await
}

#[sinex_test]
async fn test_format_acquisition_timeout_message_includes_hint_and_attempts() -> TestResult<()> {
    let msg = format_acquisition_timeout_message(Duration::from_mins(1), 120, "");
    assert!(msg.contains("permanently locked"), "got: {msg}");
    assert!(msg.contains("120 attempts"), "got: {msg}");
    Ok(())
}

#[sinex_test]
async fn test_format_acquisition_timeout_message_includes_lock_holders() -> TestResult<()> {
    let lock_holders =
        "\n\nLock holders:\n  pid=1234 app=nextest query=SELECT pg_advisory_lock(42)";
    let msg = format_acquisition_timeout_message(Duration::from_secs(30), 5, lock_holders);
    assert!(msg.contains("Lock holders"), "got: {msg}");
    assert!(msg.contains("pg_advisory_lock"), "got: {msg}");
    Ok(())
}

#[sinex_test]
async fn test_query_advisory_lock_holders_surfaces_probe_failures() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgres://127.0.0.1:1/definitely_missing");
    let probe = query_advisory_lock_holders().await;
    assert!(
        probe.contains("Advisory lock holder probe unavailable"),
        "unexpected probe output: {probe}"
    );
    assert!(
        probe.contains("failed to query pg_stat_activity")
            || probe.contains("timed out querying pg_stat_activity"),
        "unexpected probe output: {probe}"
    );
    Ok(())
}

#[sinex_test]
async fn test_format_lock_holder_field_preserves_sqlx_errors() -> TestResult<()> {
    let rendered =
        format_lock_holder_field::<i32>("pid", Err(sqlx::Error::ColumnNotFound("pid".into())));
    assert!(rendered.contains("<unavailable: pid"));
    assert!(rendered.contains("no column found"));
    Ok(())
}

#[sinex_test]
async fn test_format_lock_holder_field_renders_values() -> TestResult<()> {
    let rendered = format_lock_holder_field("state", Ok("active"));
    assert_eq!(rendered, "active");
    Ok(())
}

#[sinex_test]
async fn test_serial_test_lock_path_uses_repo_local_state_root() -> TestResult<()> {
    let path = serial_test_lock_path();
    assert!(
        path.ends_with("dev-state/state/test-locks/db-pool-serial.lock"),
        "unexpected serial lock path: {}",
        path.display()
    );
    Ok(())
}

#[sinex_test]
async fn test_acquire_process_test_guard_serializes_same_process_waiters() -> TestResult<()> {
    let first_guard = acquire_process_test_guard().await;
    let waiter = tokio::spawn(async { acquire_process_test_guard().await });

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !waiter.is_finished(),
        "second waiter acquired the serial guard before the first was dropped"
    );

    drop(first_guard);
    let second_guard = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .map_err(|_| eyre!("timed out waiting for second serial guard acquisition"))?;
    let second_guard = second_guard?;
    drop(second_guard);
    Ok(())
}

#[sinex_test]
async fn test_prune_stale_lazy_slot_databases_drops_mismatched_unlocked_db() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_prune_{}", std::process::id());
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    let quoted = quote_ident(&db_name);
    sqlx::query(&format!("CREATE DATABASE {quoted}"))
        .execute(&mut admin_conn)
        .await?;
    store_pool_meta(
        &mut admin_conn,
        &db_name,
        &PoolMeta {
            fingerprint: Some("stale-fingerprint".to_string()),
            extensions: HashMap::new(),
            dirty: false,
            updated_at_rfc3339: Timestamp::now().format_rfc3339(),
            last_error: None,
        },
    )
    .await?;

    let summary = prune_stale_lazy_slot_databases(
        &config.admin_url,
        std::slice::from_ref(&db_name),
        &Some(schema_fingerprint()?),
        &HashMap::new(),
    )
    .await?;
    assert_eq!(summary.pruned, 1, "stale idle slot should be pruned");
    assert!(
        summary.locked_stale_slots.is_empty(),
        "unlocked slot should not be reported as deferred"
    );
    assert!(
        !provisioning::database_exists_admin(&mut admin_conn, &db_name).await?,
        "stale idle slot database should be removed"
    );

    Ok(())
}

#[sinex_test]
async fn test_prune_stale_lazy_slot_databases_keeps_locked_db() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_prune_locked_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    let quoted = quote_ident(&db_name);
    sqlx::query(&format!("CREATE DATABASE {quoted}"))
        .execute(&mut admin_conn)
        .await?;
    store_pool_meta(
        &mut admin_conn,
        &db_name,
        &PoolMeta {
            fingerprint: Some("stale-fingerprint".to_string()),
            extensions: HashMap::new(),
            dirty: false,
            updated_at_rfc3339: Timestamp::now().format_rfc3339(),
            last_error: None,
        },
    )
    .await?;

    let lock_key = advisory_lock_key(&db_name);
    let mut slot_conn = PgConnection::connect(&slot_url).await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut slot_conn)
        .await?;

    let summary = prune_stale_lazy_slot_databases(
        &config.admin_url,
        std::slice::from_ref(&db_name),
        &Some(schema_fingerprint()?),
        &HashMap::new(),
    )
    .await?;
    assert_eq!(summary.pruned, 0, "locked slot should not be pruned");
    assert_eq!(
        summary.locked_stale_slots,
        vec![(
            db_name.clone(),
            "pool metadata mismatch (fingerprint=Some(\"stale-fingerprint\"), extensions={})"
                .to_string()
        )],
        "locked stale slot should be surfaced once through the deferred summary"
    );
    assert!(
        provisioning::database_exists_admin(&mut admin_conn, &db_name).await?,
        "locked slot database should remain present"
    );

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut slot_conn)
        .await;
    slot_conn.close().await?;
    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;

    Ok(())
}

#[sinex_test]
async fn test_prune_stale_lazy_slot_databases_drops_actual_schema_drift_with_clean_metadata()
-> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_prune_schema_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    reset_slot_database_for_test(&mut admin_conn, &db_name, &slot_url).await?;
    let meta = load_pool_meta(&mut admin_conn, &db_name)
        .await?
        .ok_or_else(|| eyre!("missing pool metadata after slot recreation"))?;
    let slot_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    sqlx::query(
        r"
        ALTER TABLE raw.source_material_registry
            DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
            ADD CONSTRAINT source_material_registry_status_check
            CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
        ",
    )
    .execute(&slot_pool)
    .await?;
    let drift = reset::schema_mismatch_reason(&slot_pool).await?;
    assert!(
        drift
            .as_deref()
            .is_some_and(|reason| reason.contains("source_material_registry_status_check")),
        "expected real schema drift before lazy prune, got {drift:?}"
    );
    slot_pool.close().await;

    let summary = prune_stale_lazy_slot_databases(
        &config.admin_url,
        std::slice::from_ref(&db_name),
        &meta.fingerprint,
        &meta.extensions,
    )
    .await?;
    assert_eq!(
        summary.pruned, 1,
        "schema-drifted lazy slot should be pruned"
    );
    assert_eq!(
        summary.pruned_slots,
        vec![db_name.clone()],
        "pruned slot name should be recorded for follow-up repair"
    );
    assert!(
        summary.locked_stale_slots.is_empty(),
        "pruned drifted slot should not be reported as deferred"
    );
    assert!(
        !provisioning::database_exists_admin(&mut admin_conn, &db_name).await?,
        "schema-drifted lazy slot database should be removed"
    );

    Ok(())
}

#[sinex_test]
async fn test_eagerly_recreate_pruned_lazy_slot_databases_repairs_drifted_slot() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_prune_repair_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    reset_slot_database_for_test(&mut admin_conn, &db_name, &slot_url).await?;
    let meta = load_pool_meta(&mut admin_conn, &db_name)
        .await?
        .ok_or_else(|| eyre!("missing pool metadata after slot recreation"))?;
    let slot_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    sqlx::query(
        r"
        ALTER TABLE raw.source_material_registry
            DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
            ADD CONSTRAINT source_material_registry_status_check
            CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
        ",
    )
    .execute(&slot_pool)
    .await?;
    slot_pool.close().await;

    let mut summary = prune_stale_lazy_slot_databases(
        &config.admin_url,
        std::slice::from_ref(&db_name),
        &meta.fingerprint,
        &meta.extensions,
    )
    .await?;
    assert_eq!(summary.pruned_slots, vec![db_name.clone()]);
    assert!(
        !provisioning::database_exists_admin(&mut admin_conn, &db_name).await?,
        "drifted slot should be absent before eager recreation"
    );

    eagerly_recreate_pruned_lazy_slot_databases(&config.admin_url, &mut summary).await?;
    assert_eq!(
        summary.eagerly_recreated_slots,
        vec![db_name.clone()],
        "eager repair should recreate the pruned slot immediately"
    );
    assert!(
        summary.eager_recreate_failures.is_empty(),
        "eager recreation failures should be empty for a healthy slot"
    );
    assert!(
        provisioning::database_exists_admin(&mut admin_conn, &db_name).await?,
        "eager repair should restore the pruned slot database"
    );

    let repaired_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    let repaired_drift = reset::schema_mismatch_reason(&repaired_pool).await?;
    assert!(
        repaired_drift.is_none(),
        "eagerly recreated slot should match the current schema, got {repaired_drift:?}"
    );
    repaired_pool.close().await;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;

    Ok(())
}

#[sinex_test]
async fn test_prune_stale_lazy_slot_databases_skips_transiently_unavailable_clean_slot()
-> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_prune_deferred_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    reset_slot_database_for_test(&mut admin_conn, &db_name, &slot_url).await?;

    let meta = load_pool_meta(&mut admin_conn, &db_name)
        .await?
        .ok_or_else(|| eyre!("missing pool metadata after slot recreation"))?;

    let quoted = quote_ident(&db_name);
    sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS false"
    ))
    .execute(&mut admin_conn)
    .await?;

    let summary = prune_stale_lazy_slot_databases(
        &config.admin_url,
        std::slice::from_ref(&db_name),
        &meta.fingerprint,
        &meta.extensions,
    )
    .await?;
    assert_eq!(
        summary.pruned, 0,
        "transiently unavailable clean slot should not be pruned"
    );
    assert!(
        summary.locked_stale_slots.is_empty(),
        "transient schema verification deferrals should stay silent"
    );
    assert!(
        provisioning::database_exists_admin(&mut admin_conn, &db_name).await?,
        "transiently unavailable clean slot database should remain present"
    );

    sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS true"
    ))
    .execute(&mut admin_conn)
    .await?;
    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;

    Ok(())
}

#[sinex_test]
async fn test_lazy_slot_schema_drift_reason_skips_clean_slot_when_schema_probe_loses_connection()
-> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_prune_probe_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    reset_slot_database_for_test(&mut admin_conn, &db_name, &slot_url).await?;

    let slot_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    let slot_backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&slot_pool)
        .await?;

    let quoted = quote_ident(&db_name);
    sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS false"
    ))
    .execute(&mut admin_conn)
    .await?;
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(slot_backend_pid)
        .execute(&mut admin_conn)
        .await?;

    let probe_error = reset::schema_mismatch_reason(&slot_pool)
        .await
        .expect_err("schema probe should fail after the slot stops accepting connections");
    assert!(
        is_retryable_connection_report(&probe_error)
            || probe_error
                .to_string()
                .contains("not currently accepting connections"),
        "unexpected schema probe error: {probe_error:#}"
    );

    let stale_reason = lazy_slot_schema_drift_reason(&mut admin_conn, &db_name, &slot_pool).await?;
    assert!(
        stale_reason.is_none(),
        "transient schema verification loss should be treated as clean/deferred, got {stale_reason:?}"
    );
    assert!(
        provisioning::database_exists_admin(&mut admin_conn, &db_name).await?,
        "clean slot database should remain present after transient schema probe loss"
    );
    slot_pool.close().await;

    sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS true"
    ))
    .execute(&mut admin_conn)
    .await?;
    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;

    Ok(())
}

#[sinex_test]
async fn test_try_lock_slot_database_for_drop_returns_gone_for_missing_database() -> TestResult<()>
{
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_missing_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;

    let outcome = try_lock_slot_database_for_drop(&mut admin_conn, &db_name, &slot_url).await?;
    assert!(matches!(outcome, SlotDropLockOutcome::Gone));

    Ok(())
}

#[sinex_test]
async fn test_try_lock_slot_database_for_drop_defers_transiently_unavailable_database()
-> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_busy_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    reset_slot_database_for_test(&mut admin_conn, &db_name, &slot_url).await?;

    let quoted = quote_ident(&db_name);
    sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS false"
    ))
    .execute(&mut admin_conn)
    .await?;

    let outcome = try_lock_slot_database_for_drop(&mut admin_conn, &db_name, &slot_url).await?;
    assert!(matches!(outcome, SlotDropLockOutcome::Deferred));

    sqlx::query(&format!(
        "ALTER DATABASE {quoted} WITH ALLOW_CONNECTIONS true"
    ))
    .execute(&mut admin_conn)
    .await?;
    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;

    Ok(())
}

use super::*;
use crate::sandbox::db::pool::config::PoolConfig;
use crate::sandbox::db::pool::reset;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_sqlstate_classifiers() -> TestResult<()> {
    assert!(is_missing_database_code(Some("3D000")));
    assert!(!is_missing_database_code(Some("08006")));
    assert!(is_duplicate_database_code(Some("42P04")));
    assert!(is_duplicate_database_code(Some("23505")));
    assert!(!is_duplicate_database_code(Some("3D000")));
    assert!(is_too_many_clients_code(Some("53300")));
    assert!(!is_too_many_clients_code(Some("08003")));
    Ok(())
}

#[sinex_test]
async fn test_quote_ident_escapes_embedded_quotes() -> TestResult<()> {
    assert_eq!(quote_ident("sinex_test"), "\"sinex_test\"");
    assert_eq!(quote_ident("sinex\"test"), "\"sinex\"\"test\"");
    Ok(())
}

#[sinex_test]
async fn test_retryable_sqlstate_set() -> TestResult<()> {
    assert!(RETRYABLE_CONNECTION_SQLSTATES.contains(&"08006"));
    assert!(RETRYABLE_CONNECTION_SQLSTATES.contains(&"57P01"));
    assert!(!RETRYABLE_CONNECTION_SQLSTATES.contains(&"23505"));
    Ok(())
}

#[sinex_test]
async fn test_effective_connection_budget_reserves_headroom() -> TestResult<()> {
    assert_eq!(effective_connection_budget(100, 3)?, 81);
    Ok(())
}

#[sinex_test]
async fn test_effective_connection_budget_rejects_non_positive_budget() -> TestResult<()> {
    let err = effective_connection_budget(16, 3).expect_err("budget should be rejected");
    assert!(
        err.to_string().contains("non-positive"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn test_detect_connection_budget_surfaces_connect_failures() -> TestResult<()> {
    let err = detect_connection_budget("definitely-not-a-postgres-url")
        .await
        .expect_err("invalid admin url should fail");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("failed to connect while detecting PostgreSQL connection budget"),
        "missing budget detection context: {rendered}"
    );
    assert!(
        rendered.contains("Admin connection failed"),
        "missing admin connection context: {rendered}"
    );
    Ok(())
}

#[sinex_test]
async fn test_parse_template_meta_comment_rejects_invalid_json() -> TestResult<()> {
    let err = parse_database_meta_comment::<TemplateMeta>(
        "template",
        "sinex_test_template",
        "{ definitely not valid json",
    )
    .expect_err("invalid template metadata must not be treated as missing");
    assert!(
        err.to_string()
            .contains("failed to parse template database metadata comment"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn test_parse_pool_meta_comment_rejects_invalid_json() -> TestResult<()> {
    let err = parse_database_meta_comment::<PoolMeta>(
        "pool",
        "sinex_test_pool_0",
        "{ definitely not valid json",
    )
    .expect_err("invalid pool metadata must not be treated as missing");
    assert!(
        err.to_string()
            .contains("failed to parse pool database metadata comment"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn test_try_ensure_pool_database_exists_defers_when_lifecycle_lock_held() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_deferred_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;

    let lock_id = slot_lifecycle_lock_key(&db_name);
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_id)
        .execute(&mut admin_conn)
        .await?;

    let start = std::time::Instant::now();
    let outcome = try_ensure_pool_database_exists(&db_name, &slot_url).await?;
    let elapsed = start.elapsed();

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_id)
        .execute(&mut admin_conn)
        .await;

    assert_eq!(outcome, EnsurePoolDatabaseOutcome::Deferred);
    assert!(
        elapsed < Duration::from_secs(2),
        "skip-locked provisioning should defer quickly, took {elapsed:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_recreate_pool_database_converges_schema_before_marking_clean() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_recreate_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;

    recreate_pool_database(&db_name, &slot_url).await?;

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

    let drift_before = reset::schema_mismatch_reason(&slot_pool).await?;
    assert!(
        drift_before
            .as_deref()
            .is_some_and(|reason| reason.contains("source_material_registry_status_check")),
        "expected stale status constraint drift, got {drift_before:?}"
    );
    slot_pool.close().await;

    recreate_pool_database(&db_name, &slot_url).await?;

    let repaired_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    let drift_after = reset::schema_mismatch_reason(&repaired_pool).await?;
    assert_eq!(
        drift_after, None,
        "recreated pool database should be converged before metadata is marked clean"
    );
    repaired_pool.close().await;

    let pool_meta = load_pool_meta(&mut admin_conn, &db_name).await?;
    assert!(
        pool_meta.is_some(),
        "recreated pool database should persist clean metadata"
    );
    let pool_meta = pool_meta.expect("checked above");
    assert_eq!(pool_meta.fingerprint, Some(schema_fingerprint()?));
    assert!(
        !pool_meta.dirty,
        "recreated pool database must be marked clean"
    );

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    Ok(())
}

#[sinex_test]
async fn test_reconcile_existing_pool_database_refreshes_stale_metadata() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_reconcile_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    recreate_pool_database(&db_name, &slot_url).await?;

    let current_meta = load_pool_meta(&mut admin_conn, &db_name)
        .await?
        .expect("pool metadata should exist after recreation");
    let expected_extensions = current_meta.extensions.clone();

    store_pool_meta(
        &mut admin_conn,
        &db_name,
        &PoolMeta {
            fingerprint: Some("stale-fingerprint".to_string()),
            extensions: HashMap::new(),
            dirty: true,
            updated_at_rfc3339: Timestamp::now().format_rfc3339(),
            last_error: Some("stale metadata".to_string()),
        },
    )
    .await?;

    reconcile_existing_pool_database(&config.admin_url, &db_name, &slot_url, &expected_extensions)
        .await?;

    let reconciled_meta = load_pool_meta(&mut admin_conn, &db_name)
        .await?
        .expect("reconciled pool metadata should exist");
    assert_eq!(reconciled_meta.fingerprint, Some(schema_fingerprint()?));
    assert_eq!(reconciled_meta.extensions, expected_extensions);
    assert!(
        !reconciled_meta.dirty,
        "reconciled pool metadata must be clean"
    );
    assert_eq!(reconciled_meta.last_error, None);

    let slot_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    let drift = reset::schema_mismatch_reason(&slot_pool).await?;
    assert_eq!(
        drift, None,
        "reconciled pool database should be schema-clean"
    );
    slot_pool.close().await;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    Ok(())
}

#[sinex_test]
async fn test_mark_pool_database_clean_rejects_residual_schema_drift() -> TestResult<()> {
    let config = PoolConfig::default();
    let db_name = format!("sinex_test_pool_mark_clean_drift_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    recreate_pool_database(&db_name, &slot_url).await?;

    let expected_extensions = load_pool_meta(&mut admin_conn, &db_name)
        .await?
        .expect("pool metadata should exist after recreation")
        .extensions;

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
        "expected stale status constraint drift, got {drift:?}"
    );
    slot_pool.close().await;

    let error = mark_pool_database_clean(
        &mut admin_conn,
        &db_name,
        &slot_url,
        &expected_extensions,
        PoolCleanVerification::RequireSchemaVerification,
    )
    .await
    .expect_err("residual schema drift must prevent clean pool metadata");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("still has schema drift after convergence"),
        "unexpected error: {rendered}"
    );
    assert!(
        rendered.contains("source_material_registry_status_check"),
        "drift detail should be preserved: {rendered}"
    );

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    Ok(())
}

#[sinex_test]
async fn test_retryable_connection_report_treats_runtime_shutdown_as_transient() -> TestResult<()> {
    let report = eyre!(
        "error communicating with database: A Tokio 1.x context was found, but it is being shutdown."
    );

    assert!(
        is_retryable_connection_report(&report),
        "runtime shutdown communication errors should be retried via fresh cleanup pools"
    );

    Ok(())
}

#[sinex_test]
#[ignore = "sinex-lld6 open: create_database_from_template leaks the shared template_lock_id advisory lock if the exclusive clone_lock_id acquisition fails afterward"]
async fn create_database_from_template_releases_shared_lock_when_clone_lock_acquisition_fails()
-> TestResult<()> {
    let config = PoolConfig::default();
    let template_name = format!("sinex_lld6_template_{}", std::process::id());
    let template_lock_id = advisory_lock_key(&template_name);
    let clone_lock_id = advisory_lock_key(&format!("{template_name}::clone"));

    // A competing session holds the exclusive clone lock for the whole test,
    // forcing create_database_from_template's second lock acquisition to
    // block and then time out via statement_timeout, exactly the failure
    // mode this bug is about (any error there, not just a timeout, skips
    // the unlock code below it).
    let mut blocker = sqlx::PgPool::connect(&config.admin_url)
        .await?
        .acquire()
        .await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(clone_lock_id)
        .execute(blocker.as_mut())
        .await?;

    let mut conn: PoolConnection<Postgres> = sqlx::PgPool::connect(&config.admin_url)
        .await?
        .acquire()
        .await?;
    sqlx::query("SET statement_timeout = '300ms'")
        .execute(conn.as_mut())
        .await?;

    let result =
        create_database_from_template(&mut conn, "sinex_lld6_never_created", &template_name)
            .await;
    assert!(
        result.is_err(),
        "expected the blocked clone-lock acquisition to time out and error"
    );

    // Reset statement_timeout on `conn` itself before using it to check lock
    // state (the previous SET could still be in effect for the same
    // session).
    sqlx::query("SET statement_timeout = 0")
        .execute(conn.as_mut())
        .await?;

    // If the shared template lock leaked, `conn`'s OWN session still holds
    // it, so a fresh attempt from the SAME connection to acquire it
    // exclusively (which Postgres allows self-upgrading only when there is
    // no OTHER holder) would still see it as already held by this session.
    // The reliable cross-session check: a third connection's non-blocking
    // exclusive attempt must succeed once `conn`'s shared lock is truly
    // released.
    let mut checker: PoolConnection<Postgres> = sqlx::PgPool::connect(&config.admin_url)
        .await?
        .acquire()
        .await?;
    let still_locked: bool = sqlx::query_scalar("SELECT NOT pg_try_advisory_lock($1)")
        .bind(template_lock_id)
        .fetch_one(checker.as_mut())
        .await?;
    if still_locked {
        // clean up our own probe attempt state is moot -- pg_try_advisory_lock
        // failed, so nothing to unlock on `checker`.
    } else {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(template_lock_id)
            .execute(checker.as_mut())
            .await?;
    }

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(clone_lock_id)
        .execute(blocker.as_mut())
        .await;

    assert!(
        !still_locked,
        "template_lock_id shared advisory lock must be released even when the subsequent \
         exclusive clone_lock_id acquisition fails -- it leaked because the second lock's \
         `?` early-returns past the unconditional unlock statements at the end of \
         create_database_from_template"
    );
    Ok(())
}

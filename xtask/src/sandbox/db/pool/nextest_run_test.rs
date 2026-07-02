use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn cached_lazy_pool_preparation_matches_identical_request() -> TestResult<()> {
    let slot_names = vec![
        "sinex_test_pool_0".to_string(),
        "sinex_test_pool_1".to_string(),
    ];
    let cached = CachedLazyPoolPreparation {
        expected_fingerprint: Some("abc".to_string()),
        expected_extensions: HashMap::from([("timescaledb".to_string(), "2.20".to_string())]),
        slot_names: slot_names.clone(),
        deferred_stale_slots: Vec::new(),
        next_deferred_retry_at_rfc3339: None,
        prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
    };

    assert!(cached.matches_request(&slot_names, &Some("abc".to_string())));
    assert!(!cached.matches_request(&slot_names, &Some("def".to_string())));
    Ok(())
}

#[sinex_test]
async fn cached_preparation_roundtrip_uses_repo_local_state_layout() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let (state_path, lock_path) = preparation_paths_in(temp.path(), "run-123");
    assert!(
        state_path.ends_with("sandbox-db-pool/nextest-runs/run-123.json"),
        "unexpected state path: {}",
        state_path.display()
    );
    assert!(
        lock_path.ends_with("sandbox-db-pool/nextest-runs/run-123.lock"),
        "unexpected lock path: {}",
        lock_path.display()
    );

    let cached = CachedLazyPoolPreparation {
        expected_fingerprint: Some("fingerprint".to_string()),
        expected_extensions: HashMap::from([("pg_trgm".to_string(), "1.6".to_string())]),
        slot_names: vec!["sinex_test_pool_0".to_string()],
        deferred_stale_slots: Vec::new(),
        next_deferred_retry_at_rfc3339: None,
        prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
    };
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)?;
    }
    store_cached_preparation(&state_path, &cached)?;

    let loaded = load_cached_preparation(&state_path)?
        .ok_or_else(|| eyre!("cached preparation should load"))?;
    assert_eq!(loaded, cached);
    Ok(())
}

#[sinex_test]
async fn cached_preparation_reuse_does_not_wait_for_writer_lock() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let slot_names = vec![
        "sinex_test_pool_0".to_string(),
        "sinex_test_pool_1".to_string(),
    ];
    let (state_path, lock_path) = preparation_paths_in(temp.path(), "run-456");
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let cached = CachedLazyPoolPreparation {
        expected_fingerprint: Some("fingerprint".to_string()),
        expected_extensions: HashMap::from([("timescaledb".to_string(), "2.20".to_string())]),
        slot_names: slot_names.clone(),
        deferred_stale_slots: Vec::new(),
        next_deferred_retry_at_rfc3339: None,
        prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
    };
    store_cached_preparation(&state_path, &cached)?;

    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    let _lock_file =
        Flock::lock(lock_file, FlockArg::LockExclusive).map_err(|(_lock_file, error)| error)?;

    let reused = try_reuse_cached_preparation(
        &state_path,
        &slot_names,
        &Some("fingerprint".to_string()),
        Timestamp::now(),
    )?
    .ok_or_else(|| eyre!("cached preparation should be reusable"))?;

    assert_eq!(reused.slot_names, slot_names);
    assert_eq!(
        reused.expected_extensions.get("timescaledb"),
        Some(&"2.20".to_string())
    );
    assert!(reused.prune_summary.pruned_slots.is_empty());
    Ok(())
}

#[sinex_test]
async fn deferred_cached_preparation_waits_for_retry_deadline() -> TestResult<()> {
    let cached = CachedLazyPoolPreparation {
        expected_fingerprint: Some("fingerprint".to_string()),
        expected_extensions: HashMap::new(),
        slot_names: vec!["sinex_test_pool_0".to_string()],
        deferred_stale_slots: vec![DeferredStaleSlot {
            slot_name: "sinex_test_pool_0".to_string(),
            stale_reason: "schema drift".to_string(),
        }],
        next_deferred_retry_at_rfc3339: Some(
            (Timestamp::now() + TimeDuration::seconds(60)).format_rfc3339(),
        ),
        prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
    };
    let temp = tempfile::tempdir()?;
    let (state_path, _) = preparation_paths_in(temp.path(), "run-deferred");
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)?;
    }
    store_cached_preparation(&state_path, &cached)?;

    let reused = try_reuse_cached_preparation(
        &state_path,
        &cached.slot_names,
        &cached.expected_fingerprint,
        Timestamp::now(),
    )?;
    assert!(
        reused.is_some(),
        "deferred stale slot should still reuse cache before retry deadline"
    );

    Ok(())
}

#[sinex_test]
async fn retry_deferred_stale_slots_repairs_schema_drifted_slot() -> TestResult<()> {
    use super::super::connect_admin_with_retry;
    use super::super::drop_database_if_exists_admin;
    use super::super::load_pool_meta;
    use super::super::recreate_pool_database;
    use super::super::reset;
    use super::super::url_with_db_name;
    use super::super::wait_for_database_absence_admin;

    let config = super::super::config::PoolConfig::default();
    let db_name = format!("sinex_test_pool_retry_deferred_{}", std::process::id());
    let slot_url = url_with_db_name(&config.base_url, &db_name)?;
    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    recreate_pool_database(&db_name, &slot_url).await?;
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
        drift.is_some(),
        "test fixture should create real schema drift"
    );
    slot_pool.close().await;

    let mut cached = CachedLazyPoolPreparation {
        expected_fingerprint: meta.fingerprint,
        expected_extensions: meta.extensions,
        slot_names: vec![db_name.clone()],
        deferred_stale_slots: vec![DeferredStaleSlot {
            slot_name: db_name.clone(),
            stale_reason: "actual schema drift".to_string(),
        }],
        next_deferred_retry_at_rfc3339: None,
        prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
    };

    let summary = retry_deferred_stale_slots(&config.admin_url, &mut cached).await?;
    assert_eq!(summary.pruned_slots, vec![db_name.clone()]);
    assert_eq!(summary.eagerly_recreated_slots, vec![db_name.clone()]);
    assert!(
        cached.deferred_stale_slots.is_empty(),
        "successful deferred retry should clear stale-slot backlog"
    );

    let repaired_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&slot_url)
        .await?;
    let repaired_drift = reset::schema_mismatch_reason(&repaired_pool).await?;
    assert!(
        repaired_drift.is_none(),
        "deferred retry should restore schema-clean slot, got {repaired_drift:?}"
    );
    repaired_pool.close().await;

    drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
    wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
    Ok(())
}

#[sinex_test]
async fn unreadable_cached_preparation_is_ignored_and_removed() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let (state_path, _) = preparation_paths_in(temp.path(), "run-bad");
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&state_path, "{ definitely-not-json")?;

    let loaded = load_cached_preparation(&state_path)?;
    assert!(
        loaded.is_none(),
        "corrupt preparation state should be ignored"
    );
    assert!(
        !state_path.exists(),
        "corrupt preparation state should be removed for a clean retry"
    );
    Ok(())
}

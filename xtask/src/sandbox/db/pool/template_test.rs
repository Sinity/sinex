// Small inline test is justified here because it verifies the private
// fingerprint source list directly.
use super::{
    ADHOC_TEMPLATE_BASE_NAME, SHARED_POOL_TEMPLATE_SHARD_COUNT, SHARED_TEMPLATE_BASE_NAME,
    check_template_reuse, connect_admin_with_retry, ensure_template_database_for_key,
    is_managed_pool_slot_name, load_template_trust_stamp, normalize_adhoc_template_key,
    quote_ident, schema_fingerprint, schema_fingerprint_sources, schema_fingerprint_sources_in,
    store_template_meta, store_template_trust_stamp, template_name_for_key,
    template_names_for_keys, template_trust_matches, template_trust_state_path_in,
};
use crate::sandbox::db::pool::PoolConfig;
use crate::sandbox::db::pool::config::{SLOT_MAX_CONNECTIONS, replace_db_name};
use crate::sandbox::db::pool::meta::TemplateMeta;
use crate::sandbox::db::pool::provisioning::{
    create_database_from_template_admin, wait_for_database_absence_admin,
};
use sinex_primitives::temporal::Timestamp;
use std::collections::HashMap;
use std::fs;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn schema_fingerprint_includes_convergence_inputs() -> TestResult<()> {
    let sources = schema_fingerprint_sources()?;
    let file_names = sources
        .iter()
        .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
        .map(str::to_owned)
        .collect::<Vec<String>>();

    assert!(file_names.iter().any(|name| name == "apply.rs"));
    assert!(file_names.iter().any(|name| name == "converge.rs"));
    assert!(file_names.iter().any(|name| name == "schema_registry.rs"));
    Ok(())
}

#[sinex_test]
async fn schema_fingerprint_sources_report_unreadable_schema_root() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let schema_root = temp.path().join("schema-root");
    fs::create_dir_all(&schema_root)?;
    fs::write(schema_root.join("schema"), "not-a-directory")?;

    let error = schema_fingerprint_sources_in(&schema_root)
        .expect_err("non-directory schema root should fail honestly");
    let message = format!("{error:#}");
    assert!(message.contains("failed to enumerate schema sources"));
    Ok(())
}

#[sinex_test]
async fn schema_fingerprint_is_computable_for_workspace_sources() -> TestResult<()> {
    let fingerprint = schema_fingerprint()?;
    assert!(!fingerprint.is_empty());
    Ok(())
}

#[sinex_test]
async fn template_trust_stamp_roundtrip_supports_fast_match() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let path = template_trust_state_path_in(dir.path(), SHARED_TEMPLATE_BASE_NAME);
    let stamp = super::TemplateTrustStamp {
        template_name: SHARED_TEMPLATE_BASE_NAME.to_string(),
        fingerprint: Some("fingerprint-1".to_string()),
        extensions: HashMap::from([("timescaledb".to_string(), "2.18.0".to_string())]),
        trusted_at_rfc3339: Timestamp::now().format_rfc3339(),
    };

    store_template_trust_stamp(&path, &stamp)?;
    assert_eq!(load_template_trust_stamp(&path)?, Some(stamp.clone()));
    assert!(template_trust_matches(
        &path,
        SHARED_TEMPLATE_BASE_NAME,
        &stamp.fingerprint,
        &stamp.extensions,
    )?);
    Ok(())
}

#[sinex_test]
async fn unreadable_template_trust_stamp_is_removed() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let path = template_trust_state_path_in(dir.path(), SHARED_TEMPLATE_BASE_NAME);
    std::fs::create_dir_all(
        path.parent()
            .expect("template trust path should have parent"),
    )?;
    std::fs::write(&path, "{ not-json }")?;

    assert!(load_template_trust_stamp(&path)?.is_none());
    assert!(!path.exists(), "unreadable trust stamp should be removed");
    Ok(())
}

#[sinex_test]
async fn template_name_for_key_is_stable() -> TestResult<()> {
    let first = template_name_for_key("slot-alpha");
    let second = template_name_for_key("slot-alpha");
    assert_eq!(first, second, "template sharding must be deterministic");
    Ok(())
}

#[sinex_test]
async fn template_name_for_key_reuses_semantic_adhoc_family_across_pid_suffixes()
-> TestResult<()> {
    assert_eq!(
        normalize_adhoc_template_key("sinex_test_pool_prune_repair_1234"),
        "sinex_test_pool_prune_repair"
    );
    let first = template_name_for_key("sinex_test_pool_prune_repair_1234");
    let second = template_name_for_key("sinex_test_pool_prune_repair_9876");
    assert!(
        first.starts_with(ADHOC_TEMPLATE_BASE_NAME),
        "ad hoc template names should use the dedicated ad hoc family"
    );
    assert_eq!(
        first, second,
        "ephemeral numeric suffixes should not force fresh template families"
    );
    Ok(())
}

#[sinex_test]
async fn template_name_for_key_keeps_managed_pool_slots_on_shared_family() -> TestResult<()> {
    let names = (0..64)
        .map(|index| template_name_for_key(&format!("sinex_test_pool_{index}")))
        .collect::<std::collections::HashSet<_>>();
    assert!(
        names
            .iter()
            .all(|name| name.starts_with(SHARED_TEMPLATE_BASE_NAME)),
        "managed pool slots should stay on the legacy shared-template family"
    );
    assert!(
        names.len() <= SHARED_POOL_TEMPLATE_SHARD_COUNT,
        "managed pool slots should not fan out beyond the fixed pool shard count"
    );
    assert!(is_managed_pool_slot_name("sinex_test_pool_7"));
    assert!(!is_managed_pool_slot_name("sinex_test_pool_recreate_7"));
    Ok(())
}

#[sinex_test]
async fn template_name_for_key_isolates_distinct_adhoc_semantic_families() -> TestResult<()> {
    let first = template_name_for_key("sinex_test_pool_recreate_1234");
    let second = template_name_for_key("sinex_test_template_shared_drift_1234");
    assert!(
        first.starts_with(ADHOC_TEMPLATE_BASE_NAME)
            && second.starts_with(ADHOC_TEMPLATE_BASE_NAME),
        "non-managed names should use the dedicated ad hoc template family"
    );
    assert_ne!(
        first, second,
        "distinct semantic ad hoc keys should not convoy on one shared template family"
    );
    Ok(())
}

#[sinex_test]
async fn template_names_for_keys_deduplicates_families() -> TestResult<()> {
    let keys = vec![
        "sinex_test_pool_0".to_string(),
        "sinex_test_pool_0".to_string(),
        "sinex_test_pool_1".to_string(),
        "sinex_test_pool_2".to_string(),
    ];
    let names = template_names_for_keys(&keys);
    let unique = names.iter().collect::<std::collections::HashSet<_>>();
    assert_eq!(
        names.len(),
        unique.len(),
        "template warming should only visit each family once"
    );
    Ok(())
}

#[sinex_test]
async fn template_reuse_rejects_actual_schema_drift() -> TestResult<()> {
    let config = PoolConfig::default();
    let template_name = format!("sinex_test_template_drift_{}", std::process::id());
    let desired_fingerprint = Some(schema_fingerprint()?);

    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;
    let quoted_template = quote_ident(&template_name);
    let drop_query = format!("DROP DATABASE IF EXISTS {quoted_template} WITH (FORCE)");
    sqlx::query(&drop_query).execute(&mut admin_conn).await?;
    wait_for_database_absence_admin(&mut admin_conn, &template_name).await?;

    let shared_guard = ensure_template_database_for_key(
        &config.admin_url,
        &config.base_url,
        SLOT_MAX_CONNECTIONS,
        &template_name,
    )
    .await?;
    let shared_template_name = shared_guard.info.name.clone();
    let shared_extensions = shared_guard.info.extensions.clone();
    shared_guard.release().await?;

    create_database_from_template_admin(&mut admin_conn, &template_name, &shared_template_name)
        .await?;
    let template_admin_url = replace_db_name(&config.admin_url, &template_name);
    store_template_meta(
        &mut admin_conn,
        &template_name,
        &TemplateMeta {
            fingerprint: desired_fingerprint
                .clone()
                .expect("desired fingerprint must be present"),
            extensions: shared_extensions,
        },
    )
    .await?;

    let template_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&template_admin_url)
        .await?;
    sqlx::query(
        r"
        ALTER TABLE raw.source_material_registry
            DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
            ADD CONSTRAINT source_material_registry_status_check
            CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
        ",
    )
    .execute(&template_pool)
    .await?;
    template_pool.close().await;

    let reusable = check_template_reuse(
        &mut admin_conn,
        &config.admin_url,
        &template_name,
        &desired_fingerprint,
        true,
    )
    .await?;
    assert!(
        reusable.is_none(),
        "template with actual schema drift must be recreated instead of reused"
    );

    let drop_query = format!("DROP DATABASE IF EXISTS {quoted_template} WITH (FORCE)");
    sqlx::query(&drop_query).execute(&mut admin_conn).await?;
    Ok(())
}

#[sinex_test]
async fn template_reuse_rejects_actual_schema_drift_on_shared_fast_path() -> TestResult<()> {
    let config = PoolConfig::default();
    let template_name = format!("sinex_test_template_shared_drift_{}", std::process::id());
    let desired_fingerprint = Some(schema_fingerprint()?);

    let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;
    let quoted_template = quote_ident(&template_name);
    let drop_query = format!("DROP DATABASE IF EXISTS {quoted_template} WITH (FORCE)");
    sqlx::query(&drop_query).execute(&mut admin_conn).await?;
    wait_for_database_absence_admin(&mut admin_conn, &template_name).await?;

    let shared_guard = ensure_template_database_for_key(
        &config.admin_url,
        &config.base_url,
        SLOT_MAX_CONNECTIONS,
        &template_name,
    )
    .await?;
    let shared_template_name = shared_guard.info.name.clone();
    let shared_extensions = shared_guard.info.extensions.clone();
    shared_guard.release().await?;

    create_database_from_template_admin(&mut admin_conn, &template_name, &shared_template_name)
        .await?;
    let template_admin_url = replace_db_name(&config.admin_url, &template_name);
    store_template_meta(
        &mut admin_conn,
        &template_name,
        &TemplateMeta {
            fingerprint: desired_fingerprint
                .clone()
                .expect("desired fingerprint must be present"),
            extensions: shared_extensions,
        },
    )
    .await?;

    let template_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&template_admin_url)
        .await?;
    sqlx::query(
        r"
        ALTER TABLE raw.source_material_registry
            DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
            ADD CONSTRAINT source_material_registry_status_check
            CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
        ",
    )
    .execute(&template_pool)
    .await?;
    template_pool.close().await;

    let reusable = check_template_reuse(
        &mut admin_conn,
        &config.admin_url,
        &template_name,
        &desired_fingerprint,
        false,
    )
    .await?;
    assert!(
        reusable.is_none(),
        "shared fast-path reuse must reject actual schema drift instead of trusting metadata"
    );

    let drop_query = format!("DROP DATABASE IF EXISTS {quoted_template} WITH (FORCE)");
    sqlx::query(&drop_query).execute(&mut admin_conn).await?;
    Ok(())
}

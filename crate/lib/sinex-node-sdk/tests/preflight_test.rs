// Preflight Unit Tests - Comprehensive verification phase testing

use async_nats::jetstream;
use serde_json::Value;
use sinex_node_sdk::preflight::{
    VerificationStatus, configuration, database, resources, services, verification,
};
use sinex_primitives::constants::timeouts;
use sinex_primitives::{environment::SinexEnvironment, nats::JetStreamTopology};
use std::env;
use std::ffi::OsString;
use std::fs;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::time::timeout;
use xtask::sandbox::prelude::*;

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn database_environment_name(database_url: &str) -> Option<String> {
    database_url
        .split('?')
        .next()
        .and_then(|url| url.rsplit('/').next())
        .and_then(|database_name| database_name.rsplit_once('_'))
        .map(|(_, suffix)| suffix.to_string())
}

async fn with_database_url<F, T>(database_url: &str, f: F) -> TestResult<T>
where
    F: AsyncFnOnce() -> TestResult<T>,
{
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = env::var("DATABASE_URL").ok();
    let previous_environment = env::var("SINEX_ENVIRONMENT").ok();
    let environment_name = database_environment_name(database_url);
    let _environment_guard = environment_name
        .as_deref()
        .map(sinex_primitives::environment::override_environment_for_tests)
        .transpose()?;
    unsafe { env::set_var("DATABASE_URL", database_url) };
    unsafe {
        match &environment_name {
            Some(value) => env::set_var("SINEX_ENVIRONMENT", value),
            None => env::remove_var("SINEX_ENVIRONMENT"),
        }
    }
    let result = f().await;
    unsafe {
        match previous {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        match previous_environment {
            Some(value) => env::set_var("SINEX_ENVIRONMENT", value),
            None => env::remove_var("SINEX_ENVIRONMENT"),
        }
    }
    result
}

async fn without_database_url<F, T>(f: F) -> TestResult<T>
where
    F: AsyncFnOnce() -> TestResult<T>,
{
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = env::var("DATABASE_URL").ok();
    unsafe { env::remove_var("DATABASE_URL") };
    let result = f().await;
    unsafe {
        match previous {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
    }
    result
}

async fn with_env_vars<F, T>(pairs: &[(&str, String)], f: F) -> TestResult<T>
where
    F: AsyncFnOnce() -> TestResult<T>,
{
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous: Vec<(String, Option<String>)> = pairs
        .iter()
        .map(|(key, _)| ((*key).to_string(), env::var(key).ok()))
        .collect();
    for (key, value) in pairs {
        unsafe { env::set_var(key, value) };
    }
    let result = f().await;
    unsafe {
        for (key, value) in previous {
            match value {
                Some(original) => env::set_var(key, original),
                None => env::remove_var(key),
            }
        }
    }
    result
}

async fn with_database_url_absent_and_env_vars<F, T>(
    pairs: &[(&str, String)],
    f: F,
) -> TestResult<T>
where
    F: AsyncFnOnce() -> TestResult<T>,
{
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous_database_url = env::var("DATABASE_URL").ok();
    let previous_pairs: Vec<(String, Option<String>)> = pairs
        .iter()
        .map(|(key, _)| ((*key).to_string(), env::var(key).ok()))
        .collect();

    unsafe { env::remove_var("DATABASE_URL") };
    for (key, value) in pairs {
        unsafe { env::set_var(key, value) };
    }

    let result = f().await;

    unsafe {
        match previous_database_url {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        for (key, value) in previous_pairs {
            match value {
                Some(original) => env::set_var(key, original),
                None => env::remove_var(key),
            }
        }
    }

    result
}

fn write_executable_script(path: &std::path::Path, body: &str) -> TestResult<()> {
    fs::write(path, body)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

async fn ensure_preflight_streams(
    js: &jetstream::Context,
    env: &SinexEnvironment,
) -> TestResult<String> {
    let expected_checkpoint_bucket = format!("KV_{}", env.nats_kv_bucket_name("sinex_checkpoints"));
    let topology = JetStreamTopology::new(
        env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "preflight-test-consumer".to_string(),
        None,
    );
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: topology.events_stream.clone(),
            subjects: vec![env.nats_subject("events.>")],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: topology.confirmations_stream.clone(),
            subjects: vec![format!("{}_CONFIRMATIONS", topology.events_stream)],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
            subjects: vec![env.nats_subject("source_material.begin")],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
            subjects: vec![env.nats_subject("source_material.slices.>")],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_END"),
            subjects: vec![env.nats_subject("source_material.end")],
            ..Default::default()
        })
        .await?;
    let _ = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: expected_checkpoint_bucket.clone(),
            ..Default::default()
        })
        .await?;

    Ok(expected_checkpoint_bucket)
}

async fn ensure_preflight_event_streams_only(
    js: &jetstream::Context,
    env: &SinexEnvironment,
) -> TestResult<()> {
    let topology = JetStreamTopology::new(
        env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "preflight-test-consumer".to_string(),
        None,
    );
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: topology.events_stream.clone(),
            subjects: vec![env.nats_subject("events.>")],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: topology.confirmations_stream.clone(),
            subjects: vec![format!("{}_CONFIRMATIONS", topology.events_stream)],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
            subjects: vec![env.nats_subject("source_material.begin")],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
            subjects: vec![env.nats_subject("source_material.slices.>")],
            ..Default::default()
        })
        .await?;
    let _ = js
        .get_or_create_stream(jetstream::stream::Config {
            name: env.nats_stream_name("SOURCE_MATERIAL_END"),
            subjects: vec![env.nats_subject("source_material.end")],
            ..Default::default()
        })
        .await?;
    Ok(())
}

fn write_valid_atuin_history_db(path: &std::path::Path) -> TestResult<()> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute(
        "CREATE TABLE history (
            id TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            command TEXT NOT NULL,
            cwd TEXT NOT NULL,
            session TEXT NOT NULL,
            hostname TEXT NOT NULL,
            exit INTEGER NOT NULL,
            duration INTEGER NOT NULL,
            deleted_at INTEGER
        )",
        [],
    )?;
    Ok(())
}

fn write_valid_activitywatch_db(path: &std::path::Path) -> TestResult<()> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute(
        "CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        [],
    )?;
    conn.execute(
        "CREATE TABLE events (
            bucketrow INTEGER NOT NULL,
            starttime INTEGER NOT NULL,
            endtime INTEGER NOT NULL,
            data TEXT
        )",
        [],
    )?;
    Ok(())
}

/// Test basic VerificationStatus functionality
#[sinex_test]
async fn test_verification_status_basic() -> TestResult<()> {
    // Test that VerificationStatus enum works correctly
    assert_eq!(VerificationStatus::Pass, VerificationStatus::Pass);
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Fail);

    // Test enum variants exist
    let _pass = VerificationStatus::Pass;
    let _warn = VerificationStatus::Warning;
    let _fail = VerificationStatus::Fail;

    Ok(())
}

/// Test verification status comparisons
#[sinex_test]
async fn test_verification_status_comparisons() -> TestResult<()> {
    // Test basic equality
    assert_eq!(VerificationStatus::Pass, VerificationStatus::Pass);
    assert_eq!(VerificationStatus::Warning, VerificationStatus::Warning);
    assert_eq!(VerificationStatus::Fail, VerificationStatus::Fail);

    // Test inequality
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Warning);
    assert_ne!(VerificationStatus::Warning, VerificationStatus::Fail);
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Fail);

    Ok(())
}

// ====== PHASE 1: DATABASE CONNECTIVITY TESTS ======

/// Test Phase 1: Database connectivity verification with valid connection
#[sinex_test]
async fn test_phase1_database_connectivity_success(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (status, details, messages) = database::verify_database_connectivity().await?;

        assert_eq!(status, VerificationStatus::Pass);
        assert!(!messages.is_empty());
        assert!(messages.iter().any(|m| m.contains("Database connection")));

        let details = details.as_object().expect("details should be an object");
        assert!(details.contains_key("database_url"));
        assert!(details.contains_key("postgresql_version"));
        assert!(details.contains_key("connection_pool"));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 1: Database connectivity with invalid URL
#[sinex_test]
async fn test_phase1_database_connectivity_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let (status, _details, messages) = database::verify_database_connectivity().await?;

        assert_eq!(status, VerificationStatus::Fail);
        assert!(messages.iter().any(|m| m.contains("Database connection")));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 1: Database connectivity timeout handling
#[sinex_test]
async fn test_phase1_database_connectivity_timeout() -> TestResult<()> {
    with_database_url("postgresql://192.0.2.1:5432/test", || async {
        let result = timeout(
            timeouts::PREFLIGHT_DATABASE_TIMEOUT + Duration::from_secs(2),
            database::verify_database_connectivity(),
        )
        .await;

        match result {
            Ok(Ok((status, _details, messages))) => {
                assert_eq!(status, VerificationStatus::Fail);
                assert!(
                    messages
                        .iter()
                        .any(|m| m.contains("timeout") || m.contains("Database connection"))
                );
            }
            Ok(Err(_)) => {
                // Connection error is also acceptable
            }
            Err(e) => {
                panic!("Database connectivity test should have internal timeout handling: {e}");
            }
        }

        Ok(())
    })
    .await?;

    Ok(())
}

// ====== PHASE 2: POSTGRESQL EXTENSIONS TESTS ======

/// Test Phase 2: PostgreSQL extensions verification
#[sinex_test]
async fn test_phase2_postgresql_extensions(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (_status, details, messages) = database::verify_postgresql_extensions().await?;

        assert!(!messages.is_empty());

        let details = details.as_object().expect("details should be an object");
        let extensions = details
            .get("extensions")
            .and_then(Value::as_object)
            .expect("extensions details should be present");

        assert!(extensions.contains_key("timescaledb"));
        assert!(extensions.contains_key("pg_jsonschema"));
        assert!(extensions.contains_key("vector"));
        assert!(extensions.contains_key("pg_trgm"));
        for extension in extensions.values() {
            let extension = extension
                .as_object()
                .expect("extension detail should be an object");
            assert!(extension.contains_key("available"));
            assert!(extension.contains_key("installed"));
            assert!(extension.contains_key("default_version"));
            assert!(
                !extension.contains_key("can_install"),
                "read-only preflight must not guess installability with CREATE EXTENSION probes"
            );
        }

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 2: Extensions verification with database connection failure
#[sinex_test]
async fn test_phase2_extensions_db_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let result = database::verify_postgresql_extensions().await;
        assert!(result.is_err());
        Ok(())
    })
    .await?;

    Ok(())
}

// ====== PHASE 3: SCHEMA READINESS TESTS ======

/// Test Phase 3: Schema readiness verification
#[sinex_test]
async fn test_phase3_schema_readiness(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (status, details, messages) = database::verify_schema_readiness().await?;

        assert!(!messages.is_empty());

        let details = details.as_object().expect("details should be an object");
        assert!(details.contains_key("current_schema"));
        assert!(details.contains_key("schema_sources"));
        if matches!(status, VerificationStatus::Fail) {
            assert!(
                messages
                    .iter()
                    .any(|m| m.contains("drift") || m.contains("failed")),
                "expected diagnostic message for failed schema readiness"
            );
        }

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 3: Schema readiness with invalid database
#[sinex_test]
async fn test_phase3_schema_readiness_db_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let result = database::verify_schema_readiness().await;
        assert!(result.is_err());
        Ok(())
    })
    .await?;

    Ok(())
}

// ====== PHASE 4: SYSTEM RESOURCES TESTS ======

/// Test Phase 4: System resources verification success
#[sinex_test]
async fn test_phase4_system_resources_success() -> TestResult<()> {
    let (_status, details, messages) = resources::verify_system_resources().await?;

    assert!(!messages.is_empty());

    let details = details.as_object().expect("details should be an object");
    let memory = details
        .get("memory")
        .and_then(Value::as_object)
        .expect("memory details should be present");
    assert!(memory.contains_key("total_gb"));
    assert!(memory.contains_key("available_gb"));
    assert!(memory.contains_key("meets_requirements"));

    Ok(())
}

/// Test Phase 4: Filesystem permissions verification with temp directory
#[sinex_test]
async fn test_phase4_filesystem_permissions() -> TestResult<()> {
    let root = tempfile::tempdir()?;
    let state_dir = root.path().join("state");
    let data_dir = root.path().join("data");
    let log_dir = root.path().join("logs");
    let tmp_dir = root.path().join("tmp");
    let work_dir = root.path().join("work");

    for dir in [&state_dir, &data_dir, &log_dir, &tmp_dir, &work_dir] {
        fs::create_dir_all(dir)
            .map_err(|e| color_eyre::eyre::eyre!("Failed to create {}: {}", dir.display(), e))?;
    }

    let state_dir_str = state_dir.display().to_string();
    let data_dir_str = data_dir.display().to_string();
    let log_dir_str = log_dir.display().to_string();
    let tmp_dir_str = tmp_dir.display().to_string();
    let work_dir_str = work_dir.display().to_string();

    with_env_vars(
        &[
            ("SINEX_STATE_DIR", state_dir_str.clone()),
            ("SINEX_DATA_DIR", data_dir_str.clone()),
            ("SINEX_LOG_DIR", log_dir_str.clone()),
            ("TMPDIR", tmp_dir_str.clone()),
            ("SINEX_WORK_DIR", work_dir_str.clone()),
        ],
        || async {
            let (status, details, messages) = resources::verify_system_resources().await?;

            assert_ne!(
                status,
                VerificationStatus::Fail,
                "filesystem verifier should accept existing writable dirs; messages={messages:#?}"
            );

            let directories = details
                .get("filesystem")
                .and_then(|value| value.get("directories"))
                .and_then(Value::as_object)
                .expect("filesystem directory details should be present");

            for expected_dir in [
                &state_dir_str,
                &data_dir_str,
                &log_dir_str,
                &tmp_dir_str,
                &work_dir_str,
            ] {
                let entry = directories
                    .get(expected_dir.as_str())
                    .and_then(Value::as_object)
                    .unwrap_or_else(|| panic!("missing directory details for {expected_dir}"));
                assert_eq!(entry.get("exists").and_then(Value::as_bool), Some(true));
                assert_eq!(
                    entry.get("is_directory").and_then(Value::as_bool),
                    Some(true)
                );
                assert_eq!(entry.get("writable").and_then(Value::as_bool), Some(true));
            }

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 4: Filesystem permissions verification does not fabricate missing work directories
#[sinex_test]
async fn test_phase4_filesystem_permissions_missing_work_dir_fails_honestly() -> TestResult<()> {
    let root = tempfile::tempdir()?;
    let state_dir = root.path().join("state");
    let data_dir = root.path().join("data");
    let log_dir = root.path().join("logs");
    let tmp_dir = root.path().join("tmp");
    let work_dir = root.path().join("work-missing");

    for dir in [&state_dir, &data_dir, &log_dir, &tmp_dir] {
        fs::create_dir_all(dir)
            .map_err(|e| color_eyre::eyre::eyre!("Failed to create {}: {}", dir.display(), e))?;
    }

    let state_dir_str = state_dir.display().to_string();
    let data_dir_str = data_dir.display().to_string();
    let log_dir_str = log_dir.display().to_string();
    let tmp_dir_str = tmp_dir.display().to_string();
    let work_dir_str = work_dir.display().to_string();

    with_env_vars(
        &[
            ("SINEX_STATE_DIR", state_dir_str),
            ("SINEX_DATA_DIR", data_dir_str),
            ("SINEX_LOG_DIR", log_dir_str),
            ("TMPDIR", tmp_dir_str),
            ("SINEX_WORK_DIR", work_dir_str.clone()),
        ],
        || async {
            let (status, details, messages) = resources::verify_system_resources().await?;

            assert_eq!(status, VerificationStatus::Warning);
            assert!(
                messages
                    .iter()
                    .any(|message| message.contains(&work_dir_str)
                        && message.contains("can be created")),
                "creatable missing work dir should be reported honestly; messages={messages:#?}"
            );
            assert!(
                !work_dir.exists(),
                "preflight verification must not create missing work directories"
            );

            let work_dir_details = details
                .get("filesystem")
                .and_then(|value| value.get("directories"))
                .and_then(Value::as_object)
                .and_then(|dirs| dirs.get(work_dir_str.as_str()))
                .and_then(Value::as_object)
                .expect("missing work dir should still appear in filesystem details");
            assert_eq!(
                work_dir_details.get("exists").and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                work_dir_details.get("writable").and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                work_dir_details.get("creatable").and_then(Value::as_bool),
                Some(true)
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 4: Resource verification surfaces non-UTF8 directory overrides
#[cfg(unix)]
#[sinex_test]
async fn test_phase4_system_resources_rejects_non_unicode_work_dir() -> TestResult<()> {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = env::var_os("SINEX_WORK_DIR");
    unsafe {
        env::set_var(
            "SINEX_WORK_DIR",
            OsString::from_vec(vec![0x2f, 0x74, 0x6d, 0x80]),
        );
    };

    let (status, _details, messages) = resources::verify_system_resources().await?;

    unsafe {
        match previous {
            Some(value) => env::set_var("SINEX_WORK_DIR", value),
            None => env::remove_var("SINEX_WORK_DIR"),
        }
    }

    assert_eq!(status, VerificationStatus::Fail);
    assert!(
        messages.iter().any(
            |message| message.contains("SINEX_WORK_DIR") && message.contains("not valid UTF-8")
        ),
        "expected invalid UTF-8 work dir message, got {messages:#?}"
    );
    Ok(())
}

// ====== PHASE 5: CONFIGURATION TESTS ======

/// Test Phase 5: Configuration verification success
#[sinex_test]
async fn test_phase5_configuration_success(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_database_url(&db_url, || async {
        let (_status, details, messages) = configuration::verify_configuration_generation().await?;

        assert!(!messages.is_empty());

        let details = details.as_object().expect("details should be an object");
        assert!(details.contains_key("environment"));

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 5: Missing deployment descriptor downgrades configuration readiness to warning
#[sinex_test]
async fn test_phase5_configuration_warns_without_deployment_descriptor(
    ctx: TestContext,
) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    with_env_vars(
        &[
            ("DATABASE_URL", db_url),
            ("SINEX_DEPLOYMENT_READINESS_CONFIG", String::new()),
        ],
        || async {
            let (status, details, messages) =
                configuration::verify_configuration_generation().await?;

            assert_eq!(status, VerificationStatus::Warning);
            assert_eq!(
                details
                    .get("event_sources")
                    .and_then(|value| value.get("deployment_descriptor_loaded"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert!(
                messages
                    .iter()
                    .any(|message| message.contains("Deployment descriptor is missing"))
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 5: Malformed deployment descriptor fails configuration readiness loudly
#[sinex_test]
async fn test_phase5_configuration_fails_on_malformed_deployment_descriptor(
    ctx: TestContext,
) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    let temp = tempfile::tempdir()?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(&descriptor_path, "{ this is not valid json")?;

    with_env_vars(
        &[
            ("DATABASE_URL", db_url),
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
        ],
        || async {
            let (status, _details, messages) =
                configuration::verify_configuration_generation().await?;

            assert_eq!(status, VerificationStatus::Fail);
            assert!(
                messages.iter().any(|message| {
                    message.contains("failed to parse deployment readiness descriptor")
                }),
                "expected malformed descriptor to fail loudly, got {messages:#?}"
            );
            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 5: Configuration with missing environment variables
#[sinex_test]
async fn test_phase5_configuration_missing_env() -> TestResult<()> {
    without_database_url(|| async {
        let (status, _details, messages) = configuration::verify_configuration_generation().await?;

        assert_eq!(status, VerificationStatus::Fail);
        assert!(
            messages
                .iter()
                .any(|m| m.contains("DATABASE_URL") && m.contains("missing"))
        );

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 5: Edge-mode configuration does not require DATABASE_URL
#[sinex_test]
async fn test_phase5_configuration_allows_missing_database_url_in_edge_mode() -> TestResult<()> {
    with_database_url_absent_and_env_vars(
        &[
            ("SINEX_EDGE_MODE", "1".to_string()),
            ("RUST_LOG", "info".to_string()),
        ],
        || async {
            let (status, details, messages) =
                configuration::verify_configuration_generation().await?;

            assert_ne!(status, VerificationStatus::Fail);
            assert!(
                messages
                    .iter()
                    .any(|message| { message.contains("DATABASE_URL is intentionally optional") }),
                "expected edge-mode DATABASE_URL message, got {messages:#?}"
            );
            assert_eq!(
                details
                    .get("environment")
                    .and_then(|value| value.get("runtime_database_expected"))
                    .and_then(Value::as_bool),
                Some(false)
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 5: Configuration surfaces non-UTF8 required environment values
#[cfg(unix)]
#[sinex_test]
async fn test_phase5_configuration_rejects_non_unicode_required_env(
    ctx: TestContext,
) -> TestResult<()> {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous_database_url = env::var_os("DATABASE_URL");
    let previous_rust_log = env::var_os("RUST_LOG");
    unsafe { env::set_var("DATABASE_URL", ctx.database_url()) };
    unsafe { env::set_var("RUST_LOG", OsString::from_vec(vec![0x69, 0x6e, 0x80])) };

    let (status, _details, messages) = configuration::verify_configuration_generation().await?;

    unsafe {
        match previous_database_url {
            Some(value) => env::set_var("DATABASE_URL", value),
            None => env::remove_var("DATABASE_URL"),
        }
        match previous_rust_log {
            Some(value) => env::set_var("RUST_LOG", value),
            None => env::remove_var("RUST_LOG"),
        }
    }

    assert_eq!(status, VerificationStatus::Fail);
    assert!(
        messages
            .iter()
            .any(|message| message.contains("RUST_LOG") && message.contains("not valid UTF-8")),
        "expected invalid UTF-8 RUST_LOG message, got {messages:#?}"
    );

    Ok(())
}

/// Test Phase 5: Configuration format validation
#[sinex_test]
async fn test_phase5_config_format_validation() -> TestResult<()> {
    // Test JSON configuration format (since we don't have toml crate)
    let test_config = r#"{
  "database": {
    "url": "postgresql:///test",
    "pool_size": 10
  },
  "logging": {
    "level": "info"
  }
}"#;

    // Parse JSON to verify it's valid
    let parsed: serde_json::Value = serde_json::from_str(test_config)
        .map_err(|e| color_eyre::eyre::eyre!("Test JSON should be valid: {}", e))?;

    let parsed = parsed
        .as_object()
        .expect("Parsed configuration should be a JSON object");

    assert!(parsed.contains_key("database"));
    assert!(parsed.contains_key("logging"));

    Ok(())
}

/// Test Phase 5: Missing deployment descriptors refuse ambient source inference
#[sinex_test]
async fn test_phase5_configuration_event_sources_require_deployment_descriptor(
    ctx: TestContext,
) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("home");
    fs::create_dir_all(home.join(".local/share/atuin"))?;
    fs::create_dir_all(home.join(".local/share/activitywatch/aw-server-rust"))?;
    fs::write(home.join(".bash_history"), "echo hello\n")?;
    write_valid_atuin_history_db(&home.join(".local/share/atuin/history.db"))?;
    write_valid_activitywatch_db(
        &home.join(".local/share/activitywatch/aw-server-rust/sqlite.db"),
    )?;

    with_env_vars(
        &[
            ("DATABASE_URL", db_url),
            ("HOME", home.display().to_string()),
            ("SINEX_DEPLOYMENT_READINESS_CONFIG", String::new()),
        ],
        || async {
            let (status, details, messages) =
                configuration::verify_configuration_generation().await?;
            assert_eq!(status, VerificationStatus::Warning);
            assert!(
                messages
                    .iter()
                    .any(|message| message.contains("Deployment descriptor is missing")),
                "missing deployment descriptor should downgrade readiness honestly; messages={messages:#?}"
            );

            let sources = details
                .get("event_sources")
                .and_then(|value| value.get("sources"))
                .and_then(Value::as_object)
                .expect("event source details should be present");
            assert_eq!(
                details
                    .get("event_sources")
                    .and_then(|value| value.get("configured_unavailable_count"))
                    .and_then(Value::as_u64),
                Some(0)
            );

            assert_eq!(
                sources
                    .get("terminal")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("terminal")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("atuin")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("atuin")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("activitywatch")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("activitywatch")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(false)
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 5: Configured sources report missing paths honestly
#[sinex_test]
async fn test_phase5_configuration_event_sources_report_missing_configured_paths(
    ctx: TestContext,
) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    let temp = tempfile::tempdir()?;
    let configured_home = temp.path().join("configured-home");
    fs::create_dir_all(&configured_home)?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "source": "test",
            "terminal": {
                "enabled": true,
                "instances": 1,
                "kitty_enabled": false,
                "history_sources": [
                    {
                        "path": configured_home.join(".bash_history"),
                        "shell": "bash"
                    },
                    {
                        "path": configured_home.join(".local/share/atuin/history.db"),
                        "shell": "atuin"
                    },
                    {
                        "path": configured_home.join(".local/share/fish/fish_history"),
                        "shell": "fish"
                    }
                ]
            },
            "desktop": {
                "enabled": true,
                "instances": 1,
                "clipboard_enabled": false,
                "activitywatch_db_path": configured_home.join(".local/share/activitywatch/aw-server-rust/sqlite.db"),
                "runtime_dir": configured_home.join("runtime")
            }
        }))?,
    )?;

    with_env_vars(
        &[
            ("DATABASE_URL", db_url),
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
        ],
        || async {
            let (status, details, _messages) =
                configuration::verify_configuration_generation().await?;

            assert_eq!(status, VerificationStatus::Warning);
            assert_eq!(
                details
                    .get("event_sources")
                    .and_then(|value| value.get("configured_unavailable_count"))
                    .and_then(Value::as_u64),
                Some(4)
            );
            assert_eq!(
                details
                    .get("event_sources")
                    .and_then(|value| value.get("configured_unavailable_blocking_count"))
                    .and_then(Value::as_u64),
                Some(0)
            );
            assert_eq!(
                details
                    .get("event_sources")
                    .and_then(|value| value.get("configured_unavailable_advisory_count"))
                    .and_then(Value::as_u64),
                Some(4)
            );

            let sources = details
                .get("event_sources")
                .and_then(|value| value.get("sources"))
                .and_then(Value::as_object)
                .expect("event source details should be present");

            assert_eq!(
                sources
                    .get("terminal")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("terminal")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("terminal")
                    .and_then(|value| value.get("blocking"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("atuin")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("atuin")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("atuin")
                    .and_then(|value| value.get("blocking"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("activitywatch")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("activitywatch")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("activitywatch")
                    .and_then(|value| value.get("blocking"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("hyprland")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("hyprland")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                sources
                    .get("hyprland")
                    .and_then(|value| value.get("blocking"))
                    .and_then(Value::as_bool),
                Some(false)
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 5: Deployment descriptor overrides shell-home probing for configured sources
#[sinex_test]
async fn test_phase5_configuration_event_sources_follow_deployment_descriptor(
    ctx: TestContext,
) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    let temp = tempfile::tempdir()?;
    let empty_home = temp.path().join("home-empty");
    let configured_home = temp.path().join("configured-home");
    fs::create_dir_all(empty_home.join(".local/share"))?;
    fs::create_dir_all(configured_home.join(".local/share/atuin"))?;
    fs::create_dir_all(configured_home.join(".local/share/activitywatch/aw-server-rust"))?;
    fs::write(configured_home.join(".bash_history"), "echo hello\n")?;
    write_valid_atuin_history_db(&configured_home.join(".local/share/atuin/history.db"))?;
    write_valid_activitywatch_db(
        &configured_home.join(".local/share/activitywatch/aw-server-rust/sqlite.db"),
    )?;

    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "source": "test",
            "terminal": {
                "enabled": true,
                "instances": 1,
                "kitty_enabled": true,
                "history_sources": [
                    {
                        "path": configured_home.join(".bash_history"),
                        "shell": "bash"
                    },
                    {
                        "path": configured_home.join(".local/share/atuin/history.db"),
                        "shell": "atuin"
                    }
                ]
            },
            "desktop": {
                "enabled": true,
                "instances": 1,
                "clipboard_enabled": true,
                "activitywatch_db_path": configured_home.join(".local/share/activitywatch/aw-server-rust/sqlite.db")
            }
        }))?,
    )?;

    with_env_vars(
        &[
            ("DATABASE_URL", db_url),
            ("HOME", empty_home.display().to_string()),
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
        ],
        || async {
            let (_status, details, _messages) =
                configuration::verify_configuration_generation().await?;

            let sources = details
                .get("event_sources")
                .and_then(|value| value.get("sources"))
                .and_then(Value::as_object)
                .expect("event source details should be present");

            assert_eq!(
                sources
                    .get("terminal")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("terminal")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("atuin")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("atuin")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("activitywatch")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("activitywatch")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("kitty")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("kitty")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("clipboard")
                    .and_then(|value| value.get("configured"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                sources
                    .get("clipboard")
                    .and_then(|value| value.get("available"))
                    .and_then(Value::as_bool),
                Some(true)
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 5: Descriptor-declared Fish history is rejected unless it is SQLite-backed.
#[sinex_test]
async fn test_phase5_configuration_event_sources_reject_native_fish_history(
    ctx: TestContext,
) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    let temp = tempfile::tempdir()?;
    let configured_home = temp.path().join("configured-home");
    fs::create_dir_all(configured_home.join(".local/share/fish"))?;
    fs::write(
        configured_home.join(".local/share/fish/fish_history"),
        "- cmd: echo fish\n  when: 1234567890\n",
    )?;

    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "source": "test",
            "terminal": {
                "enabled": true,
                "instances": 1,
                "kitty_enabled": false,
                "history_sources": [
                    {
                        "path": configured_home.join(".local/share/fish/fish_history"),
                        "shell": "fish"
                    }
                ]
            }
        }))?,
    )?;

    with_env_vars(
        &[
            ("DATABASE_URL", db_url),
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
        ],
        || async {
            let (status, details, _messages) =
                configuration::verify_configuration_generation().await?;

            assert_eq!(status, VerificationStatus::Fail);
            assert_eq!(
                details
                    .get("event_sources")
                    .and_then(|value| value.get("configured_unavailable_blocking_count"))
                    .and_then(Value::as_u64),
                Some(1)
            );

            let terminal = details
                .get("event_sources")
                .and_then(|value| value.get("sources"))
                .and_then(|value| value.get("terminal"))
                .expect("terminal source details should be present");

            assert_eq!(
                terminal.get("configured").and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                terminal.get("available").and_then(Value::as_bool),
                Some(false)
            );
            assert_eq!(
                terminal.get("blocking").and_then(Value::as_bool),
                Some(true)
            );
            assert!(
                terminal.get("reason").and_then(Value::as_str).is_some_and(
                    |reason| reason.contains("native Fish YAML history is unsupported")
                )
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

// ====== PHASE 6: SERVICE DEPENDENCIES TESTS ======

/// Test Phase 6: Service dependencies verification
#[sinex_test]
async fn test_phase6_service_dependencies() -> TestResult<()> {
    let (_status, details, messages) = services::verify_service_dependencies().await?;

    assert!(!messages.is_empty());
    assert!(
        messages
            .iter()
            .any(|message| message.contains("Deployment descriptor is missing"))
    );

    let details = details.as_object().expect("details should be an object");
    if let Some(binaries) = details.get("binaries") {
        assert!(binaries.is_object());
    }
    if let Some(systemd) = details.get("systemd_services") {
        assert!(systemd.is_object());
    }

    Ok(())
}

#[sinex_test]
async fn test_phase6_service_dependencies_skip_postgres_in_edge_mode() -> TestResult<()> {
    with_env_vars(&[("SINEX_EDGE_MODE", "1".to_string())], || async {
        let (_status, details, messages) = services::verify_service_dependencies().await?;
        assert!(
            messages
                .iter()
                .any(|message| { message.contains("PostgreSQL service verification skipped") })
        );
        assert_eq!(
            details
                .get("postgresql")
                .and_then(|value| value.get("skipped"))
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    })
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_phase6_service_dependencies_fail_for_missing_declared_units() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "mode": "enabled",
            "source": "test",
            "managed_units": ["sinex-missing-test-unit.service"]
        }))?,
    )?;

    with_env_vars(
        &[(
            "SINEX_DEPLOYMENT_READINESS_CONFIG",
            descriptor_path.display().to_string(),
        )],
        || async {
            let (status, _details, messages) = services::verify_service_dependencies().await?;

            assert_eq!(status, VerificationStatus::Fail);
            assert!(messages.iter().any(|message| {
                message.contains("Declared managed units are missing or unloaded")
            }));
            Ok(())
        },
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_phase6_service_dependencies_fail_on_malformed_descriptor() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(&descriptor_path, "{ definitely not valid json")?;

    with_env_vars(
        &[(
            "SINEX_DEPLOYMENT_READINESS_CONFIG",
            descriptor_path.display().to_string(),
        )],
        || async {
            let error = services::verify_service_dependencies()
                .await
                .expect_err("malformed descriptor should abort service verification");
            assert!(
                error
                    .to_string()
                    .contains("failed to parse deployment readiness descriptor"),
                "expected malformed descriptor parse failure, got {error:?}"
            );
            Ok(())
        },
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_phase6_service_dependencies_fail_on_invalid_notify_contract() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "mode": "prepared",
            "source": "test",
            "managed_units": ["sinex-ingestd.service"]
        }))?,
    )?;

    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;

    let systemctl_path = bin_dir.join("systemctl");
    write_executable_script(
        &systemctl_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  printf 'systemd 255\n'
  exit 0
fi
if [ "$1" = "show" ]; then
  unit="$2"
  case "$unit" in
    sinex-ingestd.service)
      printf 'ActiveState=active\nSubState=running\nLoadState=loaded\nType=simple\nNotifyAccess=main\nWatchdogUSec=0\n'
      exit 0
      ;;
    postgresql.service|systemd-resolved.service)
      printf 'ActiveState=active\nSubState=running\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=60000000\n'
      exit 0
      ;;
  esac
fi
printf 'unexpected invocation: %s\n' "$*" >&2
exit 1
"#,
    )?;

    with_env_vars(
        &[
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
            ("SINEX_EDGE_MODE", "1".to_string()),
            ("PATH", bin_dir.display().to_string()),
        ],
        || async {
            let (status, _details, messages) = services::verify_service_dependencies().await?;
            assert_eq!(status, VerificationStatus::Fail);
            assert!(
                messages
                    .iter()
                    .any(|message| { message.contains("violates the notify/watchdog contract") })
            );
            Ok(())
        },
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_phase6_service_dependencies_accept_inactive_declared_unit_watchdog_placeholder(
) -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "mode": "prepared",
            "source": "test",
            "managed_units": ["sinex-ingestd.service"]
        }))?,
    )?;

    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;

    let systemctl_path = bin_dir.join("systemctl");
    write_executable_script(
        &systemctl_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  printf 'systemd 255\n'
  exit 0
fi
if [ "$1" = "show" ]; then
  unit="$2"
  case "$unit" in
    sinex-ingestd.service)
      printf 'ActiveState=inactive\nSubState=dead\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=infinity\n'
      exit 0
      ;;
    postgresql.service|systemd-resolved.service)
      printf 'ActiveState=active\nSubState=running\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=3min\n'
      exit 0
      ;;
  esac
fi
printf 'unexpected invocation: %s\n' "$*" >&2
exit 1
"#,
    )?;

    with_env_vars(
        &[
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
            ("SINEX_EDGE_MODE", "1".to_string()),
            ("PATH", bin_dir.display().to_string()),
        ],
        || async {
            let (status, _details, messages) = services::verify_service_dependencies().await?;
            assert_ne!(status, VerificationStatus::Fail);
            assert!(messages.iter().any(|message| {
                message.contains("watchdog will arm on start")
            }));
            Ok(())
        },
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_phase6_service_dependencies_descriptor_skips_path_service_binaries() -> TestResult<()>
{
    let temp = tempfile::tempdir()?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "mode": "prepared",
            "source": "test",
            "managed_units": []
        }))?,
    )?;

    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    let systemctl = which::which("systemctl")?;
    std::os::unix::fs::symlink(systemctl, bin_dir.join("systemctl"))?;

    with_env_vars(
        &[
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
            ("SINEX_EDGE_MODE", "1".to_string()),
            ("PATH", bin_dir.display().to_string()),
        ],
        || async {
            let (status, details, messages) = services::verify_service_dependencies().await?;
            assert_ne!(status, VerificationStatus::Fail);
            assert!(messages.iter().any(|message| {
                message.contains("skipping PATH-based Sinex service binary checks")
            }));
            let binaries = details
                .get("binaries")
                .and_then(|value| value.get("binaries"))
                .and_then(Value::as_object)
                .expect("binaries map should be present");
            assert!(!binaries.contains_key("sinex-ingestd"));
            assert!(!binaries.contains_key("sinex-gateway"));
            assert!(!binaries.contains_key("sinex-preflight"));
            Ok(())
        },
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_phase6_service_dependencies_reports_optional_binary_probe_errors() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let descriptor_path = temp.path().join("deployment-readiness.json");
    fs::write(
        &descriptor_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "mode": "prepared",
            "source": "test",
            "managed_units": []
        }))?,
    )?;

    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    let systemctl = which::which("systemctl")?;
    std::os::unix::fs::symlink(systemctl, bin_dir.join("systemctl"))?;

    with_env_vars(
        &[
            (
                "SINEX_DEPLOYMENT_READINESS_CONFIG",
                descriptor_path.display().to_string(),
            ),
            ("SINEX_EDGE_MODE", "1".to_string()),
            ("PATH", bin_dir.display().to_string()),
        ],
        || async {
            let (_status, details, messages) = services::verify_service_dependencies().await?;
            let binaries = details
                .get("binaries")
                .and_then(|value| value.get("binaries"))
                .and_then(Value::as_object)
                .expect("binaries map should be present");
            let git = binaries
                .get("git")
                .and_then(Value::as_object)
                .expect("git probe should be present");

            assert_eq!(git.get("available").and_then(Value::as_bool), Some(false));
            assert!(
                git.get("error")
                    .and_then(Value::as_str)
                    .is_some_and(|error| error.contains("Binary 'git' not found in PATH"))
            );
            assert!(
                messages
                    .iter()
                    .any(|message| { message.contains("Optional binary 'git' unavailable") })
            );
            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 6: Binary availability verification
#[sinex_test]
async fn test_phase6_binary_availability() -> TestResult<()> {
    // Test with a binary that should exist (ls)
    let output = std::process::Command::new("which")
        .arg("ls")
        .output()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to run which command: {}", e))?;

    assert!(output.status.success(), "'ls' command should be available");

    // Test with a binary that shouldn't exist
    let output = std::process::Command::new("which")
        .arg("nonexistent_binary_12345")
        .output()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to run which command: {}", e))?;

    assert!(
        !output.status.success(),
        "Nonexistent binary should not be found"
    );

    Ok(())
}

// ====== PHASE 7: INTEGRATION TESTS ======

/// Test Phase 7: End-to-end integration verification
#[sinex_test]
async fn test_phase7_integration_success(ctx: TestContext) -> TestResult<()> {
    let db_url = ctx.database_url().to_string();
    let ctx = ctx.with_nats().shared().await?;
    let nats_url = ctx
        .nats_url()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected test NATS URL"))?;
    let js: jetstream::Context = ctx.jetstream().await?;
    let database_name = db_url
        .split('?')
        .next()
        .and_then(|url| url.rsplit('/').next())
        .ok_or_else(|| color_eyre::eyre::eyre!("expected database name in test URL"))?;
    let env_name = database_name
        .rsplit_once('_')
        .map(|(_, suffix)| suffix.to_string())
        .ok_or_else(|| color_eyre::eyre::eyre!("expected suffixed database name in test URL"))?;
    let env = SinexEnvironment::new(&env_name)?;
    let _environment_guard =
        sinex_primitives::environment::override_environment_for_tests(&env_name)?;
    let expected_checkpoint_bucket = ensure_preflight_streams(&js, &env).await?;

    with_env_vars(
        &[
            ("DATABASE_URL", db_url),
            ("SINEX_NATS_URL", nats_url),
            ("SINEX_ENVIRONMENT", env_name),
        ],
        || async {
            let (status, details, messages) = verification::verify_end_to_end_integration().await?;

            assert_eq!(
                status,
                VerificationStatus::Pass,
                "unexpected integration status; details={details:#?}; messages={messages:#?}"
            );
            assert!(!messages.is_empty());

            let details = details.as_object().expect("details should be an object");
            let integration = details
                .get("integration_tests")
                .and_then(Value::as_object)
                .expect("integration tests should be present");
            assert!(integration.contains_key("database_integration"));
            let service = integration
                .get("service_integration")
                .and_then(Value::as_object)
                .expect("service integration should be present");
            assert_eq!(
                service.get("checkpoint_bucket").and_then(Value::as_str),
                Some(expected_checkpoint_bucket.as_str())
            );
            assert!(
                service
                    .get("required_streams")
                    .and_then(Value::as_array)
                    .is_some_and(|streams| streams.len() == 5)
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

/// Test Phase 7: Integration with database connection failure
#[sinex_test]
async fn test_phase7_integration_db_failure() -> TestResult<()> {
    with_database_url("postgresql://invalid:5432/nonexistent", || async {
        let (status, _details, messages) = verification::verify_end_to_end_integration().await?;

        assert_eq!(status, VerificationStatus::Fail);
        assert!(
            messages
                .iter()
                .any(|m| m.contains("Database integration test failed"))
        );

        Ok(())
    })
    .await?;

    Ok(())
}

/// Test Phase 7: Edge-mode integration skips database verification
#[sinex_test]
async fn test_phase7_integration_skips_database_in_edge_mode(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_url = ctx
        .nats_url()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected test NATS URL"))?;
    let js: jetstream::Context = ctx.jetstream().await?;
    let env_name = "edge".to_string();
    let env = SinexEnvironment::new(&env_name)?;
    let _environment_guard =
        sinex_primitives::environment::override_environment_for_tests(&env_name)?;
    let expected_checkpoint_bucket = ensure_preflight_streams(&js, &env).await?;

    with_database_url_absent_and_env_vars(
        &[
            ("SINEX_EDGE_MODE", "1".to_string()),
            ("SINEX_NATS_URL", nats_url),
            ("SINEX_ENVIRONMENT", env_name),
            ("RUST_LOG", "info".to_string()),
        ],
        || async {
            let (status, details, messages) = verification::verify_end_to_end_integration().await?;

            assert_eq!(status, VerificationStatus::Pass);
            assert!(
                messages
                    .iter()
                    .any(|message| { message.contains("Database integration skipped") }),
                "expected database skip message, got {messages:#?}"
            );
            assert_eq!(
                details
                    .get("integration_tests")
                    .and_then(|value| value.get("database_integration"))
                    .and_then(|value| value.get("skipped"))
                    .and_then(Value::as_bool),
                Some(true)
            );
            assert_eq!(
                details
                    .get("integration_tests")
                    .and_then(|value| value.get("service_integration"))
                    .and_then(|value| value.get("checkpoint_bucket"))
                    .and_then(Value::as_str),
                Some(expected_checkpoint_bucket.as_str())
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_phase7_integration_warns_when_checkpoint_bucket_missing(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_url = ctx
        .nats_url()
        .ok_or_else(|| color_eyre::eyre::eyre!("expected test NATS URL"))?;
    let js: jetstream::Context = ctx.jetstream().await?;
    let env_name = "edge".to_string();
    let env = SinexEnvironment::new(&env_name)?;
    let _environment_guard =
        sinex_primitives::environment::override_environment_for_tests(&env_name)?;
    ensure_preflight_event_streams_only(&js, &env).await?;

    with_database_url_absent_and_env_vars(
        &[
            ("SINEX_EDGE_MODE", "1".to_string()),
            ("SINEX_NATS_URL", nats_url),
            ("SINEX_ENVIRONMENT", env_name),
        ],
        || async {
            let (status, _details, messages) =
                verification::verify_end_to_end_integration().await?;

            assert_eq!(status, VerificationStatus::Warning);
            assert!(
                messages
                    .iter()
                    .any(|message| message.contains("checkpoint KV bucket")),
                "expected checkpoint bucket failure in messages, got {messages:#?}"
            );

            Ok(())
        },
    )
    .await?;

    Ok(())
}

// ====== UTILITY AND HELPER TESTS ======

/// Test verification status basic properties
#[sinex_test]
async fn test_verification_status_properties() -> TestResult<()> {
    // Test that VerificationStatus enum works correctly
    let statuses = vec![
        VerificationStatus::Pass,
        VerificationStatus::Warning,
        VerificationStatus::Fail,
    ];

    for status in statuses {
        // Test equality and cloning
        let cloned_status = status;
        assert_eq!(status, cloned_status);

        // Test debug formatting
        let debug_str = format!("{status:?}");
        assert!(!debug_str.is_empty());
    }

    Ok(())
}

/// Test error message formatting and context
#[sinex_test]
async fn test_error_message_formatting() -> TestResult<()> {
    // Test various error scenarios and verify message formatting
    let test_cases = vec![
        ("✓ Success message format", true),
        ("✗ Failure message format", false),
        ("⚠ Warning message format", false),
        ("ℹ Info message format", false),
    ];

    for (message, is_success) in test_cases {
        if is_success {
            assert!(
                message.starts_with("✓"),
                "Success messages should start with ✓"
            );
        } else {
            assert!(
                message.starts_with("✗") || message.starts_with("⚠") || message.starts_with("ℹ"),
                "Non-success messages should start with appropriate symbol"
            );
        }
    }

    Ok(())
}

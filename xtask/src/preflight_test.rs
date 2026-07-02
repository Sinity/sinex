use super::*;
use crate::sandbox::{EnvGuard, sinex_test};
use std::os::unix::process::ExitStatusExt;
use tempfile::tempdir;

#[sinex_test]
async fn test_infra_status_capture() -> TestResult<()> {
    // This test just verifies the capture doesn't panic
    let status = InfraStatus::capture();
    // The actual values depend on the environment
    let _ = status.all_ready();
    let _ = status.stack_running();
    Ok(())
}

#[sinex_test]
async fn test_write_state_file_atomically_creates_parent_dirs() -> TestResult<()> {
    let dir = tempdir()?;
    let path = dir.path().join("nested").join("state.json");

    write_state_file_atomically(&path, "{\"ok\":true}")?;

    assert_eq!(std::fs::read_to_string(&path)?, "{\"ok\":true}");
    Ok(())
}

#[sinex_test]
async fn test_write_state_file_atomically_replaces_existing_contents() -> TestResult<()> {
    let dir = tempdir()?;
    let path = dir.path().join("state.json");
    std::fs::write(&path, "old")?;

    write_state_file_atomically(&path, "new")?;

    assert_eq!(std::fs::read_to_string(&path)?, "new");
    Ok(())
}

#[sinex_test]
async fn test_open_schema_apply_lock_file_creates_state_dir() -> TestResult<()> {
    let dir = tempdir()?;
    let state_dir = dir.path().join("nested").join("preflight");

    let lock_file = open_schema_apply_lock_file(&state_dir)?;
    drop(lock_file);

    assert!(state_dir.is_dir());
    assert!(schema_apply_lock_path(&state_dir).is_file());
    Ok(())
}

#[sinex_test]
async fn test_state_dir_uses_configured_state_dir_preflight_child() -> TestResult<()> {
    let dir = tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_STATE_DIR"]);
    env.set("SINEX_STATE_DIR", dir.path());

    assert_eq!(state_dir(), dir.path().join("preflight"));
    assert_eq!(
        cache_path(),
        dir.path().join("preflight/preflight-cache.json")
    );
    Ok(())
}

#[sinex_test]
async fn test_open_schema_apply_lock_file_surfaces_state_dir_creation_failure() -> TestResult<()> {
    let dir = tempdir()?;
    let blocking_file = dir.path().join("not-a-directory");
    std::fs::write(&blocking_file, "occupied")?;

    let error =
        open_schema_apply_lock_file(&blocking_file).expect_err("state-dir collision must surface");
    let rendered = format!("{error:#}");
    assert!(rendered.contains("failed to create preflight state dir"));
    assert!(rendered.contains(&blocking_file.display().to_string()));
    Ok(())
}

#[sinex_test]
async fn test_write_state_file_atomically_reports_parent_creation_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let blocking_path = dir.path().join("not-a-dir");
    std::fs::write(&blocking_path, "blocker")?;
    let path = blocking_path.join("state.json");

    let error = write_state_file_atomically(&path, "value").unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("failed to create preflight state directory"));
    Ok(())
}

#[sinex_test]
async fn test_unix_timestamp_secs_rejects_pre_epoch_clock() -> TestResult<()> {
    let before_epoch = UNIX_EPOCH
        .checked_sub(std::time::Duration::from_secs(1))
        .expect("pre-epoch timestamp");
    let error = unix_timestamp_secs(before_epoch, "test clock").unwrap_err();
    assert!(format!("{error:#}").contains("test clock: system clock is before the unix epoch"));
    Ok(())
}

#[sinex_test]
async fn test_check_contract_tables_ready_reports_probe_failures() -> TestResult<()> {
    let error = check_contract_tables_ready(Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "psql missing",
    )))
    .unwrap_err();
    assert!(format!("{error:#}").contains("psql missing"));

    let error = check_contract_tables_ready(Ok(std::process::Output {
        status: std::process::ExitStatus::from_raw(1 << 8),
        stdout: Vec::new(),
        stderr: b"permission denied".to_vec(),
    }))
    .unwrap_err();
    assert!(format!("{error:#}").contains("permission denied"));
    Ok(())
}

#[sinex_test]
async fn test_hash_contracts_dir_from_returns_empty_for_missing_directory() -> TestResult<()> {
    let dir = tempdir()?;
    let missing = dir.path().join("missing-payloads");
    assert_eq!(hash_contracts_dir_from(&missing)?, "empty");
    Ok(())
}

#[sinex_test]
async fn test_hash_contracts_dir_from_hashes_rust_sources_only() -> TestResult<()> {
    let dir = tempdir()?;
    std::fs::write(dir.path().join("alpha.rs"), "pub struct Alpha;")?;
    std::fs::write(dir.path().join("beta.txt"), "ignored")?;

    let hash = hash_contracts_dir_from(dir.path())?;
    std::fs::write(dir.path().join("beta.txt"), "ignored differently")?;
    let hash_after_non_rust_change = hash_contracts_dir_from(dir.path())?;

    assert_ne!(hash, "empty");
    assert_eq!(hash, hash_after_non_rust_change);
    Ok(())
}

#[sinex_test]
async fn test_hash_contracts_dir_from_recurses_into_subdirectories() -> TestResult<()> {
    let dir = tempdir()?;
    let subdir = dir.path().join("nested");
    std::fs::create_dir_all(&subdir)?;
    std::fs::write(dir.path().join("top.rs"), "pub struct Top;")?;
    std::fs::write(subdir.join("inner.rs"), "pub struct Inner;")?;

    let hash = hash_contracts_dir_from(dir.path())?;
    assert_ne!(hash, "empty");

    // Changing the nested file must change the hash.
    std::fs::write(subdir.join("inner.rs"), "pub struct InnerChanged;")?;
    let hash_after_nested_change = hash_contracts_dir_from(dir.path())?;
    assert_ne!(hash, hash_after_nested_change);
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_hash_contracts_dir_from_rejects_non_utf8_source_names() -> TestResult<()> {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let dir = tempdir()?;
    let invalid_name =
        OsString::from_vec(vec![b'a', b'l', b'p', b'h', b'a', 0xff, b'.', b'r', b's']);
    std::fs::write(dir.path().join(invalid_name), "pub struct Alpha;")?;

    let error = hash_contracts_dir_from(dir.path()).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("not valid UTF-8"));
    assert!(message.contains(&dir.path().display().to_string()));
    Ok(())
}

#[sinex_test]
async fn test_ensure_compiled_contracts_inventory_current_accepts_matching_hash() -> TestResult<()>
{
    ensure_compiled_contracts_inventory_current("deadbeefcafebabe", "deadbeefcafebabe")?;
    Ok(())
}

#[sinex_test]
async fn test_ensure_compiled_contracts_inventory_current_rejects_stale_hash() -> TestResult<()> {
    let error = ensure_compiled_contracts_inventory_current("deadbeefcafebabe", "feedface00000000")
        .unwrap_err();
    assert!(format!("{error:#}").contains("stale event payload inventory"));
    Ok(())
}

#[sinex_test]
async fn test_ensure_compiled_contracts_inventory_current_rejects_missing_hash() -> TestResult<()> {
    let error =
        ensure_compiled_contracts_inventory_current("deadbeefcafebabe", "unknown").unwrap_err();
    assert!(
        format!("{error:#}").contains("does not carry a compiled event payload inventory hash")
    );
    Ok(())
}

#[sinex_test]
async fn test_pending_cache_blockers_reports_unconverged_setup() -> TestResult<()> {
    assert_eq!(
        pending_cache_blockers(false, false),
        Vec::<&'static str>::new()
    );
    assert_eq!(
        pending_cache_blockers(true, false),
        vec!["schema apply still pending"]
    );
    assert_eq!(
        pending_cache_blockers(false, true),
        vec!["contracts deployment still pending"]
    );
    assert_eq!(
        pending_cache_blockers(true, true),
        vec![
            "schema apply still pending",
            "contracts deployment still pending"
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_load_preflight_cache_from_reports_malformed_json() -> TestResult<()> {
    let dir = tempdir()?;
    let path = dir.path().join("preflight-cache.json");
    std::fs::write(&path, "{not json")?;

    let error = load_preflight_cache_from(&path).unwrap_err();
    assert!(format!("{error:#}").contains("failed to parse preflight cache"));
    Ok(())
}

#[sinex_test]
async fn test_parse_schema_apply_probe_output_reports_invalid_output() -> TestResult<()> {
    let error = parse_schema_apply_probe_output(&std::process::Output {
        status: std::process::ExitStatus::from_raw(0),
        stdout: b"wat".to_vec(),
        stderr: Vec::new(),
    })
    .unwrap_err();
    assert!(format!("{error:#}").contains("schema readiness probe returned invalid output"));
    Ok(())
}

#[sinex_test]
async fn test_parse_schema_apply_probe_output_accepts_statement_timeout_prefix() -> TestResult<()> {
    let pending = parse_schema_apply_probe_output(&std::process::Output {
        status: std::process::ExitStatus::from_raw(0),
        stdout: b"SET\n0\n".to_vec(),
        stderr: Vec::new(),
    })?;

    assert!(!pending);
    Ok(())
}

#[sinex_test]
async fn test_infra_status_all_ready_requires_tls() -> TestResult<()> {
    assert!(
        InfraStatus {
            postgres: true,
            nats: true,
            tls: true,
            schema_apply_pending: false,
        }
        .all_ready()
    );

    assert!(
        !InfraStatus {
            postgres: true,
            nats: true,
            tls: false,
            schema_apply_pending: false,
        }
        .all_ready()
    );
    Ok(())
}

#[sinex_test]
async fn test_wait_for_schema_apply_completion_returns_when_pending_clears() -> TestResult<()> {
    let mut pending = vec![true, true, false].into_iter();

    wait_for_schema_apply_completion_with(
        std::time::Duration::from_millis(10),
        std::time::Duration::ZERO,
        || Ok(pending.next().unwrap_or(false)),
    )?;
    Ok(())
}

#[sinex_test]
async fn test_wait_for_schema_apply_completion_times_out_if_pending_never_clears() -> TestResult<()>
{
    let error = wait_for_schema_apply_completion_with(
        std::time::Duration::from_millis(1),
        std::time::Duration::ZERO,
        || Ok(true),
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("schema apply is still pending"));
    Ok(())
}

#[sinex_test]
async fn test_schema_readiness_probe_sql_sets_statement_timeout() -> TestResult<()> {
    assert!(SCHEMA_READINESS_PROBE_SQL.contains("SET statement_timeout = '5s'"));
    assert!(SCHEMA_READINESS_PROBE_SQL.contains("to_regclass('core.events')"));
    Ok(())
}

#[sinex_test]
async fn test_read_optional_state_file_reports_non_not_found_errors() -> TestResult<()> {
    let dir = tempdir()?;
    let error = read_optional_state_file(dir.path(), "state file").unwrap_err();
    assert!(format!("{error:#}").contains("failed to read state file file"));
    Ok(())
}

#[sinex_test]
async fn test_auto_start_stack_uses_default_for_invalid_timeout_override() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set("SINEX_INFRA_START_TIMEOUT", "bogus");
    assert_eq!(
        crate::parse_positive_u64_env_or_default(
            "SINEX_INFRA_START_TIMEOUT",
            120,
            "infra start timeout"
        ),
        120
    );
    Ok(())
}

#[sinex_test]
async fn test_tls_dir_ready_requires_server_key() -> TestResult<()> {
    let dir = tempdir()?;
    std::fs::write(dir.path().join("ca.pem"), "ca")?;
    std::fs::write(dir.path().join("server.pem"), "server")?;
    std::fs::write(dir.path().join("client.pem"), "client")?;

    assert!(!tls_dir_ready(dir.path()));

    std::fs::write(dir.path().join("server-key.pem"), "server-key")?;

    assert!(tls_dir_ready(dir.path()));
    Ok(())
}

#[sinex_test]
async fn test_ensure_ready_uses_default_for_invalid_ttl_override() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set("SINEX_PREFLIGHT_TTL_SECS", "0");
    assert_eq!(
        crate::parse_positive_u64_env_or_default(
            "SINEX_PREFLIGHT_TTL_SECS",
            PREFLIGHT_CACHE_DEFAULT_TTL_SECS,
            "preflight cache ttl"
        ),
        PREFLIGHT_CACHE_DEFAULT_TTL_SECS
    );
    Ok(())
}

#[sinex_test]
async fn test_check_required_tools_with_accepts_healthy_tools() -> TestResult<()> {
    check_required_tools_with(&["pg_isready", "psql"], |_tool| {
        Ok(ToolInfo {
            path: "/nix/store/fake-tool".into(),
            version: "1.0.0".to_string(),
            probe_issue: None,
        })
    })?;
    Ok(())
}

#[sinex_test]
async fn test_check_required_tools_with_surfaces_missing_and_broken_tools() -> TestResult<()> {
    let error = check_required_tools_with(&["pg_isready", "psql", "createdb"], |tool| match tool {
        "pg_isready" => Ok(ToolInfo {
            path: "/nix/store/pg_isready".into(),
            version: "pg_isready 16".to_string(),
            probe_issue: None,
        }),
        "psql" => Err(eyre!("Tool 'psql' not found in PATH")),
        "createdb" => Ok(ToolInfo {
            path: "/nix/store/createdb".into(),
            version: "unknown".to_string(),
            probe_issue: Some("Failed to run 'createdb --version'".to_string()),
        }),
        _ => unreachable!(),
    })
    .unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("psql"));
    assert!(message.contains("not found in PATH"));
    assert!(message.contains("createdb"));
    assert!(message.contains("createdb --version"));
    Ok(())
}

#[sinex_test]
async fn test_set_dev_token_if_missing_adds_admin_role_suffix() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set_optional("SINEX_ENVIRONMENT", None);
    _guard.set_optional("SINEX_API_TOKEN", None);
    _guard.set_optional("SINEX_API_TOKEN_FILE", None);
    _guard.set_optional("SINEX_API_ADMIN_TOKEN_FILE", None);

    set_dev_token_if_missing();

    let token = std::env::var("SINEX_API_TOKEN")?;
    assert!(token.starts_with("dev-token-"));
    assert!(token.ends_with(":admin"));
    Ok(())
}

#[sinex_test]
async fn test_local_runtime_env_overrides_include_dev_token_and_tls_defaults() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set_optional("SINEX_ENVIRONMENT", None);
    _guard.set_optional("SINEX_API_TOKEN", None);
    _guard.set_optional("SINEX_API_TOKEN_FILE", None);
    _guard.set_optional("SINEX_API_ADMIN_TOKEN_FILE", None);
    _guard.set_optional("SINEX_API_TLS_CERT", None);
    _guard.set_optional("SINEX_API_TLS_KEY", None);

    let overrides = local_runtime_env_overrides();

    assert!(overrides.iter().any(|(key, value)| {
        key == "SINEX_API_TOKEN" && value.starts_with("dev-token-") && value.ends_with(":admin")
    }));
    assert!(
        overrides
            .iter()
            .any(|(key, value)| { key == "SINEX_API_TLS_CERT" && value.ends_with("server.pem") })
    );
    assert!(
        overrides.iter().any(|(key, value)| {
            key == "SINEX_API_TLS_KEY" && value.ends_with("server-key.pem")
        })
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_spawn_process_group_leader_creates_dedicated_group() -> TestResult<()> {
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "sleep 30"]);
    let mut child = spawn_process_group_leader(&mut command)?;
    let pid = nix::unistd::Pid::from_raw(child.id() as i32);
    let process_group = nix::unistd::getpgid(Some(pid))?;

    assert_eq!(process_group, pid);

    terminate_child_process_tree(&mut child)?;
    Ok(())
}

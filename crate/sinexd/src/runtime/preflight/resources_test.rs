use super::{
    configured_hostname_resolution_probe, resolution_target_host, verify_disk_space,
    verify_filesystem_permissions,
};
use serde_json::Value;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use tempfile::{TempDir, tempdir, tempdir_in};
use xtask::sandbox::{EnvGuard, sinex_test};

fn spacious_tempdir() -> ::xtask::sandbox::TestResult<TempDir> {
    let root = std::env::current_dir()?.join(".sinex/test-preflight-resources");
    fs::create_dir_all(&root)?;
    Ok(tempdir_in(root)?)
}

#[sinex_test]
async fn resolution_target_host_skips_local_and_socket_targets()
-> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        resolution_target_host("postgresql://db.example/sinex"),
        Ok(Some("db.example".to_string()))
    );
    assert_eq!(
        resolution_target_host("nats://nats.example:4222"),
        Ok(Some("nats.example".to_string()))
    );
    assert_eq!(
        resolution_target_host("127.0.0.1:4222"),
        Ok(None),
        "loopback-only endpoints should not be reported as hostname resolution targets"
    );
    assert_eq!(
        resolution_target_host("postgresql:///sinex?host=/tmp"),
        Ok(None),
        "unix-socket URLs should not be treated as DNS targets"
    );
    Ok(())
}

#[sinex_test]
async fn resolution_target_host_rejects_invalid_configured_target()
-> ::xtask::sandbox::TestResult<()> {
    let error = resolution_target_host("db.example")
        .expect_err("bare hostname should surface as invalid configured endpoint");
    assert!(error.contains("not a URL or host:port target"));
    Ok(())
}

#[sinex_test]
async fn configured_hostname_resolution_targets_deduplicate_hosts()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://db.example/sinex");
    env.set("SINEX_NATS_URL", "nats://db.example:4222");
    env.set("SINEX_API_URL", "https://gateway.example/rpc");

    let targets = configured_hostname_resolution_probe().hosts;
    assert_eq!(
        targets,
        vec!["db.example".to_string(), "gateway.example".to_string()]
    );
    Ok(())
}

#[sinex_test]
async fn configured_hostname_resolution_probe_collects_invalid_inputs()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "db.example");
    env.set("SINEX_NATS_URL", "nats://nats.example:4222");

    let probe = configured_hostname_resolution_probe();
    assert_eq!(probe.hosts, vec!["nats.example".to_string()]);
    assert_eq!(probe.invalid_inputs.len(), 1);
    assert_eq!(probe.invalid_inputs[0].env_name, "DATABASE_URL");
    assert_eq!(probe.invalid_inputs[0].raw, "db.example");
    assert!(
        probe.invalid_inputs[0]
            .error
            .contains("not a URL or host:port target")
    );
    Ok(())
}

#[sinex_test]
async fn verify_disk_space_accepts_missing_paths_when_parent_filesystem_exists()
-> ::xtask::sandbox::TestResult<()> {
    let root = spacious_tempdir()?;
    let state_dir = root.path().join("state");
    let data_dir = root.path().join("data");
    let log_dir = root.path().join("logs");
    let tmp_dir = root.path().join("tmp");
    let missing_work_dir = root.path().join("work-missing");

    for dir in [&state_dir, &data_dir, &log_dir, &tmp_dir] {
        fs::create_dir_all(dir)?;
    }

    let mut env = EnvGuard::new();
    env.set("SINEX_STATE_DIR", state_dir.display().to_string());
    env.set("SINEX_DATA_DIR", data_dir.display().to_string());
    env.set("SINEX_LOG_DIR", log_dir.display().to_string());
    env.set("TMPDIR", tmp_dir.display().to_string());
    env.set("SINEX_WORK_DIR", missing_work_dir.display().to_string());

    let mut messages = Vec::new();
    let disk_info = verify_disk_space(&mut messages)?;
    let missing_work_dir_str = missing_work_dir.display().to_string();
    assert!(
        disk_info["paths"][missing_work_dir_str.as_str()]["meets_requirements"]
            .as_bool()
            .unwrap_or(false),
        "missing work dir should reuse the nearest existing parent filesystem: {disk_info:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn verify_filesystem_permissions_accepts_missing_paths_when_parent_is_creatable()
-> ::xtask::sandbox::TestResult<()> {
    let root = tempdir()?;
    let state_dir = root.path().join("state");
    let data_dir = root.path().join("data");
    let log_dir = root.path().join("logs");
    let tmp_dir = root.path().join("tmp");
    let missing_work_dir = root.path().join("work-missing");

    for dir in [&state_dir, &data_dir, &log_dir, &tmp_dir] {
        fs::create_dir_all(dir)?;
    }

    let mut env = EnvGuard::new();
    env.set("SINEX_STATE_DIR", state_dir.display().to_string());
    env.set("SINEX_DATA_DIR", data_dir.display().to_string());
    env.set("SINEX_LOG_DIR", log_dir.display().to_string());
    env.set("TMPDIR", tmp_dir.display().to_string());
    env.set("SINEX_WORK_DIR", missing_work_dir.display().to_string());

    let mut messages = Vec::new();
    let fs_info = verify_filesystem_permissions(&mut messages).await?;
    let missing_work_dir_str = missing_work_dir.display().to_string();

    assert!(
        fs_info
            .get("meets_requirements")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        "missing work dir should be treated as creatable when its parent is writable"
    );
    assert!(
        fs_info["directories"][missing_work_dir_str.as_str()]["creatable"]
            .as_bool()
            .unwrap_or(false),
        "missing work dir should be marked creatable: {fs_info:#?}"
    );
    assert!(
        messages.iter().any(|message| {
            message.contains("can be created") && message.contains(&missing_work_dir_str)
        }),
        "filesystem probe should report the missing path as creatable: {messages:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn verify_filesystem_permissions_rejects_missing_paths_when_parent_is_not_writable()
-> ::xtask::sandbox::TestResult<()> {
    let root = tempdir()?;
    let state_dir = root.path().join("state");
    let data_dir = root.path().join("data");
    let log_dir = root.path().join("logs");
    let tmp_dir = root.path().join("tmp");
    let locked_parent = root.path().join("locked");
    let missing_work_dir = locked_parent.join("work-missing");

    for dir in [&state_dir, &data_dir, &log_dir, &tmp_dir, &locked_parent] {
        fs::create_dir_all(dir)?;
    }
    fs::set_permissions(&locked_parent, fs::Permissions::from_mode(0o555))?;

    let mut env = EnvGuard::new();
    env.set("SINEX_STATE_DIR", state_dir.display().to_string());
    env.set("SINEX_DATA_DIR", data_dir.display().to_string());
    env.set("SINEX_LOG_DIR", log_dir.display().to_string());
    env.set("TMPDIR", tmp_dir.display().to_string());
    env.set("SINEX_WORK_DIR", missing_work_dir.display().to_string());

    let mut messages = Vec::new();
    let fs_info = verify_filesystem_permissions(&mut messages).await?;
    let missing_work_dir_str = missing_work_dir.display().to_string();

    assert!(
        !fs_info
            .get("meets_requirements")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        "missing work dir should fail when its nearest existing parent is not writable"
    );
    assert!(
        messages.iter().any(|message| {
            message.contains("cannot be created") && message.contains(&missing_work_dir_str)
        }),
        "filesystem probe should report the missing path as non-creatable: {messages:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn configured_hostname_resolution_probe_handles_comma_separated_nats_urls()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(
        "SINEX_NATS_URL",
        "nats://nats1.example:4222,nats://nats2.example:4222",
    );

    let targets = configured_hostname_resolution_probe().hosts;
    assert!(
        targets.contains(&"nats1.example".to_string()),
        "first NATS host should be probed: {targets:?}"
    );
    assert!(
        targets.contains(&"nats2.example".to_string()),
        "second NATS host should be probed: {targets:?}"
    );
    Ok(())
}

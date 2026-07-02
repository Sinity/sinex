use super::{
    AllCheckoutsCleanup, AllCheckoutsStatus, CleanupActionKind, GIT_REPOSITORY_ENV_KEYS,
    collect_snapshot_names, dir_size, discover_nats_port, git_subprocess, list_snapshots,
    parse_cmdline_bytes, parse_proc_stat_ppid, probe_annex_available,
    require_successful_command, service_pid_state, stop_dev_sinexd_pid,
    sync_event_payload_schemas_for_database_url,
};
use super::{StackConfig, StackStatus};
use crate::infra::state::{CheckoutInventoryRoot, LockInfo, LockInspection};
use crate::sandbox::prelude::*;
use sinex_primitives::temporal::Timestamp;
use std::ffi::OsString;
use std::fs;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::Command as StdCommand;
use std::time::{Duration, Instant};

#[sinex_test]
async fn nats_port_matches_flake_hash_for_sinex_checkout() -> ::xtask::sandbox::TestResult<()> {
    let checkout = Path::new("/realm/project/sinex");
    assert_eq!(StackConfig::port_offset_for_checkout(checkout), 86);
    assert_eq!(StackConfig::nats_port_for_checkout(checkout), 4308);
    Ok(())
}

#[sinex_test]
async fn discover_nats_port_reads_generated_config() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let config_dir = temp.path().join("config/nats");
    fs::create_dir_all(&config_dir)?;
    fs::write(
        config_dir.join("nats.conf"),
        r#"
host = "127.0.0.1"
port = 4310
"#,
    )?;

    assert_eq!(discover_nats_port(temp.path()), Some(4310));
    Ok(())
}

#[sinex_test]
async fn service_pid_state_classifies_stale_pid_files() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let pid_file = temp.path().join("service.pid");
    fs::write(&pid_file, "999999999\n")?;

    assert_eq!(service_pid_state(&pid_file), super::ServicePidState::Stale);
    Ok(())
}

#[sinex_test]
async fn all_checkouts_status_totals_stale_pid_files_and_sizes() -> Result<()> {
    let base = tempfile::tempdir()?;
    let cache_root = base.path().join("hash123");
    let dev_state = cache_root.join("dev-state");
    fs::create_dir_all(dev_state.join("data/postgres"))?;
    fs::create_dir_all(dev_state.join("run"))?;
    fs::write(
        dev_state.join("data/postgres/postmaster.pid"),
        "999999999\n",
    )?;
    fs::write(dev_state.join("run/nats.pid"), "999999998\n")?;
    fs::write(dev_state.join("run/example.log"), "hello")?;

    let status = AllCheckoutsStatus::gather(
        base.path().to_path_buf(),
        vec![CheckoutInventoryRoot {
            cache_root,
            dev_state_dir: dev_state,
            checkout_path: None,
            lock: LockInspection::Missing,
        }],
    );

    assert_eq!(status.totals.checkout_count, 1);
    assert_eq!(status.totals.stale_postgres_pid_files, 1);
    assert_eq!(status.totals.stale_nats_pid_files, 1);
    assert!(status.totals.state_bytes >= 5);
    assert_eq!(
        status.checkouts[0].postgres.pid_state,
        super::ServicePidState::Stale
    );
    assert_eq!(
        status.checkouts[0].nats.pid_state,
        super::ServicePidState::Stale
    );
    assert!(!status.checkouts[0].remediation.is_empty());
    Ok(())
}

#[sinex_test]
async fn all_checkouts_cleanup_removes_stale_lock_and_pid_files() -> Result<()> {
    let base = tempfile::tempdir()?;
    let checkout = tempfile::tempdir()?;
    let cache_root = base.path().join("hash123");
    let dev_state = cache_root.join("dev-state");
    let pg_pid = dev_state.join("data/postgres/postmaster.pid");
    let nats_pid = dev_state.join("run/nats.pid");
    let lock_file = dev_state.join(".lock");
    fs::create_dir_all(pg_pid.parent().unwrap())?;
    fs::create_dir_all(nats_pid.parent().unwrap())?;
    fs::write(&pg_pid, "999999999\n")?;
    fs::write(&nats_pid, "999999998\n")?;
    fs::write(&lock_file, "{}")?;

    let cleanup = AllCheckoutsCleanup::run(
        base.path().to_path_buf(),
        vec![CheckoutInventoryRoot {
            cache_root,
            dev_state_dir: dev_state,
            checkout_path: Some(checkout.path().to_path_buf()),
            lock: LockInspection::Stale(LockInfo {
                pid: 999_999_997,
                checkout_path: checkout.path().to_path_buf(),
                acquired_at: Timestamp::now(),
                description: Some("test stale lock".to_string()),
            }),
        }],
        false,
        true,
    )?;

    assert!(!pg_pid.exists());
    assert!(!nats_pid.exists());
    assert!(!lock_file.exists());
    assert_eq!(cleanup.totals.removed_files, 3);
    assert!(
        cleanup.checkouts[0]
            .actions
            .iter()
            .any(|action| action.action == CleanupActionKind::RemoveStaleLock)
    );
    Ok(())
}

#[sinex_test]
async fn all_checkouts_cleanup_dry_run_leaves_stale_files() -> Result<()> {
    let base = tempfile::tempdir()?;
    let cache_root = base.path().join("hash123");
    let dev_state = cache_root.join("dev-state");
    let nats_pid = dev_state.join("run/nats.pid");
    fs::create_dir_all(nats_pid.parent().unwrap())?;
    fs::write(&nats_pid, "999999998\n")?;

    let cleanup = AllCheckoutsCleanup::run(
        base.path().to_path_buf(),
        vec![CheckoutInventoryRoot {
            cache_root,
            dev_state_dir: dev_state,
            checkout_path: None,
            lock: LockInspection::Missing,
        }],
        true,
        true,
    )?;

    assert!(nats_pid.exists());
    assert_eq!(cleanup.totals.removed_files, 1);
    assert!(cleanup.checkouts[0].actions[0].dry_run);
    Ok(())
}

#[sinex_test]
async fn all_checkouts_cleanup_dry_run_reports_dev_local_sinexd() -> Result<()> {
    let base = tempfile::tempdir()?;
    let checkout = tempfile::tempdir()?;
    let cache_root = base.path().join("hash123");
    let dev_state = cache_root.join("dev-state");
    fs::create_dir_all(&dev_state)?;
    let fake_bin = checkout.path().join("sinexd");
    fs::write(
        &fake_bin,
        "#!/usr/bin/env bash\n\
         sleep 30 &\n\
         child=$!\n\
         trap 'kill \"$child\" 2>/dev/null; wait \"$child\" 2>/dev/null; exit 0' TERM INT EXIT\n\
         wait \"$child\"\n",
    )?;
    let mut permissions = fs::metadata(&fake_bin)?.permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_bin, permissions)?;

    let mut child = StdCommand::new(&fake_bin)
        .current_dir(checkout.path())
        .spawn()
        .wrap_err("failed to spawn fake dev-local sinexd")?;
    let pid = child.id();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let status = AllCheckoutsStatus::gather(
            base.path().to_path_buf(),
            vec![CheckoutInventoryRoot {
                cache_root: cache_root.clone(),
                dev_state_dir: dev_state.clone(),
                checkout_path: Some(checkout.path().to_path_buf()),
                lock: LockInspection::Missing,
            }],
        );
        if status.checkouts[0].sinexd.pids.contains(&pid) {
            let cleanup = AllCheckoutsCleanup::run(
                base.path().to_path_buf(),
                vec![CheckoutInventoryRoot {
                    cache_root,
                    dev_state_dir: dev_state,
                    checkout_path: Some(checkout.path().to_path_buf()),
                    lock: LockInspection::Missing,
                }],
                true,
                false,
            )?;
            assert_eq!(cleanup.totals.stopped_sinexd, 1);
            assert!(
                cleanup.checkouts[0]
                    .actions
                    .iter()
                    .any(|action| action.action == CleanupActionKind::StopSinexd
                        && action.dry_run)
            );
            stop_dev_sinexd_pid(pid, false).ok();
            child.wait().ok();
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    stop_dev_sinexd_pid(pid, false).ok();
    child.wait().ok();
    bail!("fake dev-local sinexd pid {pid} was not detected");
}

#[sinex_test]
async fn current_checkout_status_reports_dev_local_sinexd() -> Result<()> {
    let checkout = crate::config::workspace_root();
    let config = StackConfig::for_current_checkout()?;
    let temp = tempfile::Builder::new()
        .prefix(".sinex-test-sinexd-")
        .tempdir_in(&checkout)?;
    let fake_bin = temp.path().join("sinexd");
    fs::write(
        &fake_bin,
        "#!/usr/bin/env bash\n\
         sleep 30 &\n\
         child=$!\n\
         trap 'kill \"$child\" 2>/dev/null; wait \"$child\" 2>/dev/null; exit 0' TERM INT EXIT\n\
         wait \"$child\"\n",
    )?;
    let mut permissions = fs::metadata(&fake_bin)?.permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_bin, permissions)?;

    let mut child = StdCommand::new(&fake_bin)
        .current_dir(temp.path())
        .spawn()
        .wrap_err("failed to spawn fake current-checkout sinexd")?;
    let pid = child.id();
    let deadline = Instant::now() + Duration::from_secs(2);

    while Instant::now() < deadline {
        let status = StackStatus::gather(&config);
        if status.sinexd.pids.contains(&pid) {
            assert!(status.sinexd.running);
            assert_eq!(status.checkout_root, checkout);
            stop_dev_sinexd_pid(pid, false).ok();
            child.wait().ok();
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }

    stop_dev_sinexd_pid(pid, false).ok();
    child.wait().ok();
    bail!("fake current-checkout sinexd pid {pid} was not detected");
}

#[sinex_test]
async fn parse_cmdline_bytes_ignores_empty_nul_segments() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        parse_cmdline_bytes(b"postgres\0-D\0/tmp/dev-state/data/postgres\0\0"),
        vec![
            "postgres".to_string(),
            "-D".to_string(),
            "/tmp/dev-state/data/postgres".to_string()
        ]
    );
    Ok(())
}

#[sinex_test]
async fn parse_proc_stat_ppid_handles_comm_with_spaces() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        parse_proc_stat_ppid("123 (postgres: checkpointer) S 42 1 1 0"),
        Some(42)
    );
    Ok(())
}

#[sinex_test]
async fn probe_annex_available_treats_missing_binary_as_absent()
-> ::xtask::sandbox::TestResult<()> {
    let available = probe_annex_available(Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "missing",
    )))
    .unwrap();
    assert!(!available);
    Ok(())
}

#[sinex_test]
async fn probe_annex_available_reports_nonzero_status() -> ::xtask::sandbox::TestResult<()> {
    let error = probe_annex_available(Ok(std::process::Output {
        status: std::process::ExitStatus::from_raw(1 << 8),
        stdout: Vec::new(),
        stderr: b"git-annex broken".to_vec(),
    }))
    .unwrap_err();
    assert!(format!("{error:#}").contains("git-annex broken"));
    Ok(())
}

#[sinex_test]
async fn require_successful_command_reports_failure_output() -> ::xtask::sandbox::TestResult<()>
{
    let error = require_successful_command(
        "git init for annex repository",
        Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"permission denied".to_vec(),
        }),
    )
    .unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("permission denied"));
    assert!(message.contains("git init for annex repository"));
    Ok(())
}

#[sinex_test]
async fn annex_git_subprocess_clears_hook_repository_environment()
-> ::xtask::sandbox::TestResult<()> {
    let command = git_subprocess("git");
    for key in GIT_REPOSITORY_ENV_KEYS {
        let is_removed = command
            .get_envs()
            .any(|(name, value)| name == *key && value.is_none());
        assert!(
            is_removed,
            "{key} must be removed so annex initialization cannot mutate the hook caller repo"
        );
    }
    Ok(())
}

#[sinex_test]
async fn list_snapshots_reports_directory_read_failures() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let not_a_dir = temp.path().join("snapshots");
    fs::write(&not_a_dir, "blocked")?;

    let probe = list_snapshots(&not_a_dir);
    assert!(probe.snapshots.is_empty());
    assert!(
        probe
            .issue
            .unwrap_or_default()
            .contains("failed to read snapshots directory")
    );
    Ok(())
}

#[sinex_test]
async fn list_snapshots_collects_known_extensions_sorted() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    fs::write(temp.path().join("b.tar.zst"), "")?;
    fs::write(temp.path().join("a.sql.zst"), "")?;
    fs::write(temp.path().join("ignore.txt"), "")?;

    let probe = list_snapshots(temp.path());
    assert_eq!(probe.snapshots, vec!["a".to_string(), "b".to_string()]);
    assert!(probe.issue.is_none());
    Ok(())
}

#[sinex_test]
async fn collect_snapshot_names_reports_entry_failures_without_dropping_snapshots()
-> ::xtask::sandbox::TestResult<()> {
    let probe = collect_snapshot_names(
        Path::new("/tmp/snapshots"),
        [
            Ok(OsString::from("b.tar.zst")),
            Err(std::io::Error::other("entry read failed")),
            Ok(OsString::from("a.sql.zst")),
            Ok(OsString::from("ignore.txt")),
        ],
    );

    assert_eq!(probe.snapshots, vec!["a".to_string(), "b".to_string()]);
    assert!(
        probe
            .issue
            .unwrap_or_default()
            .contains("failed to read snapshot entry")
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn collect_snapshot_names_reports_non_utf8_entry_names()
-> ::xtask::sandbox::TestResult<()> {
    use std::os::unix::ffi::OsStringExt;

    let probe = collect_snapshot_names(
        Path::new("/tmp/snapshots"),
        [
            Ok(OsString::from_vec(vec![
                b'b', 0xff, b'.', b't', b'a', b'r', b'.', b'z', b's', b't',
            ])),
            Ok(OsString::from("a.sql.zst")),
        ],
    );

    assert_eq!(probe.snapshots, vec!["a".to_string()]);
    assert!(
        probe
            .issue
            .unwrap_or_default()
            .contains("entry name is not valid UTF-8")
    );
    Ok(())
}

#[sinex_test]
async fn dir_size_reports_non_directory_paths() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let file_path = temp.path().join("postgres");
    fs::write(&file_path, "blocked")?;

    let probe = dir_size(&file_path);
    assert_eq!(probe.bytes, 0);
    assert!(
        probe
            .issue
            .unwrap_or_default()
            .contains("expected directory while sizing stack data path")
    );
    Ok(())
}

#[sinex_test]
async fn sync_event_payload_schemas_uses_in_process_registry(
    ctx: TestContext,
) -> TestResult<()> {
    let result = sync_event_payload_schemas_for_database_url(ctx.database_url(), false)?;
    assert!(result.discovered > 0);
    assert_eq!(
        result.discovered,
        result.created + result.updated + result.unchanged
    );
    Ok(())
}

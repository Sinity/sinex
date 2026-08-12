use super::{
    AllCheckoutsCleanup, AllCheckoutsStatus, CleanupActionKind, CleanupScope,
    GIT_REPOSITORY_ENV_KEYS, collect_snapshot_names, dir_size, discover_nats_port, git_subprocess,
    list_snapshots, parse_cmdline_bytes, parse_proc_stat_ppid, probe_annex_available,
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
#[ignore = "sinex-v37i open: port_offset_for_checkout's 100-slot hash collides across plausible \
            concurrent worktree checkouts, handing them the same NATS port"]
async fn nats_port_for_checkout_does_not_collide_across_plausible_worktrees()
-> ::xtask::sandbox::TestResult<()> {
    // Two plausible agent-worktree checkout paths (this project's own naming
    // convention, see /realm/worktrees/agent-<hash> in CLAUDE.md) that a
    // brute-force search found collide on the 100-slot `port_offset_for_checkout`
    // hash space (`sha256(path)[0] % 100`) -- both derive offset 22, so two
    // concurrent agent worktrees using these checkouts would be handed the
    // exact same dev NATS port.
    let a = Path::new("/realm/worktrees/agent-00000000");
    let b = Path::new("/realm/worktrees/agent-00000012");

    assert_ne!(
        StackConfig::nats_port_for_checkout(a),
        StackConfig::nats_port_for_checkout(b),
        "two distinct concurrent-worktree checkout paths were handed the same NATS port \
         ({}) -- the 100-slot hash space is too small to avoid collisions across this \
         project's own documented concurrent-worktree workflow",
        StackConfig::nats_port_for_checkout(a)
    );
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
        CleanupScope::StaleFilesOnly,
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
        CleanupScope::StaleFilesOnly,
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
                CleanupScope::AllRunning,
            )?;
            assert_eq!(cleanup.totals.stopped_sinexd, 1);
            assert!(
                cleanup.checkouts[0]
                    .actions
                    .iter()
                    .any(|action| action.action == CleanupActionKind::StopSinexd && action.dry_run)
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
async fn cleanup_scope_should_stop_running_matrix() -> ::xtask::sandbox::TestResult<()> {
    // StaleFilesOnly never signals a running process, regardless of whether
    // the checkout exists or is even knowable.
    assert!(!CleanupScope::StaleFilesOnly.should_stop_running(Some(true)));
    assert!(!CleanupScope::StaleFilesOnly.should_stop_running(Some(false)));
    assert!(!CleanupScope::StaleFilesOnly.should_stop_running(None));

    // AllRunning is the broad, human-invoked mode: stops regardless of
    // checkout existence.
    assert!(CleanupScope::AllRunning.should_stop_running(Some(true)));
    assert!(CleanupScope::AllRunning.should_stop_running(Some(false)));
    assert!(CleanupScope::AllRunning.should_stop_running(None));

    // OrphanedCheckoutsOnly (sinex-grlv) is the automatic, unconditionally-
    // safe mode: only when the checkout is provably gone. A live checkout, or
    // one we could not determine, must never be touched — that is the
    // never-reap invariant a fully-automatic sweep depends on.
    assert!(!CleanupScope::OrphanedCheckoutsOnly.should_stop_running(Some(true)));
    assert!(CleanupScope::OrphanedCheckoutsOnly.should_stop_running(Some(false)));
    assert!(!CleanupScope::OrphanedCheckoutsOnly.should_stop_running(None));
    Ok(())
}

/// A fake dev-owned process plus a background reaper thread that blocks on
/// its `wait()` as soon as it exits.
///
/// Production dev-postgres is daemonized (`pg_ctl start` detaches, reparented
/// to init), so nothing needs to reap it. A process spawned in-process via
/// `std::process::Command` for a test is different: if it is SIGKILLed but
/// never `wait()`-ed, it becomes a zombie that still answers `kill(pid, 0)
/// == 0`, which would make `force_cleanup`'s "did SIGKILL actually work?"
/// poll see a false "still alive" and fail the whole stop — an artifact of
/// being a direct test-process child, not evidence against the production
/// code. Reaping promptly in the background avoids that artifact while still
/// asserting on the real OS process, not just a recorded `CleanupAction`.
struct ReapedProcess {
    pid: u32,
    exited: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ReapedProcess {
    fn is_alive(&self) -> bool {
        !self.exited.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Spawn a fake `postgres -D <data_dir>` process — matching enough of
/// `postgres_pid_is_dev_owned`'s /proc/pid/cmdline shape (argv0 ends with
/// "postgres", an argument equal to `data_dir`) that the real ownership check
/// treats it as a genuine dev-owned instance — and write a `postmaster.pid`
/// pointing at it, matching what `PostgresManager` reads. On SIGTERM it
/// removes its own `postmaster.pid`, mirroring a real postmaster's clean fast
/// shutdown, so `pg_ctl stop -m fast -w` can actually observe the shutdown
/// (rather than always falling through to the ~10s internal timeout) — the
/// production `pg_stop` path terminates it for real either way.
fn spawn_fake_dev_postgres(data_dir: &Path) -> Result<ReapedProcess> {
    fs::create_dir_all(data_dir)?;
    let bin_path = data_dir.join("postgres");
    fs::write(
        &bin_path,
        "#!/usr/bin/env bash\n\
         pidfile=\"$(dirname \"$0\")/postmaster.pid\"\n\
         trap 'rm -f \"$pidfile\"; exit 0' TERM\n\
         sleep 30 &\n\
         wait \"$!\"\n",
    )?;
    let mut permissions = fs::metadata(&bin_path)?.permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o755);
    fs::set_permissions(&bin_path, permissions)?;
    let child = StdCommand::new(&bin_path)
        .arg("-D")
        .arg(data_dir)
        .spawn()
        .wrap_err("failed to spawn fake dev-owned postgres")?;
    let pid = child.id();
    fs::write(data_dir.join("postmaster.pid"), format!("{pid}\n"))?;

    let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let exited_writer = exited.clone();
    std::thread::spawn(move || {
        let mut child = child;
        let _ = child.wait();
        exited_writer.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    Ok(ReapedProcess { pid, exited })
}

#[sinex_test]
async fn orphaned_only_sweep_stops_postgres_for_deleted_checkout_but_spares_live_one() -> Result<()>
{
    // Two checkouts: one whose directory still exists (must be left running
    // even though nothing else distinguishes it), one whose directory has
    // been removed (the sinex-grlv orphan class — must be reaped). Both
    // dev-postgres data directories live under the cache base, matching
    // production (`$SINEX_DEV_STATE_DIR/data/postgres` under
    // `/var/cache/sinex/...`, never under the checkout itself) — so ownership
    // proof via /proc/pid/cmdline stays valid even after the checkout is gone.
    let base = tempfile::tempdir()?;
    let live_checkout = tempfile::tempdir()?;
    let gone_checkout = tempfile::tempdir()?;
    let gone_checkout_path = gone_checkout.path().to_path_buf();

    let live_cache_root = base.path().join("live-hash");
    let live_dev_state = live_cache_root.join("dev-state");
    let live_pg_data = live_dev_state.join("data/postgres");
    let live_process = spawn_fake_dev_postgres(&live_pg_data)?;

    let gone_cache_root = base.path().join("gone-hash");
    let gone_dev_state = gone_cache_root.join("dev-state");
    let gone_pg_data = gone_dev_state.join("data/postgres");
    let gone_process = spawn_fake_dev_postgres(&gone_pg_data)?;

    // Delete the "gone" checkout directory — mirrors a worktree removed after
    // dispatch. The dev-postgres process and its data dir (under the cache
    // base, not the checkout) are untouched by this.
    drop(gone_checkout);
    assert!(!gone_checkout_path.exists());

    let roots = vec![
        CheckoutInventoryRoot {
            cache_root: live_cache_root,
            dev_state_dir: live_dev_state,
            checkout_path: Some(live_checkout.path().to_path_buf()),
            lock: LockInspection::Missing,
        },
        CheckoutInventoryRoot {
            cache_root: gone_cache_root,
            dev_state_dir: gone_dev_state,
            checkout_path: Some(gone_checkout_path.clone()),
            lock: LockInspection::Missing,
        },
    ];

    let cleanup = AllCheckoutsCleanup::run(
        base.path().to_path_buf(),
        roots,
        false,
        CleanupScope::OrphanedCheckoutsOnly,
    )?;

    assert_eq!(
        cleanup.totals.stopped_postgres, 1,
        "orphan-only sweep must stop exactly the deleted checkout's postgres"
    );
    // `AllCheckoutsCleanup::run` sorts checkouts by cache_root, so look each
    // one up by name rather than assuming input order survived.
    let live_result = cleanup
        .checkouts
        .iter()
        .find(|c| c.cache_root.ends_with("live-hash"))
        .expect("live-hash checkout present in cleanup result");
    let gone_result = cleanup
        .checkouts
        .iter()
        .find(|c| c.cache_root.ends_with("gone-hash"))
        .expect("gone-hash checkout present in cleanup result");
    assert!(
        live_result.actions.is_empty(),
        "the live checkout must never be touched by an orphan-only sweep: {live_result:?}",
    );
    assert!(
        gone_result
            .actions
            .iter()
            .any(|action| action.action == CleanupActionKind::StopPostgres),
        "expected a StopPostgres action for the deleted checkout: {gone_result:?}",
    );

    // Real-process proof: the orphan's real OS process is actually gone
    // (not merely a recorded CleanupAction) and the live one is untouched.
    let stop_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < stop_deadline && gone_process.is_alive() {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        !gone_process.is_alive(),
        "orphaned postgres for the deleted checkout must have actually exited"
    );
    assert!(
        live_process.is_alive(),
        "postgres for the live checkout must still be running"
    );

    unsafe { libc::kill(live_process.pid as i32, libc::SIGKILL) };
    let live_stop_deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < live_stop_deadline && live_process.is_alive() {
        std::thread::sleep(Duration::from_millis(25));
    }
    Ok(())
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
async fn probe_annex_available_treats_missing_binary_as_absent() -> ::xtask::sandbox::TestResult<()>
{
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
async fn require_successful_command_reports_failure_output() -> ::xtask::sandbox::TestResult<()> {
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
async fn collect_snapshot_names_reports_non_utf8_entry_names() -> ::xtask::sandbox::TestResult<()> {
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
async fn sync_event_payload_schemas_uses_in_process_registry(ctx: TestContext) -> TestResult<()> {
    let result = sync_event_payload_schemas_for_database_url(ctx.database_url(), false)?;
    assert!(result.discovered > 0);
    assert_eq!(
        result.discovered,
        result.created + result.updated + result.unchanged
    );
    Ok(())
}

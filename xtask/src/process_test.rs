use super::*;
use crate::sandbox::{EnvGuard, sinex_test};
use tempfile::tempdir;

fn command_env_value(command: &Command, key: &str) -> Option<String> {
    command.get_envs().find_map(|(env_key, env_value)| {
        (env_key == key).then(|| env_value.map(|value| value.to_string_lossy().into_owned()))?
    })
}

#[sinex_test]
async fn test_process_builder_basic() -> TestResult<()> {
    let output = ProcessBuilder::new("echo").arg("hello").run()?;

    assert!(output.success());
    assert_eq!(output.stdout.trim(), "hello");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_git() -> TestResult<()> {
    let output = ProcessBuilder::git().args(["--version"]).run()?;

    assert!(output.success());
    assert!(output.stdout.contains("git version"));
    Ok(())
}

#[sinex_test]
async fn test_process_builder_failure() -> TestResult<()> {
    let result = ProcessBuilder::new("false").run();
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_process_builder_run_success() -> TestResult<()> {
    let success = ProcessBuilder::new("true").run_success()?;
    assert!(success);

    let failure = ProcessBuilder::new("false").run_success()?;
    assert!(!failure);
    Ok(())
}

#[sinex_test]
async fn test_process_builder_stdout() -> TestResult<()> {
    let output = ProcessBuilder::new("echo")
        .arg("test output")
        .run_stdout()?;

    assert_eq!(output, "test output");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_multiple_args() -> TestResult<()> {
    let output = ProcessBuilder::new("echo")
        .args(["one", "two", "three"])
        .run()?;

    assert!(output.success());
    assert_eq!(output.stdout.trim(), "one two three");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_cargo_helper() -> TestResult<()> {
    let output = ProcessBuilder::cargo().args(["--version"]).run()?;

    assert!(output.success());
    assert!(output.stdout.contains("cargo"));
    Ok(())
}

#[sinex_test]
async fn test_cargo_builder_marks_xtask_managed_subprocesses() -> TestResult<()> {
    let command = ProcessBuilder::cargo().build_std_command();

    assert_eq!(
        command_env_value(&command, "SINEX_XTASK_MANAGED_CARGO").as_deref(),
        Some("1")
    );
    Ok(())
}

#[sinex_test]
async fn test_cargo_builder_forces_nonincremental_with_sccache() -> TestResult<()> {
    let mut env = EnvGuard::with_keys(&["RUSTC_WRAPPER", "CARGO_INCREMENTAL"]);
    env.set("RUSTC_WRAPPER", "/nix/store/hash/bin/sccache");
    env.clear("CARGO_INCREMENTAL");

    let command = ProcessBuilder::cargo().build_std_command();

    assert_eq!(
        command_env_value(&command, "CARGO_INCREMENTAL").as_deref(),
        Some("0")
    );
    Ok(())
}

#[sinex_test]
async fn test_cargo_builder_respects_explicit_incremental() -> TestResult<()> {
    let mut env = EnvGuard::with_keys(&["RUSTC_WRAPPER", "CARGO_INCREMENTAL"]);
    env.set("RUSTC_WRAPPER", "/nix/store/hash/bin/sccache");
    env.set("CARGO_INCREMENTAL", "1");

    let command = ProcessBuilder::cargo().build_std_command();

    assert_eq!(command_env_value(&command, "CARGO_INCREMENTAL"), None);
    Ok(())
}

#[sinex_test]
async fn test_cargo_builder_allows_incremental_without_sccache() -> TestResult<()> {
    let mut env = EnvGuard::with_keys(&["RUSTC_WRAPPER", "CARGO_INCREMENTAL"]);
    env.clear("RUSTC_WRAPPER");
    env.clear("CARGO_INCREMENTAL");

    let command = ProcessBuilder::cargo().build_std_command();

    assert_eq!(command_env_value(&command, "CARGO_INCREMENTAL"), None);
    Ok(())
}

#[sinex_test]
async fn test_helper_process_timeout_uses_executable_basename() -> TestResult<()> {
    assert_eq!(
        helper_process_timeout_for_program("/cache/sinex/target/debug/xtask"),
        Duration::from_secs(DEFAULT_HEAVY_HELPER_PROCESS_TIMEOUT_SECS)
    );
    assert_eq!(
        helper_process_timeout_for_program("/nix/store/hash/bin/cargo"),
        Duration::from_secs(DEFAULT_HEAVY_HELPER_PROCESS_TIMEOUT_SECS)
    );
    assert_eq!(
        helper_process_timeout_for_program("/tmp/custom-helper"),
        Duration::from_secs(DEFAULT_HELPER_PROCESS_TIMEOUT_SECS)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test]
async fn test_managed_children_default_to_low_interactive_priority() -> TestResult<()> {
    let output = ProcessBuilder::new("sh")
        .args([
            "-c",
            "printf 'nice=%s\\n' \"$(awk '{ print $19 }' /proc/$$/stat)\"; ionice -p $$",
        ])
        .run()?;

    assert!(output.success());
    assert!(
        output.stdout.contains("nice=10"),
        "managed child did not inherit default nice=10:\n{}",
        output.combined()
    );
    assert!(
        output.stdout.contains("idle"),
        "managed child did not inherit idle IO priority:\n{}",
        output.combined()
    );
    Ok(())
}

#[sinex_test]
async fn test_process_builder_with_description() -> TestResult<()> {
    let result = ProcessBuilder::new("nonexistent_command_xyz")
        .with_description("test command")
        .run();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("test command"));
    Ok(())
}

#[sinex_test]
async fn test_process_builder_env() -> TestResult<()> {
    let output = ProcessBuilder::new("sh")
        .args(["-c", "echo $TEST_VAR"])
        .env("TEST_VAR", "test_value")
        .run()?;

    assert!(output.success());
    assert_eq!(output.stdout.trim(), "test_value");
    Ok(())
}

#[sinex_test]
async fn test_process_builder_current_dir() -> TestResult<()> {
    let output = ProcessBuilder::new("pwd").current_dir("/tmp").run()?;

    assert!(output.success());
    assert!(output.stdout.contains("/tmp"));
    Ok(())
}

#[sinex_test]
async fn test_process_output_combined() -> TestResult<()> {
    let output = ProcessBuilder::new("sh")
        .args(["-c", "echo stdout; echo stderr >&2"])
        .run()?;

    let combined = output.combined();
    assert!(combined.contains("stdout"));
    assert!(combined.contains("stderr"));
    Ok(())
}

#[sinex_test]
async fn test_process_builder_run_ok() -> TestResult<()> {
    ProcessBuilder::new("true").run_ok()?;

    let result = ProcessBuilder::new("false").run_ok();
    assert!(result.is_err());
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test]
async fn test_parse_proc_stat_handles_spacey_command_names() -> TestResult<()> {
    let sample =
        "1234 (cargo check) S 4321 1234 1234 0 -1 4194304 0 0 0 0 11 7 0 0 20 0 4 0 55 4096 33";
    let parsed = parse_proc_stat(sample).expect("sample stat line should parse");
    assert_eq!(parsed.ppid, 4321);
    assert_eq!(parsed.total_cpu_ticks, 18);
    assert_eq!(parsed.start_ticks, 55);
    assert_eq!(parsed.rss_pages, 33);
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test]
async fn test_collect_process_tree_stats_walks_descendants() -> TestResult<()> {
    let table = HashMap::from([
        (
            10_u32,
            ProcSample {
                ppid: 1,
                start_ticks: 100,
                total_cpu_ticks: 5,
                rss_pages: 2,
            },
        ),
        (
            11_u32,
            ProcSample {
                ppid: 10,
                start_ticks: 101,
                total_cpu_ticks: 7,
                rss_pages: 3,
            },
        ),
        (
            12_u32,
            ProcSample {
                ppid: 11,
                start_ticks: 102,
                total_cpu_ticks: 9,
                rss_pages: 4,
            },
        ),
        (
            99_u32,
            ProcSample {
                ppid: 1,
                start_ticks: 200,
                total_cpu_ticks: 100,
                rss_pages: 100,
            },
        ),
    ]);

    let (cpu_ticks, rss_pages, process_count) =
        collect_process_tree_stats(10, &table).expect("root should be present");
    assert_eq!(cpu_ticks, 21);
    assert_eq!(rss_pages, 9);
    assert_eq!(process_count, 3);
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_terminate_registered_process_groups_handles_exited_group_leader() -> TestResult<()> {
    let _ = terminate_registered_process_groups("test setup cleanup")?;
    let dir = tempdir()?;
    let pid_file = dir.path().join("sleep.pid");
    let script = format!("sleep 30 & echo $! > {} ; exit 0", pid_file.display());

    let success = ProcessBuilder::new("sh")
        .args(["-c", &script])
        .run_success()?;
    assert!(success);

    let sleep_pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
    assert_eq!(unsafe { libc::kill(sleep_pid, 0) }, 0);

    let terminated = terminate_registered_process_groups("test cleanup")?;
    assert!(terminated >= 1);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if unsafe { libc::kill(sleep_pid, 0) } != 0 {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    Err(color_eyre::eyre::eyre!(
        "background sleep process {sleep_pid} survived registered process-group cleanup"
    ))
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_process_builder_timeout_kills_descendants() -> TestResult<()> {
    let dir = tempdir()?;
    let pid_file = dir.path().join("sleep.pid");
    let script = format!("sleep 30 & echo $! > {} ; sleep 30", pid_file.display());

    let result = ProcessBuilder::new("sh")
        .args(["-c", &script])
        .with_description("timeout descendant cleanup")
        .with_timeout(Duration::from_millis(250))
        .run_capture();

    let error = result.expect_err("timed command should fail");
    assert!(
        error.to_string().contains("timed out after"),
        "timeout error should mention deadline: {error:#}"
    );

    let sleep_pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if unsafe { libc::kill(sleep_pid, 0) } != 0 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    Err(color_eyre::eyre::eyre!(
        "timed-out ProcessBuilder left descendant process {sleep_pid} alive"
    ))
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_process_builder_run_tokio_status_timeout_kills_descendants() -> TestResult<()> {
    let dir = tempdir()?;
    let pid_file = dir.path().join("sleep.pid");
    let script = format!("sleep 30 & echo $! > {} ; sleep 30", pid_file.display());

    let result = ProcessBuilder::new("sh")
        .args(["-c", &script])
        .with_description("async timeout descendant cleanup")
        .with_timeout(Duration::from_millis(250))
        .run_tokio_status()
        .await;

    let error = result.expect_err("timed async command should fail");
    assert!(
        error.to_string().contains("timed out after"),
        "timeout error should mention deadline: {error:#}"
    );

    let sleep_pid: i32 = std::fs::read_to_string(&pid_file)?.trim().parse()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if unsafe { libc::kill(sleep_pid, 0) } != 0 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    Err(color_eyre::eyre::eyre!(
        "timed-out async ProcessBuilder left descendant process {sleep_pid} alive"
    ))
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_probe_process_tree_metrics_reports_live_descendants() -> TestResult<()> {
    let mut child = ProcessBuilder::new("sh")
        .args(["-c", "sleep 30 & wait"])
        .spawn()?;
    let pid = child.id();

    let snapshot = probe_process_tree_metrics(pid, Duration::from_millis(120))
        .expect("live process tree snapshot should exist");
    assert!(
        snapshot.sample_count >= 1,
        "live probe should record at least one sample"
    );
    assert!(
        snapshot.process_count_max.unwrap_or_default() >= 2,
        "live probe should see the shell plus its background child"
    );

    terminate_std_child_process_group(&mut child, "probe-process-tree", "test cleanup")?;
    let _ = child.wait();
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_resolve_shared_cgroup_targets_prefers_system_slice() -> TestResult<()> {
    let dir = tempdir()?;
    let system_nix_daemon_dir = dir.path().join("system.slice/nix-daemon.service");
    let legacy_nix_daemon_dir = dir
        .path()
        .join("nix.slice/nix-build.slice/nix-daemon.service");
    let system_nix_build_dir = dir.path().join("system.slice/nix-build.slice");
    let legacy_nix_build_dir = dir.path().join("nix.slice/nix-build.slice");
    for path in [
        &system_nix_daemon_dir,
        &legacy_nix_daemon_dir,
        &system_nix_build_dir,
        &legacy_nix_build_dir,
    ] {
        std::fs::create_dir_all(path)?;
        std::fs::write(path.join("cpu.stat"), "usage_usec 1000\n")?;
        std::fs::write(path.join("memory.current"), "67108864\n")?;
    }

    let targets = resolve_shared_cgroup_targets(dir.path());

    assert_eq!(targets.nix_daemon, Some(system_nix_daemon_dir));
    assert_eq!(targets.nix_build_slice, Some(system_nix_build_dir));
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_probe_shared_build_metrics_reports_cgroup_activity() -> TestResult<()> {
    let dir = tempdir()?;
    let nix_daemon_dir = dir
        .path()
        .join("nix.slice/nix-build.slice/nix-daemon.service");
    let nix_build_dir = dir.path().join("nix.slice/nix-build.slice");
    let background_dir = dir.path().join("background.slice");
    std::fs::create_dir_all(&nix_daemon_dir)?;
    std::fs::create_dir_all(&nix_build_dir)?;
    std::fs::create_dir_all(&background_dir)?;

    std::fs::write(nix_daemon_dir.join("cpu.stat"), "usage_usec 1000\n")?;
    std::fs::write(nix_daemon_dir.join("memory.current"), "67108864\n")?;
    std::fs::write(nix_build_dir.join("cpu.stat"), "usage_usec 2000\n")?;
    std::fs::write(nix_build_dir.join("memory.current"), "536870912\n")?;
    std::fs::write(background_dir.join("cpu.stat"), "usage_usec 500\n")?;
    std::fs::write(background_dir.join("memory.current"), "33554432\n")?;

    let nix_daemon_dir_for_thread = nix_daemon_dir.clone();
    let nix_build_dir_for_thread = nix_build_dir.clone();
    let background_dir_for_thread = background_dir.clone();
    let update_handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(40));
        let _ = std::fs::write(
            nix_daemon_dir_for_thread.join("cpu.stat"),
            "usage_usec 21000\n",
        );
        let _ = std::fs::write(
            nix_daemon_dir_for_thread.join("memory.current"),
            "134217728\n",
        );
        let _ = std::fs::write(
            nix_build_dir_for_thread.join("cpu.stat"),
            "usage_usec 42000\n",
        );
        let _ = std::fs::write(
            nix_build_dir_for_thread.join("memory.current"),
            "1073741824\n",
        );
        let _ = std::fs::write(
            background_dir_for_thread.join("cpu.stat"),
            "usage_usec 10500\n",
        );
        let _ = std::fs::write(
            background_dir_for_thread.join("memory.current"),
            "268435456\n",
        );
    });

    let snapshot = probe_shared_build_metrics_at_root(dir.path(), Duration::from_millis(120))
        .expect("shared build metrics should exist for synthetic cgroups");
    update_handle
        .join()
        .expect("cgroup update thread should join");

    assert!(
        snapshot.shared_nix_daemon_cpu_usage_avg.is_some(),
        "shared nix-daemon CPU should be sampled"
    );
    assert_eq!(
        snapshot
            .shared_nix_daemon_memory_usage_max_mb
            .map(f64::round),
        Some(128.0)
    );
    assert!(
        snapshot.shared_nix_build_slice_cpu_usage_avg.is_some(),
        "shared nix-build CPU should be sampled"
    );
    assert_eq!(
        snapshot
            .shared_nix_build_slice_memory_usage_max_mb
            .map(f64::round),
        Some(1024.0)
    );
    assert!(
        snapshot.shared_background_slice_cpu_usage_avg.is_some(),
        "shared background-slice CPU should be sampled"
    );
    assert_eq!(
        snapshot
            .shared_background_slice_memory_usage_max_mb
            .map(f64::round),
        Some(256.0)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_probe_shared_build_metrics_discovers_nested_cgroup_layouts() -> TestResult<()> {
    let dir = tempdir()?;
    let nix_daemon_dir = dir
        .path()
        .join("custom.slice/worker.scope/nix-daemon.service");
    let nix_build_dir = dir
        .path()
        .join("custom.slice/worker.scope/builds/nix-build.slice");
    std::fs::create_dir_all(&nix_daemon_dir)?;
    std::fs::create_dir_all(&nix_build_dir)?;

    std::fs::write(nix_daemon_dir.join("cpu.stat"), "usage_usec 1000\n")?;
    std::fs::write(nix_daemon_dir.join("memory.current"), "67108864\n")?;
    std::fs::write(nix_build_dir.join("cpu.stat"), "usage_usec 2000\n")?;
    std::fs::write(nix_build_dir.join("memory.current"), "536870912\n")?;

    let nix_daemon_dir_for_thread = nix_daemon_dir.clone();
    let nix_build_dir_for_thread = nix_build_dir.clone();
    let update_handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(40));
        let _ = std::fs::write(
            nix_daemon_dir_for_thread.join("cpu.stat"),
            "usage_usec 21000\n",
        );
        let _ = std::fs::write(
            nix_daemon_dir_for_thread.join("memory.current"),
            "134217728\n",
        );
        let _ = std::fs::write(
            nix_build_dir_for_thread.join("cpu.stat"),
            "usage_usec 42000\n",
        );
        let _ = std::fs::write(
            nix_build_dir_for_thread.join("memory.current"),
            "1073741824\n",
        );
    });

    let snapshot = probe_shared_build_metrics_at_root(dir.path(), Duration::from_millis(120))
        .expect("shared build metrics should discover nested cgroup layouts");
    update_handle
        .join()
        .expect("cgroup update thread should join");

    assert!(
        snapshot.shared_nix_daemon_cpu_usage_avg.is_some(),
        "shared nix-daemon CPU should be discovered from basename search"
    );
    assert_eq!(
        snapshot
            .shared_nix_daemon_memory_usage_max_mb
            .map(f64::round),
        Some(128.0)
    );
    assert!(
        snapshot.shared_nix_build_slice_cpu_usage_avg.is_some(),
        "shared nix-build CPU should be discovered from basename search"
    );
    assert_eq!(
        snapshot
            .shared_nix_build_slice_memory_usage_max_mb
            .map(f64::round),
        Some(1024.0)
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test]
async fn test_configure_persistent_service_child_std_creates_dedicated_process_group()
-> TestResult<()> {
    let mut command = std::process::Command::new("sleep");
    command.arg("30");
    configure_persistent_service_child_std(&mut command);

    let mut child = command.spawn()?;
    let pid = child.id() as i32;
    let process_group = nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(pid)))?;
    assert_eq!(process_group.as_raw(), pid);

    terminate_std_child_process_group(&mut child, "persistent-service-child", "test cleanup")?;
    let _ = child.wait();
    Ok(())
}

#[sinex_test]
async fn test_status_indicates_clean_interactive_shutdown_accepts_success_and_interrupts()
-> ::xtask::sandbox::TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        assert!(status_indicates_clean_interactive_shutdown(
            &std::process::ExitStatus::from_raw(0)
        ));
        assert!(status_indicates_clean_interactive_shutdown(
            &std::process::ExitStatus::from_raw(libc::SIGINT)
        ));
        assert!(status_indicates_clean_interactive_shutdown(
            &std::process::ExitStatus::from_raw(libc::SIGTERM)
        ));
        assert!(!status_indicates_clean_interactive_shutdown(
            &std::process::ExitStatus::from_raw(1 << 8)
        ));
    }

    #[cfg(not(unix))]
    {
        let _ = status_indicates_clean_interactive_shutdown;
    }
    Ok(())
}

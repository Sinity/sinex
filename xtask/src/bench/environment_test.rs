use super::{
    Environment, command_stdout, database_url_masked, format_probe_issues, git_dirty,
    truncate_process_line,
};
use crate::sandbox::{sinex_serial_test, sinex_test};

#[sinex_test]
async fn command_stdout_reports_non_zero_exit() -> crate::sandbox::TestResult<()> {
    let error = command_stdout("sh", &["-c", "echo boom >&2; exit 7"])
        .expect_err("non-zero exit should be reported");
    assert!(error.contains("status 7"), "unexpected error: {error}");
    assert!(error.contains("boom"), "unexpected error: {error}");
    Ok(())
}

#[sinex_test]
async fn format_text_includes_probe_issues() -> crate::sandbox::TestResult<()> {
    let env = Environment {
        timestamp: "2026-03-27T00:00:00Z".to_string(),
        hostname: "host".to_string(),
        uname: "uname".to_string(),
        kernel: "kernel".to_string(),
        arch: "x86_64".to_string(),
        os: "NixOS".to_string(),
        cpu_model: "cpu".to_string(),
        cpu_cores: 1,
        cpu_threads: 1,
        memory_total_kb: 1024,
        memory_available_kb: 512,
        load_avg: "0.0 0.0 0.0".to_string(),
        pressure_cpu_some_avg10: Some(1.0),
        pressure_io_some_avg10: Some(2.0),
        pressure_io_full_avg10: Some(3.0),
        pressure_memory_some_avg10: Some(4.0),
        pressure_memory_full_avg10: Some(5.0),
        shm_used_mb: Some(6.0),
        shm_free_mb: Some(7.0),
        sinnix_observe_available: false,
        active_heavy_processes: vec!["pid 1: cargo test".to_string()],
        rustc_version: "rustc".to_string(),
        cargo_version: "cargo".to_string(),
        rustup_toolchain: "toolchain".to_string(),
        postgres_version: "psql".to_string(),
        database_url_masked: "postgres://***@db/sinex".to_string(),
        nats_url: "nats://127.0.0.1:4222".to_string(),
        git_sha: "abc".to_string(),
        git_sha_short: "abc".to_string(),
        git_branch: "master".to_string(),
        git_dirty: false,
        probe_issues: vec!["hostname: failed".to_string()],
    };

    let text = env.format_text();
    assert!(text.contains("## Probe issues"));
    assert!(text.contains("hostname: failed"));
    Ok(())
}

#[sinex_test]
async fn database_url_masked_redacts_credentials() -> crate::sandbox::TestResult<()> {
    let old = std::env::var_os("DATABASE_URL");
    unsafe {
        std::env::set_var("DATABASE_URL", "postgres://user:secret@example.test/sinex");
    }
    let masked = database_url_masked();
    match old {
        Some(value) => unsafe { std::env::set_var("DATABASE_URL", value) },
        None => unsafe { std::env::remove_var("DATABASE_URL") },
    }
    assert_eq!(masked, "postgres://***@example.test/sinex");
    assert_eq!(
        format_probe_issues(&["boom".to_string()]),
        "\n## Probe issues\n- boom\n"
    );
    Ok(())
}

#[test]
fn truncate_process_line_does_not_panic_on_multibyte_command() {
    // /proc/<pid>/cmdline can contain arbitrary argv bytes, including
    // multi-byte UTF-8 well before the byte-length budget. The previous
    // implementation sliced `&command[..max]` directly and panicked when
    // `max` landed inside a multi-byte codepoint.
    let command = "проверка ".repeat(20);
    let truncated = truncate_process_line(&command, 15);
    assert!(truncated.chars().all(|c| c != '\u{FFFD}'));
}

/// sinex-fd72: `git_dirty()` uses `git diff --quiet` alone, which compares the
/// working tree against the INDEX, not the index against HEAD. A change that
/// has been `git add`'d but not committed is invisible to it -- the working
/// tree now matches the index, so `git diff --quiet` reports clean even though
/// the repo genuinely has uncommitted (staged) changes.
#[sinex_serial_test]
#[ignore = "sinex-fd72 open: git_dirty() uses `git diff --quiet` alone, which misses staged-but-uncommitted changes"]
async fn git_dirty_detects_staged_but_uncommitted_changes() -> crate::sandbox::TestResult<()> {
    let repo = tempfile::tempdir()?;
    let run_git = |args: &[&str]| -> crate::sandbox::TestResult<()> {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(repo.path())
            .status()?;
        assert!(status.success(), "git {args:?} failed");
        Ok(())
    };

    run_git(&["init", "-q"])?;
    run_git(&["config", "user.email", "test@example.test"])?;
    run_git(&["config", "user.name", "Test"])?;
    let tracked = repo.path().join("tracked.txt");
    std::fs::write(&tracked, "original\n")?;
    run_git(&["add", "tracked.txt"])?;
    run_git(&["commit", "-q", "-m", "initial"])?;

    // Modify and stage, but do NOT commit -- the repo is genuinely dirty.
    std::fs::write(&tracked, "modified\n")?;
    run_git(&["add", "tracked.txt"])?;

    let cwd = std::env::current_dir()?;
    std::env::set_current_dir(repo.path())?;
    let dirty = git_dirty();
    std::env::set_current_dir(cwd)?;

    assert_eq!(
        dirty,
        Ok(true),
        "a staged-but-uncommitted change must be reported as dirty; sinex-fd72",
    );
    Ok(())
}

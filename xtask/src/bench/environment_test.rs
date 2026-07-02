use super::{Environment, command_stdout, database_url_masked, format_probe_issues};
use crate::sandbox::sinex_test;

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

use std::process::Command;

#[test]
fn deployed_serve_route_fails_closed_before_subsystem_startup_when_storage_is_insufficient() {
    let output = Command::new("timeout")
        // The systemd unit invokes this same explicit `serve` route. Keep the
        // normal subsystem defaults enabled so this proves the production
        // boundary, rather than a test-only configuration path.
        .args(["5s", env!("CARGO_BIN_EXE_sinexd"), "serve"])
        .env("SINEX_STATE_DIR", "/dev/full")
        .env("SINEX_CONTENT_STORE_PATH", "/dev/full")
        .env("SINEX_DATA_DIR", "/dev/full")
        .env("SINEX_LOG_DIR", "/dev/full")
        .env("SINEX_WORK_DIR", "/dev/full")
        .env("TMPDIR", "/dev/full")
        .env_remove("SINEX_EVENT_ENGINE_ENABLED")
        .env_remove("SINEX_API_ENABLED")
        // If the startup guard moves below subsystem configuration, the
        // normal production route must fail on this missing downstream input
        // instead of reporting the storage preflight failure.
        .env_remove("DATABASE_URL")
        .output()
        .expect("timeout and sinexd must be executable");

    assert_eq!(
        output.status.code(),
        Some(1),
        "sinexd must fail immediately, not time out or continue startup"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Insufficient disk space"),
        "startup must report the fail-closed storage error, got:\n{stderr}"
    );
    assert!(
        !stderr.contains("Database URL"),
        "storage preflight must run before event-engine database configuration, got:\n{stderr}"
    );
}

use std::process::Command;

#[test]
fn serve_fails_closed_before_startup_when_storage_is_insufficient() {
    let output = Command::new("timeout")
        .args(["5s", env!("CARGO_BIN_EXE_sinexd"), "serve"])
        .env("SINEX_STATE_DIR", "/dev/full")
        .env("SINEX_CONTENT_STORE_PATH", "/dev/full")
        .env("SINEX_DATA_DIR", "/dev/full")
        .env("SINEX_LOG_DIR", "/dev/full")
        .env("SINEX_WORK_DIR", "/dev/full")
        .env("TMPDIR", "/dev/full")
        .env("SINEX_EVENT_ENGINE_ENABLED", "0")
        .env("SINEX_API_ENABLED", "0")
        .output()
        .expect("timeout and sinexd must be executable");

    assert!(!output.status.success(), "sinexd must refuse startup");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Insufficient disk space"),
        "startup must report the fail-closed storage error, got:\n{stderr}"
    );
}

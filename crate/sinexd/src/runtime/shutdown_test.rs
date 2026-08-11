use super::default_checkpoint_path;
use xtask::sandbox::{EnvGuard, sinex_serial_test};

/// `sinex-6yi8`: `default_checkpoint_path`'s fallback branch (no
/// `SINEX_RUNTIME_DIR`/`SINEX_WORK_DIR` set) resolves via raw
/// `dirs::cache_dir()` with no `environment().work_directory(...)`
/// namespacing, unlike its two siblings `default_work_dir` (config.rs) and
/// `resolve_work_dir` (runtime_cli.rs), both of which append the
/// deployment-environment suffix. Without that suffix, a dev and a prod
/// `sinexd`/automaton-adapter instance on the same host (no `SINEX_RUNTIME_DIR`
/// override — true for any non-NixOS run) resolve checkpoints to the exact
/// same path and silently cross-contaminate replay/hot-reload state.
#[sinex_serial_test]
async fn default_checkpoint_path_is_namespaced_by_deployment_environment()
-> xtask::sandbox::TestResult<()> {
    let _env_override =
        sinex_primitives::environment::override_environment_for_tests("sinex-6yi8-test-ns")?;
    let mut env_guard = EnvGuard::with_keys(&["SINEX_RUNTIME_DIR", "SINEX_WORK_DIR"]);
    env_guard.remove("SINEX_RUNTIME_DIR");
    env_guard.remove("SINEX_WORK_DIR");

    let path = default_checkpoint_path("sinex-6yi8-module");
    let path_str = path.to_string_lossy();

    assert!(
        path_str.contains("sinex-6yi8-test-ns"),
        "default_checkpoint_path must namespace its `dirs::cache_dir()` fallback \
         through environment().work_directory(...) like its two siblings \
         (default_work_dir in config.rs, resolve_work_dir in runtime_cli.rs) -- \
         got unnamespaced path {path_str:?}, so a dev and a prod instance on the \
         same host with no SINEX_RUNTIME_DIR/SINEX_WORK_DIR override would \
         silently share the same checkpoint file"
    );

    Ok(())
}

/// `sinex-6yi8`: when `SINEX_RUNTIME_DIR` IS set, `default_checkpoint_path`
/// takes it as an already-caller-provided absolute path and does not
/// re-namespace it -- this pins that intentional pass-through behavior so a
/// future fix for the fallback branch above doesn't accidentally start
/// double-namespacing an explicit override too.
#[sinex_serial_test]
async fn default_checkpoint_path_does_not_renamespace_explicit_runtime_dir()
-> xtask::sandbox::TestResult<()> {
    let mut env_guard = EnvGuard::with_keys(&["SINEX_RUNTIME_DIR", "SINEX_WORK_DIR"]);
    env_guard.set("SINEX_RUNTIME_DIR", "/tmp/sinex-6yi8-explicit-dir");
    env_guard.remove("SINEX_WORK_DIR");

    let path = default_checkpoint_path("sinex-6yi8-module");

    assert_eq!(
        path,
        std::path::PathBuf::from("/tmp/sinex-6yi8-explicit-dir/sinex-6yi8-module.checkpoint.json")
    );

    Ok(())
}

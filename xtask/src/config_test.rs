use super::*;
use crate::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn test_config_from_env() -> TestResult<()> {
    let config = Config::from_env();
    // Should at least have a hostname
    assert!(!config.hostname.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_history_db_path() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_STATE_DIR", "XTASK_HISTORY_DB"]);
    env.set("SINEX_STATE_DIR", dir.path());
    env.clear("XTASK_HISTORY_DB");

    let config = Config::from_env();
    let path = config.history_db_path();
    assert_eq!(path, dir.path().join("xtask-history.db"));
    Ok(())
}

#[sinex_test]
async fn test_history_db_path_respects_override() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let override_path = dir.path().join("custom-history.db");
    let mut env = EnvGuard::with_keys(&["SINEX_STATE_DIR", "XTASK_HISTORY_DB"]);
    env.set("SINEX_STATE_DIR", dir.path());
    env.set("XTASK_HISTORY_DB", &override_path);

    let config = Config::from_env();
    let path = config.history_db_path();
    assert_eq!(path, override_path);
    Ok(())
}

#[sinex_test]
async fn test_workspace_state_dir_rejects_sinnix_dev_cache_state() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let stale_state = PathBuf::from("/var/cache/sinex/sinity/hash/dev-state/state");
    let mut env = EnvGuard::with_keys(&["SINEX_STATE_DIR"]);
    env.set("SINEX_STATE_DIR", &stale_state);

    assert_eq!(
        workspace_state_dir_for(workspace.path()),
        workspace.path().join(".sinex/state")
    );
    Ok(())
}

#[sinex_test]
async fn test_workspace_state_dir_honors_explicit_temp_override() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let state = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_STATE_DIR"]);
    env.set("SINEX_STATE_DIR", state.path());

    assert_eq!(workspace_state_dir_for(workspace.path()), state.path());
    Ok(())
}

#[sinex_test]
async fn test_config_cache_dir_respects_sinex_cache_dir() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let cache_dir = dir.path().join("explicit-cache");
    let mut env = EnvGuard::with_keys(&["SINEX_CACHE_DIR", "SINEX_DEV_CACHE_ROOT"]);
    env.set("SINEX_CACHE_DIR", &cache_dir);
    env.clear("SINEX_DEV_CACHE_ROOT");

    let config = Config::from_env();
    assert_eq!(config.cache_dir, cache_dir);
    Ok(())
}

#[sinex_test]
async fn test_config_cache_dir_uses_dev_cache_root_without_cache_dir() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let cache_root = dir.path().join("dev-cache");
    let mut env = EnvGuard::with_keys(&["SINEX_CACHE_DIR", "SINEX_DEV_CACHE_ROOT"]);
    env.clear("SINEX_CACHE_DIR");
    env.set("SINEX_DEV_CACHE_ROOT", &cache_root);

    let config = Config::from_env();
    assert_eq!(config.cache_dir, cache_root);
    Ok(())
}

#[sinex_test]
async fn test_workspace_target_dir_respects_cargo_target_dir() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let target_dir = dir.path().join("target-cache");
    let mut env = EnvGuard::with_keys(&["CARGO_TARGET_DIR"]);
    env.set("CARGO_TARGET_DIR", &target_dir);

    assert_eq!(workspace_target_dir(), target_dir);
    Ok(())
}

#[sinex_test]
async fn test_workspace_cache_root_respects_sinex_dev_cache_root() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let cache_root = workspace.path().join("configured-cache");
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_CACHE_ROOT", "CARGO_TARGET_DIR"]);
    env.set("SINEX_DEV_CACHE_ROOT", &cache_root);
    env.clear("CARGO_TARGET_DIR");

    assert_eq!(workspace_cache_root_for(workspace.path()), cache_root);
    Ok(())
}

#[sinex_test]
async fn test_workspace_cache_root_falls_back_to_checkout_cache() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_CACHE_ROOT"]);
    env.clear("SINEX_DEV_CACHE_ROOT");

    assert_eq!(
        workspace_cache_root_for(workspace.path()),
        workspace.path().join(".sinex/cache")
    );
    Ok(())
}

#[sinex_test]
async fn test_workspace_target_dir_respects_sinex_dev_cache_root() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let cache_root = workspace.path().join("configured-cache");
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_CACHE_ROOT", "CARGO_TARGET_DIR"]);
    env.set("SINEX_DEV_CACHE_ROOT", &cache_root);
    env.clear("CARGO_TARGET_DIR");

    assert_eq!(
        workspace_target_dir_for(workspace.path()),
        cache_root.join("target")
    );
    Ok(())
}

/// Mint a synthetic sinex checkout layout (Cargo.toml + xtask/Cargo.toml)
/// under `root` so the workspace markers resolve there.
fn write_synthetic_checkout(root: &Path) -> std::io::Result<()> {
    std::fs::write(root.join("Cargo.toml"), "[workspace]\n")?;
    std::fs::create_dir_all(root.join("xtask/src"))?;
    std::fs::write(
        root.join("xtask/Cargo.toml"),
        "[package]\nname = \"xtask\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
    )
}

#[sinex_test]
async fn test_path_belongs_to_other_checkout_detects_cross_checkout() -> TestResult<()> {
    let active = tempfile::tempdir()?;
    let other = tempfile::tempdir()?;
    write_synthetic_checkout(active.path())?;
    write_synthetic_checkout(other.path())?;
    let other_inner = other.path().join("crate/foo");
    std::fs::create_dir_all(&other_inner)?;

    assert!(path_belongs_to_other_checkout(&other_inner, active.path()));
    assert!(!path_belongs_to_other_checkout(
        &active.path().join("crate/foo"),
        active.path()
    ));
    Ok(())
}

#[sinex_test]
async fn test_workspace_pinned_env_path_ignores_cross_checkout_value() -> TestResult<()> {
    let active = tempfile::tempdir()?;
    let other = tempfile::tempdir()?;
    write_synthetic_checkout(active.path())?;
    write_synthetic_checkout(other.path())?;
    let cross = other.path().join(".sinex/state");

    let mut env = EnvGuard::with_keys(&["SINEX_TEST_PINNED_PATH"]);
    env.set("SINEX_TEST_PINNED_PATH", &cross);
    let fallback = active.path().join("fallback");
    let resolved =
        workspace_pinned_env_path("SINEX_TEST_PINNED_PATH", active.path(), || fallback.clone());
    assert_eq!(
        resolved, fallback,
        "cross-checkout env value must fall back to the workspace-local default"
    );
    Ok(())
}

#[sinex_test]
async fn test_workspace_pinned_env_path_honors_local_value() -> TestResult<()> {
    let active = tempfile::tempdir()?;
    write_synthetic_checkout(active.path())?;
    let inside = active.path().join(".sinex/state");

    let mut env = EnvGuard::with_keys(&["SINEX_TEST_PINNED_PATH"]);
    env.set("SINEX_TEST_PINNED_PATH", &inside);
    let resolved = workspace_pinned_env_path("SINEX_TEST_PINNED_PATH", active.path(), || {
        active.path().join("fallback")
    });
    assert_eq!(resolved, inside);
    Ok(())
}

#[sinex_test]
async fn test_workspace_pinned_env_path_honors_external_value() -> TestResult<()> {
    // /dev/shm and /tmp paths are legitimate explicit overrides — not in
    // any sinex checkout, so they must be honored.
    let active = tempfile::tempdir()?;
    write_synthetic_checkout(active.path())?;
    let external = tempfile::tempdir_in("/tmp")?;
    // No write_synthetic_checkout — `external` is not a sinex checkout.

    let mut env = EnvGuard::with_keys(&["SINEX_TEST_PINNED_PATH"]);
    env.set("SINEX_TEST_PINNED_PATH", external.path());
    let resolved = workspace_pinned_env_path("SINEX_TEST_PINNED_PATH", active.path(), || {
        active.path().join("fallback")
    });
    assert_eq!(
        resolved,
        external.path(),
        "paths outside any sinex checkout must be honored"
    );
    Ok(())
}

#[sinex_test]
async fn test_workspace_root_discovery_prefers_enclosing_checkout() -> TestResult<()> {
    let checkout = tempfile::tempdir()?;
    write_synthetic_checkout(checkout.path())?;
    std::fs::create_dir_all(checkout.path().join("crate/sinex-primitives"))?;

    let nested = checkout.path().join("crate/sinex-primitives");
    let root = workspace_root_from_current_dir(&nested)
        .expect("nested checkout path should resolve to workspace root");
    assert_eq!(root, checkout.path());
    Ok(())
}

#[sinex_test]
async fn test_workspace_root_discovery_rejects_non_xtask_workspace() -> TestResult<()> {
    let other = tempfile::tempdir_in("/tmp")?;
    std::fs::write(other.path().join("Cargo.toml"), "[workspace]\n")?;

    assert!(
        workspace_root_from_current_dir(other.path()).is_none(),
        "a generic Cargo workspace without xtask/Cargo.toml is not the Sinex root"
    );
    Ok(())
}

#[sinex_test]
async fn test_load_preferences_valid_toml() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let prefs_dir = dir.path().join("xtask");
    std::fs::create_dir_all(&prefs_dir)?;
    std::fs::write(
        prefs_dir.join("preferences.toml"),
        "notify_on_completion = true\n",
    )?;

    let prefs = load_user_preferences_from(dir.path())?;
    assert!(
        prefs.notify_on_completion,
        "should read notify_on_completion = true"
    );
    Ok(())
}

#[sinex_test]
async fn test_load_preferences_missing_file() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    // No preferences.toml written — should return defaults without panic
    let prefs = load_user_preferences_from(dir.path())?;
    assert!(
        !prefs.notify_on_completion,
        "missing file should yield default false"
    );
    Ok(())
}

#[sinex_test]
async fn test_load_preferences_malformed_toml() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let prefs_dir = dir.path().join("xtask");
    std::fs::create_dir_all(&prefs_dir)?;
    std::fs::write(prefs_dir.join("preferences.toml"), "[[[not valid")?;

    let error = load_user_preferences_from(dir.path())
        .expect_err("malformed TOML should surface a parse error");
    assert!(
        error.to_string().contains("failed to parse"),
        "expected parse context, got: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn test_load_preferences_rejects_unknown_coordinator_section() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let prefs_dir = dir.path().join("xtask");
    std::fs::create_dir_all(&prefs_dir)?;
    std::fs::write(
        prefs_dir.join("preferences.toml"),
        "notify_on_completion = true\n\n[coordinator]\nauto_sequence = [\"check -> test\"]\n",
    )?;

    let error = load_user_preferences_from(dir.path())
        .expect_err("schema-only coordinator preferences should not be accepted");
    assert!(
        error.to_string().contains("failed to parse"),
        "expected parse context, got: {error}"
    );
    Ok(())
}

// ── Foreign-target-dir guard tests ────────────────────────────────────────

/// A `/var/cache/sinex/<user>/<OTHER-hash>/target` inherited from the
/// orchestrator's devshell must be overridden to the worktree-correct dir.
///
/// This is the exact scenario that caused #1749 to merge non-compiling: the
/// worktree agent's `CARGO_TARGET_DIR` pointed at the main checkout's warm
/// artifacts and `xtask check` false-passed in 0.3 s.
#[sinex_test]
async fn test_workspace_target_dir_overrides_foreign_sinex_cache_hash() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    // The hash for THIS workspace (tempdir path) is computed by workspace_hash.
    let correct_hash = workspace_hash(workspace.path());
    // Construct a cache path with a DIFFERENT hash — simulates the orchestrator's dir.
    let other_hash = if correct_hash == "000000000000" {
        "111111111111"
    } else {
        "000000000000"
    };
    let foreign = format!("/var/cache/sinex/sinity/{other_hash}/target");

    let mut env = EnvGuard::with_keys(&["CARGO_TARGET_DIR", "SINEX_DEV_CACHE_ROOT"]);
    env.set("CARGO_TARGET_DIR", &foreign);
    env.clear("SINEX_DEV_CACHE_ROOT");

    // The function must NOT use the foreign path. It must fall back to the
    // workspace-local cache tree.
    let expected = workspace.path().join(".sinex/cache/target");
    assert_eq!(
        workspace_target_dir_for(workspace.path()),
        expected,
        "foreign-hash CARGO_TARGET_DIR must be overridden to the worktree-correct dir"
    );
    Ok(())
}

/// A `/var/cache/sinex/<user>/<SAME-hash>/target` that was legitimately set
/// by the active devshell must be respected verbatim — it is not foreign.
#[sinex_test]
async fn test_workspace_target_dir_keeps_matching_sinex_cache_hash() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let hash = workspace_hash(workspace.path());
    let matching = format!("/var/cache/sinex/sinity/{hash}/target");

    let mut env = EnvGuard::with_keys(&["CARGO_TARGET_DIR"]);
    env.set("CARGO_TARGET_DIR", &matching);

    assert_eq!(
        workspace_target_dir_for(workspace.path()),
        PathBuf::from(&matching),
        "a same-hash sinnix cache path must be returned unchanged"
    );
    Ok(())
}

/// An arbitrary user-set `CARGO_TARGET_DIR` (not a sinnix cache shape, not
/// inside another checkout) must always be respected verbatim.
///
/// This ensures the existing `test_workspace_target_dir_respects_cargo_target_dir`
/// contract holds after the foreign-target guard is added.
#[sinex_test]
async fn test_workspace_target_dir_keeps_arbitrary_custom_path() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let custom = workspace.path().join("my-custom-target");

    let mut env = EnvGuard::with_keys(&["CARGO_TARGET_DIR", "SINEX_DEV_CACHE_ROOT"]);
    env.set("CARGO_TARGET_DIR", &custom);
    env.clear("SINEX_DEV_CACHE_ROOT");

    assert_eq!(
        workspace_target_dir_for(workspace.path()),
        custom,
        "an arbitrary non-cache CARGO_TARGET_DIR must not be overridden"
    );
    Ok(())
}

/// When `CARGO_TARGET_DIR` is unset the function falls back to the
/// workspace-local cache tree.
#[sinex_test]
async fn test_workspace_target_dir_fallback_when_unset() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["CARGO_TARGET_DIR", "SINEX_DEV_CACHE_ROOT"]);
    env.clear("CARGO_TARGET_DIR");
    env.clear("SINEX_DEV_CACHE_ROOT");

    assert_eq!(
        workspace_target_dir_for(workspace.path()),
        workspace.path().join(".sinex/cache/target")
    );
    Ok(())
}

/// A foreign-hash sinnix cache path in `SINEX_DEV_CACHE_ROOT` is also
/// rejected by `workspace_pinned_env_path` so the corrected target-dir
/// fallback chain does not silently route back to the main checkout's cache.
#[sinex_test]
async fn test_workspace_pinned_env_path_rejects_foreign_sinex_cache_hash() -> TestResult<()> {
    let workspace = tempfile::tempdir()?;
    let correct_hash = workspace_hash(workspace.path());
    let other_hash = if correct_hash == "000000000000" {
        "111111111111"
    } else {
        "000000000000"
    };
    let foreign = PathBuf::from(format!("/var/cache/sinex/sinity/{other_hash}"));
    let fallback_dir = workspace.path().join(".sinex/cache");

    let mut env = EnvGuard::with_keys(&["SINEX_TEST_PINNED_PATH"]);
    env.set("SINEX_TEST_PINNED_PATH", &foreign);
    let resolved = workspace_pinned_env_path("SINEX_TEST_PINNED_PATH", workspace.path(), || {
        fallback_dir.clone()
    });
    assert_eq!(
        resolved, fallback_dir,
        "a foreign-hash sinnix cache path must fall back to the workspace-local default"
    );
    Ok(())
}

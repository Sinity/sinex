use super::*;
use crate::sandbox::EnvGuard;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn resolve_state_dir_honors_dev_state_relocation() -> TestResult<()> {
    // The dev shell relocates infra state onto NVMe via SINEX_DEV_STATE_DIR.
    // A relocated path outside any checkout must be honored verbatim so the
    // postgres socket lands where DATABASE_URL/PGHOST advertise it.
    let relocated = tempfile::tempdir()?;
    let checkout = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_STATE_DIR"]);
    env.set("SINEX_DEV_STATE_DIR", relocated.path());

    let resolved = CheckoutState::resolve_state_dir(checkout.path());
    assert_eq!(resolved, relocated.path());
    Ok(())
}

#[sinex_test]
async fn resolve_state_dir_falls_back_to_in_checkout() -> TestResult<()> {
    // Unset env (CI, bare `nix develop`) → in-checkout `.sinex/`, preserving
    // pre-relocation behavior.
    let checkout = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_STATE_DIR"]);
    env.clear("SINEX_DEV_STATE_DIR");

    let resolved = CheckoutState::resolve_state_dir(checkout.path());
    assert_eq!(resolved, checkout.path().join(".sinex"));
    Ok(())
}

#[sinex_test]
async fn test_lock_info_current() -> TestResult<()> {
    let info = LockInfo::current(PathBuf::from("/test/path"), Some("test".to_string()));
    assert_eq!(info.pid, std::process::id());
    assert_eq!(info.checkout_path, PathBuf::from("/test/path"));
    assert!(info.is_alive()); // Current process should be alive
    Ok(())
}

#[sinex_test]
async fn test_lock_info_dead_process() -> TestResult<()> {
    let info = LockInfo {
        pid: 99_999_999, // Very unlikely to exist
        checkout_path: PathBuf::from("/test"),
        acquired_at: Timestamp::now(),
        description: None,
    };
    // This might actually be alive in rare cases, but usually not
    // Just test that is_alive() doesn't crash
    let _ = info.is_alive();
    Ok(())
}

#[sinex_test]
async fn test_remove_lock_file_reports_remove_failures() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let error = remove_lock_file(temp.path(), "test lock").unwrap_err();
    assert!(format!("{error:#}").contains("failed to remove test lock"));
    Ok(())
}

#[sinex_test]
async fn test_release_lock_reports_malformed_lock_file() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let state = CheckoutState::new(temp.path().to_path_buf())?;
    fs::create_dir_all(state.state_dir())?;
    fs::write(state.lock_file(), "not json")?;

    let error = state.release_lock().unwrap_err();
    assert!(format!("{error:#}").contains("Failed to parse lock file during release"));
    Ok(())
}

#[sinex_test]
async fn inventory_roots_under_maps_dev_state_locks_without_cleanup() -> TestResult<()> {
    let base = tempfile::tempdir()?;
    let checkout = tempfile::tempdir()?;
    let dev_state = base.path().join("hash123/dev-state");
    fs::create_dir_all(&dev_state)?;
    let lock = LockInfo::current(checkout.path().to_path_buf(), Some("infra".to_string()));
    fs::write(
        dev_state.join(".lock"),
        serde_json::to_string_pretty(&lock)?,
    )?;

    let roots = CheckoutState::inventory_roots_under(base.path())?;

    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].dev_state_dir, dev_state);
    assert_eq!(roots[0].checkout_path.as_deref(), Some(checkout.path()));
    assert!(matches!(roots[0].lock, LockInspection::Live(_)));
    assert!(roots[0].dev_state_dir.join(".lock").exists());
    Ok(())
}

#[sinex_test]
async fn inventory_roots_under_reports_malformed_locks() -> TestResult<()> {
    let base = tempfile::tempdir()?;
    let dev_state = base.path().join("hash123/dev-state");
    fs::create_dir_all(&dev_state)?;
    fs::write(dev_state.join(".lock"), "not json")?;

    let roots = CheckoutState::inventory_roots_under(base.path())?;

    assert_eq!(roots.len(), 1);
    assert!(roots[0].checkout_path.is_none());
    assert!(matches!(roots[0].lock, LockInspection::Malformed(_)));
    Ok(())
}

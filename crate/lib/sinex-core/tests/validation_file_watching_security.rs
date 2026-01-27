use camino::Utf8Path;
use sinex_core::types::validation::file_watching_security::{
    check_path_depth, validate_discovered_file, validate_watch_path, validate_watch_paths,
    FileWatchingSecurityPolicy,
};
use xtask::sandbox::sinex_test;

#[sinex_test]
fn file_watching_policy_respects_forbidden_paths() -> TestResult<()> {
    let policy = FileWatchingSecurityPolicy::default();
    assert!(validate_watch_path("/etc/shadow", &policy).is_err());
    assert!(validate_watch_path("/proc/version", &policy).is_err());

    let temp_dir = std::env::temp_dir();
    if let Some(temp_str) = temp_dir.to_str() {
        assert!(validate_watch_path(temp_str, &policy).is_ok());
    }

    let permissive = FileWatchingSecurityPolicy::permissive();
    assert!(validate_watch_path("/etc/shadow", &permissive).is_ok());
    Ok(())
}

#[sinex_test]
fn validating_multiple_watch_paths_returns_all() -> TestResult<()> {
    let policy = FileWatchingSecurityPolicy::default();
    let temp_dir = std::env::temp_dir();
    let temp_str = temp_dir.to_str().unwrap_or("/tmp");

    let paths = vec![format!("{temp_str}/test1"), format!("{temp_str}/test2")];
    let validated = validate_watch_paths(&paths, &policy)?;
    assert_eq!(validated.len(), 2);

    let bad_paths = vec![format!("{temp_str}/test"), "/etc/shadow".to_string()];
    assert!(validate_watch_paths(&bad_paths, &policy).is_err());
    Ok(())
}

#[sinex_test]
fn path_depth_checks_enforce_limits() -> TestResult<()> {
    let shallow_path = Utf8Path::new("home/user");
    let deep_path = Utf8Path::new("home/user/docs/projects/sinex/src/lib/core/types");

    assert!(check_path_depth(shallow_path, Some(10)).is_ok());
    assert!(check_path_depth(deep_path, Some(10)).is_ok());
    assert!(check_path_depth(deep_path, Some(3)).is_err());
    assert!(check_path_depth(deep_path, None).is_ok());
    Ok(())
}

#[sinex_test]
fn discovered_file_validation_holds_roots() -> TestResult<()> {
    let policy = FileWatchingSecurityPolicy::default();
    let temp_dir = std::env::temp_dir();
    let temp_str = temp_dir.to_str().unwrap_or("/tmp");

    assert!(validate_discovered_file(&format!("{temp_str}/test.txt"), temp_str, &policy).is_ok());
    assert!(validate_discovered_file("../../etc/passwd", temp_str, &policy).is_err());
    Ok(())
}

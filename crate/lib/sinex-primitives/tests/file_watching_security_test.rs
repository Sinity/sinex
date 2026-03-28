use sinex_primitives::validation::file_watching_security::{
    FileWatchingSecurityPolicy, validate_watch_paths,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn validate_watch_paths_counts_accessible_trees() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("root.log"), "ok")?;
    std::fs::create_dir(dir.path().join("nested"))?;
    std::fs::write(dir.path().join("nested").join("child.log"), "ok")?;

    let mut policy = FileWatchingSecurityPolicy::permissive();
    policy.max_watched_files = Some(4);

    let paths = vec![dir.path().to_string_lossy().into_owned()];
    let validated = validate_watch_paths(&paths, &policy)?;
    assert_eq!(validated.len(), 1);
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn validate_watch_paths_rejects_unreadable_subdirectories() -> TestResult<()> {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir()?;
    let unreadable = dir.path().join("sealed");
    std::fs::create_dir(&unreadable)?;
    std::fs::write(unreadable.join("secret.log"), "nope")?;

    let original_permissions = std::fs::metadata(&unreadable)?.permissions();
    let mut restricted_permissions = original_permissions.clone();
    restricted_permissions.set_mode(0o000);
    std::fs::set_permissions(&unreadable, restricted_permissions)?;

    let mut policy = FileWatchingSecurityPolicy::permissive();
    policy.max_watched_files = Some(10);

    let paths = vec![dir.path().to_string_lossy().into_owned()];
    let error = validate_watch_paths(&paths, &policy)
        .expect_err("unreadable subdirectories must fail honestly");

    std::fs::set_permissions(&unreadable, original_permissions)?;

    assert!(error
        .to_string()
        .contains("Failed to read directory while estimating watched file count"));
    assert!(error
        .to_string()
        .contains(unreadable.to_string_lossy().as_ref()));
    Ok(())
}

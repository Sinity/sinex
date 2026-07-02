use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn deps_update_requires_package_or_all() -> crate::TestResult<()> {
    let error = cargo_update_args(&[], false, false, false, false)
        .expect_err("empty targeted update should be rejected");
    assert!(error.to_string().contains("--package"));
    Ok(())
}

#[sinex_test]
async fn deps_update_builds_targeted_recursive_dry_run_args() -> crate::TestResult<()> {
    let args = cargo_update_args(&["reqwest".to_string()], false, true, true, false)?;
    assert_eq!(
        args,
        ["update", "-p", "reqwest", "--recursive", "--dry-run"]
    );
    Ok(())
}

#[sinex_test]
async fn deps_update_builds_all_lockfile_args() -> crate::TestResult<()> {
    let args = cargo_update_args(&[], false, false, true, true)?;
    assert_eq!(args, ["update", "--dry-run"]);
    Ok(())
}

#[sinex_test]
async fn deps_update_builds_manifest_resolution_args() -> crate::TestResult<()> {
    let args = cargo_update_args(&[], true, false, false, false)?;
    assert_eq!(args, ["metadata", "--format-version=1"]);
    Ok(())
}

#[sinex_test]
async fn deps_update_rejects_mixed_resolution_modes() -> crate::TestResult<()> {
    let error = cargo_update_args(&["reqwest".to_string()], true, false, false, false)
        .expect_err("resolve mode should not accept package update selectors");
    assert!(error.to_string().contains("--resolve"));
    Ok(())
}

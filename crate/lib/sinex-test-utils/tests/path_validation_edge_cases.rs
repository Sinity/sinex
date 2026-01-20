use sinex_test_utils::{path_validation::validate_test_path, sinex_test, TestResult};
use std::fs;
use tempfile::tempdir;

#[sinex_test]
fn rejects_symlink_paths() -> TestResult<()> {
    let tmp = tempdir().expect("tempdir");
    let target = tmp.path().join("target.txt");
    fs::write(&target, "data").unwrap();
    let link = tmp.path().join("link.txt");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &link).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&target, &link).unwrap();

    let result = validate_test_path(link.to_string_lossy().as_ref());
    assert!(result.is_err(), "symlinks should be rejected");
    Ok(())
}

#[sinex_test]
fn accepts_unicode_paths() -> TestResult<()> {
    let tmp = tempdir().expect("tempdir");
    let unicode = tmp.path().join("测试文件.txt");
    fs::write(&unicode, "ok").unwrap();

    let result = validate_test_path(unicode.to_string_lossy().as_ref());
    assert!(result.is_ok(), "unicode paths should be accepted");
    Ok(())
}

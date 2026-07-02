use super::*;

use xtask_macros::*;

#[sinex_test]
async fn test_validate_test_path_accepts_safe_paths() -> TestResult<()> {
    let temp_path = create_test_temp_file("path-validation", "test-file.txt")?;
    assert!(validate_test_path(temp_path.as_str()).is_ok());

    Ok(())
}

#[sinex_test]
async fn test_validate_test_path_rejects_dangerous_paths() -> TestResult<()> {
    // These should be rejected
    let dangerous_paths = [
        "/etc/passwd",
        "/bin/sh",
        "/root/.ssh/authorized_keys",
        "/var/log/system.log",
        "../../../etc/passwd",
        "",
        "/",
    ];

    for path in &dangerous_paths {
        let result = validate_test_path(path);
        assert!(result.is_err(), "Path should be rejected: {path}");
    }

    Ok(())
}

#[sinex_test]
async fn test_create_test_temp_dir() -> TestResult<()> {
    let temp_dir = create_test_temp_dir("path_validation_test")?;

    // Directory should exist
    assert!(temp_dir.exists());
    assert!(temp_dir.is_dir());

    // Should be in temp directory
    let system_temp = env::temp_dir();
    assert!(temp_dir.starts_with(&system_temp));

    // Should contain test identifier
    assert!(temp_dir.as_str().contains("sinex-test"));
    assert!(temp_dir.as_str().contains("path_validation_test"));

    // Clean up
    remove_test_dir(&temp_dir)?;
    assert!(!temp_dir.exists());

    Ok(())
}

#[sinex_test]
async fn test_create_test_temp_file() -> TestResult<()> {
    let temp_file = create_test_temp_file("file_test", "test-data.txt")?;

    // File path should be valid
    assert!(validate_test_path(temp_file.as_str()).is_ok());

    // Parent directory should exist
    assert!(temp_file.parent().unwrap().exists());

    // Should contain sanitized filename
    assert!(temp_file.file_name().unwrap().contains("test-data"));

    // Clean up directory
    if let Some(parent) = temp_file.parent() {
        remove_test_dir(parent)?;
    }

    Ok(())
}

#[sinex_test]
async fn test_sanitize_filename() -> TestResult<()> {
    let test_cases = [
        ("normal_file.txt", "normal_file.txt"),
        ("file/with/slashes.txt", "file_with_slashes.txt"),
        ("file:with:colons.txt", "file_with_colons.txt"),
        ("file\"with\"quotes.txt", "file_with_quotes.txt"),
        ("..dangerous", "dangerous"),
        ("also_dangerous..", "also_dangerous"),
    ];

    for (input, expected) in &test_cases {
        let result = sanitize_filename(input);
        assert_eq!(&result, expected, "Failed to sanitize: {input}");
    }

    Ok(())
}

#[sinex_test]
async fn test_remove_test_dir_safety() -> TestResult<()> {
    // Create a legitimate test directory
    let test_dir = create_test_temp_dir("removal_test")?;

    // Should allow removal of test directory
    assert!(remove_test_dir(&test_dir).is_ok());

    // Should reject removal of system directories
    let system_paths = [
        Utf8Path::new("/etc"),
        Utf8Path::new("/home"),
        Utf8Path::new("/usr"),
    ];

    for path in &system_paths {
        let result = remove_test_dir(path);
        assert!(result.is_err(), "Should reject removal of: {path}");
    }

    Ok(())
}

#[sinex_test]
async fn test_path_depth_validation() -> TestResult<()> {
    // Reasonable depth should be fine
    let reasonable_path = "/tmp/sinex-test/sub1/sub2/sub3/file.txt";
    assert!(validate_test_path(reasonable_path).is_ok());

    // Excessive depth should be rejected
    let deep_path = "/tmp/".to_string() + &"deep/".repeat(25) + "file.txt";
    assert!(validate_test_path(&deep_path).is_err());

    Ok(())
}

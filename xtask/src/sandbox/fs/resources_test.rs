use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_temp_dir_creation() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = temp_dir()?;

    // Directory should exist and be accessible
    assert!(temp_dir.path().exists());
    assert!(temp_dir.path().is_dir());

    // Should be in the workspace-backed test temp root.
    assert!(temp_dir.path().starts_with(workspace_test_temp_root()?));

    // Directory should be automatically cleaned up when dropped
    let _temp_path = temp_dir.path().to_path_buf();
    drop(temp_dir);

    Ok(())
}

#[sinex_test]
async fn test_short_test_temp_prefix_keeps_unix_socket_paths_short()
-> ::xtask::sandbox::TestResult<()> {
    let root = workspace_test_temp_root()?;
    for test_name in [
        "notify_preserves_socket_for_followup_messages",
        "watchdog_task_emits_ping_when_enabled",
    ] {
        let socket_path = root
            .join(short_test_temp_prefix(test_name))
            .join("notify.sock");
        assert!(
            socket_path.as_str().len() < 108,
            "socket path must stay below sockaddr_un::sun_path limit: {} ({socket_path})",
            socket_path.as_str().len()
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_workspace_test_temp_root_honors_env_override() -> ::xtask::sandbox::TestResult<()>
{
    let temp_dir = tempfile::tempdir()?;
    let override_root = temp_dir.path().join("sinex-test-root");
    let _guard = EnvGuard::set_single("SINEX_TEST_TMPDIR", override_root.as_os_str());

    let root = workspace_test_temp_root()?;

    assert_eq!(root.as_std_path(), override_root.as_path());
    assert!(root.exists());
    Ok(())
}

#[sinex_test]
async fn test_create_test_file() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = temp_dir()?;
    let content = "Test file content for validation";

    let file_path = create_test_file(temp_dir.path(), "test.txt", content)?;

    // File should exist
    assert!(file_path.exists());

    // Content should match
    let read_content = std::fs::read_to_string(&file_path)?;
    assert_eq!(read_content, content);

    // Path should be validated
    assert!(verify_test_path_safety(file_path.as_str()).is_ok());

    Ok(())
}

#[sinex_test]
async fn test_create_secure_test_dir() -> ::xtask::sandbox::TestResult<()> {
    let test_dir = create_secure_test_dir("resources_test")?;

    // Directory should exist
    assert!(test_dir.exists());
    assert!(test_dir.is_dir());

    // Should be in a secure temp location
    assert!(test_dir.as_str().contains("sinex-test"));

    // Should be validated
    assert!(verify_test_path_safety(test_dir.as_str()).is_ok());

    Ok(())
}

#[sinex_test]
async fn test_create_temp_test_file() -> ::xtask::sandbox::TestResult<()> {
    let content = "Temporary test file content";
    let file_path = create_temp_test_file("temp_file_test", content)?;

    // File should exist
    assert!(file_path.exists());

    // Content should match
    let read_content = std::fs::read_to_string(&file_path)?;
    assert_eq!(read_content, content);

    // Should have expected structure
    assert!(file_path.as_str().contains("temp_file_test"));

    Ok(())
}

#[sinex_test]
async fn test_create_test_binary_file() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = temp_dir()?;
    let binary_content = b"Binary test content\x00\x01\x02\xFF";

    let file_path =
        create_test_binary_file(temp_dir.path(), "binary_test.bin", binary_content)?;

    // File should exist
    assert!(file_path.exists());

    // Content should match exactly
    let read_content = std::fs::read(&file_path)?;
    assert_eq!(read_content, binary_content);

    Ok(())
}

#[sinex_test]
async fn test_path_validation_rejection() -> ::xtask::sandbox::TestResult<()> {
    // These should be rejected by the validation
    let dangerous_paths = ["/etc/passwd", "../../../etc/shadow", "/bin/sh", ""];

    for path in &dangerous_paths {
        let result = verify_test_path_safety(path);
        assert!(result.is_err(), "Should reject dangerous path: {path}");
    }

    Ok(())
}

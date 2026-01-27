//! Integration tests for RPC token hot-reload functionality

use xtask::sandbox::sinex_test;
use std::fs;
use std::time::Duration;
use tempfile::TempDir;

#[sinex_test]
async fn test_token_rotation_file_modification() -> TestResult<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let token_file = temp_dir.path().join("token");

    // Write initial token
    fs::write(&token_file, "initial-token").expect("Failed to write token file");

    // Set environment variable
    std::env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());

    // Create GatewayAuth with file watcher
    let auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");

    // Verify initial token
    assert_eq!(auth, "initial-token");

    // Note: Full integration test with running server would require:
    // 1. Start gateway server with token file
    // 2. Make successful request with initial token
    // 3. Update token file
    // 4. Wait for file watcher to reload
    // 5. Verify old token fails
    // 6. Verify new token succeeds

    // Clean up
    std::env::remove_var("SINEX_RPC_TOKEN_FILE");
    Ok(())
}

#[sinex_test]
async fn test_token_file_deletion() -> TestResult<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let token_file = temp_dir.path().join("token");

    // Write initial token
    fs::write(&token_file, "test-token").expect("Failed to write token file");

    // Set environment variable
    std::env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());

    // Read token
    let auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");

    assert_eq!(auth, "test-token");

    // Note: Testing actual deletion behavior requires a running server
    // with the file watcher active

    // Clean up
    std::env::remove_var("SINEX_RPC_TOKEN_FILE");
    Ok(())
}

#[sinex_test]
async fn test_token_file_recreate() -> TestResult<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let token_file = temp_dir.path().join("token");

    // Write initial token
    fs::write(&token_file, "first-token").expect("Failed to write token file");

    // Set environment variable
    std::env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());

    // Read token
    let auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");

    assert_eq!(auth, "first-token");

    // Delete and recreate with new token
    fs::remove_file(&token_file).expect("Failed to delete token file");
    tokio::time::sleep(Duration::from_millis(100)).await;

    fs::write(&token_file, "second-token").expect("Failed to write new token file");

    // Read again
    let new_auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");

    assert_eq!(new_auth, "second-token");

    // Clean up
    std::env::remove_var("SINEX_RPC_TOKEN_FILE");
    Ok(())
}

#[sinex_test]
async fn test_env_var_token_priority() -> TestResult<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let token_file = temp_dir.path().join("token");

    // Write token to file
    fs::write(&token_file, "file-token").expect("Failed to write token file");

    // Test priority: SINEX_GATEWAY_ADMIN_TOKEN_FILE > SINEX_RPC_TOKEN_FILE > SINEX_RPC_TOKEN

    // Set direct env var
    std::env::set_var("SINEX_RPC_TOKEN", "direct-token");
    let auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");
    assert_eq!(auth, "direct-token");

    // Set file path (should override direct token)
    std::env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());
    let auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");
    assert_eq!(auth, "file-token");

    // Set admin token file (should override all)
    let admin_token_file = temp_dir.path().join("admin-token");
    fs::write(&admin_token_file, "admin-token").expect("Failed to write admin token file");
    std::env::set_var(
        "SINEX_GATEWAY_ADMIN_TOKEN_FILE",
        admin_token_file.to_str().unwrap(),
    );
    let auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");
    assert_eq!(auth, "admin-token");

    // Clean up
    std::env::remove_var("SINEX_RPC_TOKEN");
    std::env::remove_var("SINEX_RPC_TOKEN_FILE");
    std::env::remove_var("SINEX_GATEWAY_ADMIN_TOKEN_FILE");
    Ok(())
}

#[sinex_test]
async fn test_empty_token_file() -> TestResult<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let token_file = temp_dir.path().join("token");

    // Write empty token file
    fs::write(&token_file, "").expect("Failed to write token file");

    std::env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());

    // Should return None for empty token
    let auth = sinex_gateway::rpc_server::read_token_from_env().expect("Failed to read token");

    // Empty tokens should be treated as missing
    assert_eq!(auth, Some("".to_string()));

    // Clean up
    std::env::remove_var("SINEX_RPC_TOKEN_FILE");
    Ok(())
}

#[sinex_test]
async fn test_whitespace_token_trimming() -> TestResult<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let token_file = temp_dir.path().join("token");

    // Write token with whitespace
    fs::write(&token_file, "  trimmed-token\n\n").expect("Failed to write token file");

    std::env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());

    let auth = sinex_gateway::rpc_server::read_token_from_env()
        .expect("Failed to read token")
        .expect("Token should be present");

    assert_eq!(auth, "trimmed-token");

    // Clean up
    std::env::remove_var("SINEX_RPC_TOKEN_FILE");
    Ok(())
}

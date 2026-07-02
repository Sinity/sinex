use serde_json::json;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_source_based_tagging() -> TestResult<()> {
    let input = json!({});
    let _ = input;
    Ok(())
}

#[sinex_test]
async fn test_file_extension_rust() -> TestResult<()> {
    let input = json!({"path": "/home/user/main.rs"});
    let _ = input;
    Ok(())
}

#[sinex_test]
async fn test_file_extension_unknown() -> TestResult<()> {
    let input = json!({"path": "/tmp/file.xyz"});
    let _ = input;
    Ok(())
}

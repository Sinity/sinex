use color_eyre::eyre::eyre;
use sinexctl::error::{enhance_rpc_error, is_not_found_error};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_not_found_error_detection() -> TestResult<()> {
    let err = eyre!("HTTP 404: Resource not found");
    assert!(is_not_found_error(&err));

    let err = eyre!("Node not found");
    assert!(is_not_found_error(&err));

    let err = eyre!("Connection timeout");
    assert!(!is_not_found_error(&err));
    Ok(())
}

#[sinex_test]
async fn test_enhance_connection_error() -> TestResult<()> {
    let original = eyre!("connection refused");
    let enhanced = enhance_rpc_error("test.method", original);
    let enhanced_str = enhanced.to_string();

    assert!(enhanced_str.contains("Cannot connect to gateway"));
    assert!(enhanced_str.contains("Troubleshooting"));
    assert!(enhanced_str.contains("systemctl status"));
    Ok(())
}

#[sinex_test]
async fn test_enhance_auth_error() -> TestResult<()> {
    let original = eyre!("HTTP 401: Unauthorized");
    let enhanced = enhance_rpc_error("test.method", original);
    let enhanced_str = enhanced.to_string();

    assert!(enhanced_str.contains("Authentication failed"));
    assert!(enhanced_str.contains("SINEX_RPC_TOKEN"));
    Ok(())
}

#[sinex_test]
async fn test_enhance_node_not_found() -> TestResult<()> {
    let original = eyre!("HTTP 404: Node not found");
    let enhanced = enhance_rpc_error("coordination.instance_health", original);
    let enhanced_str = enhanced.to_string();

    assert!(enhanced_str.contains("sinexctl node list"));
    Ok(())
}

#[sinex_test]
async fn test_enhance_operation_not_found() -> TestResult<()> {
    let original = eyre!("Operation not found");
    let enhanced = enhance_rpc_error("ops.get", original);
    let enhanced_str = enhanced.to_string();

    assert!(
        enhanced_str.contains("sinexctl ops list"),
        "Enhanced error does not contain expected text. Got: {enhanced_str}"
    );
    Ok(())
}

use color_eyre::eyre::eyre;
use sinexctl::error::{enhance_rpc_error, format_public_rpc_error_details, is_not_found_error};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_not_found_error_detection() -> TestResult<()> {
    let err = eyre!("HTTP 404: Resource not found");
    assert!(is_not_found_error(&err));

    let err = eyre!("RuntimeModule not found");
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
    assert!(enhanced_str.contains("SINEX_API_TOKEN"));
    Ok(())
}

#[sinex_test]
async fn test_enhance_runtime_not_found() -> TestResult<()> {
    let original = eyre!("HTTP 404: RuntimeModule not found");
    let enhanced = enhance_rpc_error("coordination.instance_health", original);
    let enhanced_str = enhanced.to_string();

    assert!(enhanced_str.contains("sinexctl runtime list"));
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

#[sinex_test]
async fn test_format_public_rpc_error_details_uses_stable_category() -> TestResult<()> {
    let details = format_public_rpc_error_details(&serde_json::json!({
        "error_id": "018f0000-0000-7000-8000-000000000000",
        "kind": "database",
        "kind_name": "database",
        "status_code": 500,
        "context": {
            "operation": "events.query"
        }
    }));

    assert!(details.contains("kind=database"));
    assert!(details.contains("status=500"));
    assert!(details.contains("error_id=018f0000-0000-7000-8000-000000000000"));
    assert!(!details.contains("operation"));
    Ok(())
}

#[sinex_test]
async fn test_format_public_rpc_error_details_preserves_dev_error_payload() -> TestResult<()> {
    let data = serde_json::json!({
        "error_id": "018f0000-0000-7000-8000-000000000000",
        "public": {
            "kind": "database",
            "kind_name": "database",
            "status_code": 500,
            "message": "A database error occurred"
        },
        "error": {
            "type": "Database",
            "details": {
                "message": "SELECT token FROM auth"
            }
        }
    });

    let details = format_public_rpc_error_details(&data);
    assert!(details.contains("\"error\""));
    assert!(details.contains("SELECT token FROM auth"));
    assert!(details.contains("018f0000-0000-7000-8000-000000000000"));
    Ok(())
}

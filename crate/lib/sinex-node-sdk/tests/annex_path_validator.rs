use sinex_node_sdk::annex::path_validator::{create_secure_temp_path, validate_and_convert_path};
use sinex_test_utils::sinex_test;
use sinex_test_utils::TestResult;

#[sinex_test]
fn validate_and_convert_path_enforces_security() -> TestResult<()> {
    let valid_path = validate_and_convert_path("/tmp/test.txt")?;
    assert!(valid_path.to_string().contains("test.txt"));

    assert!(validate_and_convert_path("../../../etc/passwd").is_err());
    assert!(validate_and_convert_path("/path/../../../etc/passwd").is_err());
    Ok(())
}

#[sinex_test]
fn create_secure_temp_path_generates_unique_location() -> TestResult<()> {
    let temp_path = create_secure_temp_path("sinex_blob", "tmp")?;
    assert!(temp_path.to_string().contains("sinex_blob"));
    assert_eq!(temp_path.extension().unwrap_or(""), "tmp");
    assert!(!temp_path.exists());
    Ok(())
}

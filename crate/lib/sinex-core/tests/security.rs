use sinex_core::db::security::SecurityValidator;
use sinex_test_utils::sinex_test;
use sinex_test_utils::TestResult;

#[sinex_test]
async fn path_sanitization_rejects_traversal() -> TestResult<()> {
    assert_eq!(
        SecurityValidator::sanitize_path("/home/user/file.txt").unwrap(),
        "/home/user/file.txt"
    );
    assert!(SecurityValidator::sanitize_path("../../../etc/passwd").is_err());
    assert!(SecurityValidator::sanitize_path("..\\..\\windows\\system32").is_err());
    assert!(SecurityValidator::sanitize_path("%2e%2e%2f%2e%2e%2fetc%2fpasswd").is_err());
    assert!(SecurityValidator::sanitize_path("..%252f..%252fetc%252fpasswd").is_err());
    Ok(())
}

#[sinex_test]
async fn unicode_sanitization_strips_nulls() -> TestResult<()> {
    assert_eq!(
        SecurityValidator::sanitize_unicode("test\0value"),
        "testvalue"
    );
    assert_eq!(
        SecurityValidator::sanitize_unicode("test\u{200B}value"),
        "test\u{200B}value"
    );
    Ok(())
}

#[sinex_test]
async fn json_depth_limits_nested_structures() -> TestResult<()> {
    let shallow = serde_json::json!({"a": {"b": {"c": 1}}});
    assert!(SecurityValidator::check_json_depth(&shallow, 5).is_ok());
    assert!(SecurityValidator::check_json_depth(&shallow, 2).is_err());
    Ok(())
}

#[sinex_test]
async fn json_size_guards_total_elements() -> TestResult<()> {
    let small = serde_json::json!({"a": 1, "b": 2});
    assert!(SecurityValidator::check_json_size(&small, 10).is_ok());
    assert!(SecurityValidator::check_json_size(&small, 2).is_err());
    Ok(())
}

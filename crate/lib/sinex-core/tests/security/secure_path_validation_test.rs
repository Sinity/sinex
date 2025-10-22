//! Core secure-path validation tests that ensure the sanitisation helpers work
//! independently of higher-level services.

use serde_json::json;
use sinex_core::types::validation::{
    deserialize_sanitized_path,
    deserialize_sanitized_path_vec,
    PathValidationLevel,
    SecurePath,
};
use sinex_test_utils::sinex_test;

#[sinex_test]
fn test_secure_path_validation_levels() -> color_eyre::eyre::Result<()> {
    let secure_path = SecurePath::new("/valid/path", PathValidationLevel::Basic)?;
    assert_eq!(secure_path.as_str(), "/valid/path");

    assert!(SecurePath::new("../../../etc/passwd", PathValidationLevel::Basic).is_err());
    assert!(SecurePath::new("/valid/../../../etc/passwd", PathValidationLevel::Strict).is_err());

    assert!(SecurePath::new("/absolute/path", PathValidationLevel::AbsoluteOnly).is_ok());
    assert!(SecurePath::new("relative/path", PathValidationLevel::AbsoluteOnly).is_err());

    assert!(SecurePath::new("relative/path", PathValidationLevel::RelativeOnly).is_ok());
    assert!(SecurePath::new("/absolute/path", PathValidationLevel::RelativeOnly).is_err());

    Ok(())
}

#[sinex_test]
fn test_sanitized_path_deserialization() -> color_eyre::eyre::Result<()> {
    let valid_json = json!("/tmp/valid/path");
    let deserializer = serde_json::from_value::<serde_json::Value>(valid_json).unwrap();
    let path = deserialize_sanitized_path(deserializer);
    assert!(path.is_ok());

    let malicious_json = json!("../../../etc/passwd");
    let deserializer = serde_json::from_value::<serde_json::Value>(malicious_json).unwrap();
    let result = deserialize_sanitized_path(deserializer);
    assert!(result.is_err(), "Path traversal should be rejected");

    let path_vec_json = json!(["/tmp/path1", "/tmp/path2", "../../etc/passwd"]);
    let deserializer = serde_json::from_value::<serde_json::Value>(path_vec_json).unwrap();
    let result = deserialize_sanitized_path_vec(deserializer);
    assert!(result.is_err(), "Vector with malicious path should be rejected");

    let valid_vec_json = json!(["/tmp/path1", "/tmp/path2"]);
    let deserializer = serde_json::from_value::<serde_json::Value>(valid_vec_json).unwrap();
    let result = deserialize_sanitized_path_vec(deserializer);
    assert!(result.is_ok());

    Ok(())
}

#[sinex_test]
fn test_path_length_limits() -> color_eyre::eyre::Result<()> {
    let very_long_path = "/tmp/".to_string() + &"a".repeat(5000);
    let result = SecurePath::new(&very_long_path, PathValidationLevel::Basic);
    assert!(result.is_err(), "Excessively long paths should be rejected");

    Ok(())
}

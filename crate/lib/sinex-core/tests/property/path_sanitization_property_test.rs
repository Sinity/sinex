use proptest::prelude::*;
use serde_json::json;
use sinex_core::db::sanitization::EventSanitizer;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::types::validation::{validate_path, ValidationError};
use sinex_core::{Event, Id, Ulid};
use sinex_test_utils::prelude::*;
use std::path::Path;

/// Property tests for path sanitization and validation functions
///
/// This module tests critical path handling invariants:
/// - Path traversal attacks are properly neutralized
/// - Legitimate paths remain unchanged  
/// - Sanitization is idempotent
/// - No crashes on malformed inputs
/// - Unicode handling is secure

// =============================================================================
// Path Validation Properties
// =============================================================================

/// Generate arbitrary file paths for testing
fn arb_file_path() -> impl Strategy<Value = String> {
    prop_oneof![
        // Normal paths
        "[a-zA-Z0-9_.-]{1,50}/[a-zA-Z0-9_.-]{1,50}",
        "/[a-zA-Z0-9_.-]{1,50}/[a-zA-Z0-9_.-]{1,50}",
        "\\./[a-zA-Z0-9_.-]{1,20}",
        // Relative paths
        "./[a-zA-Z0-9_.-]{1,20}",
        "../[a-zA-Z0-9_.-]{1,20}",
        // Complex paths
        "/home/user/[a-zA-Z0-9_.-]{1,20}/subdir/file\\.[a-zA-Z]{2,4}",
        "/tmp/[a-zA-Z0-9_.-]{1,30}",
    ]
}

/// Generate malicious path traversal attempts
fn arb_malicious_path() -> impl Strategy<Value = String> {
    prop_oneof![
        // Classic path traversal
        Just("../../../etc/passwd".to_string()),
        Just("..\\..\\..\\windows\\system32\\config\\sam".to_string()),
        Just("/path/../../../etc/shadow".to_string()),
        // URL-encoded traversal
        Just("..%2f..%2f..%2fetc%2fpasswd".to_string()),
        Just("%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd".to_string()),
        Just("%252e%252e%252f%252e%252e%252f%252e%252e%252fetc%252fpasswd".to_string()),
        // Null byte injection
        Just("/tmp/safe.txt\0../../../etc/passwd".to_string()),
        Just("test\0\0\0traversal".to_string()),
        // Mixed encodings
        Just("..%c0%af..%c0%af..%c0%afetc%c0%afpasswd".to_string()),
        Just("..%c1%9c..%c1%9c..%c1%9cetc%c1%9cpasswd".to_string()),
        // Windows-style traversal
        Just("..\\..\\..\\etc\\passwd".to_string()),
        Just("C:\\..\\..\\windows\\system32\\drivers\\etc\\hosts".to_string()),
        // Very long traversal chains
        Just("../".repeat(100) + "etc/passwd"),
        Just("..\\".repeat(100) + "windows\\system32"),
    ]
}

/// Generate edge case paths
fn arb_edge_case_path() -> impl Strategy<Value = String> {
    prop_oneof![
        // Empty and minimal
        Just("".to_string()),
        Just(".".to_string()),
        Just("..".to_string()),
        Just("/".to_string()),
        Just("\\".to_string()),
        // Very long paths
        Just("x".repeat(5000)),
        Just("/".to_string() + &"x".repeat(4095)),
        // Special characters
        Just("file with spaces.txt".to_string()),
        Just("file!@#$%^&*()_+.txt".to_string()),
        Just("file[]{}.txt".to_string()),
        // Unicode paths
        Just("файл.txt".to_string()),
        Just("测试文件.txt".to_string()),
        Just("🦀🔒📁.rs".to_string()),
        // Control characters
        Just("file\n.txt".to_string()),
        Just("file\r.txt".to_string()),
        Just("file\t.txt".to_string()),
    ]
}

#[sinex_prop(cases = 1000)]
fn test_validate_path_neutralizes_traversal(
    #[strategy(arb_malicious_path())] malicious_path: String,
) -> TestResult<()> {
    match validate_path(&malicious_path) {
        Ok(validated) => {
            let validated_str = validated.as_str();
            prop_assert!(
                !path_escapes_root(&validated),
                "Validated path should not escape root: {} -> {}",
                malicious_path,
                validated_str
            );
            prop_assert!(
                !validated_str.contains('\0'),
                "Validated path should not contain null bytes: {}",
                validated_str
            );
        }
        Err(_) => {
            // Rejection is acceptable for malicious paths
        }
    }
    Ok(())
}

#[sinex_prop]
fn test_validate_path_preserves_legitimate_paths(
    #[strategy(arb_file_path())] legitimate_path: String,
) -> TestResult<()> {
    if legitimate_path.contains("..") || legitimate_path.contains('\0') {
        return Ok(());
    }

    match validate_path(&legitimate_path) {
        Ok(validated) => {
            if let Some(filename) = Path::new(&legitimate_path).file_name() {
                if let Some(validated_filename) = validated.file_name() {
                    prop_assert_eq!(
                        filename.to_string_lossy(),
                        validated_filename,
                        "Filename should be preserved: {} -> {}",
                        legitimate_path,
                        validated.as_str()
                    );
                }
            }
        }
        Err(e) => {
            prop_assert!(
                matches!(e, ValidationError::Path(_)),
                "Should fail with path error for legitimate path: {} -> {:?}",
                legitimate_path,
                e
            );
        }
    }
    Ok(())
}

#[sinex_prop]
fn test_event_sanitization_is_idempotent(
    #[strategy(prop_oneof![
        arb_file_path(),
        arb_malicious_path(),
        arb_edge_case_path()
    ])]
    path: String,
) -> TestResult<()> {
    let mut event1 = Event::test_event(
        EventSource::new(path.clone()),
        EventType::new("test.event"),
        json!({"test": "data"}),
    );
    event1.id = Some(Id::from_ulid(Ulid::new()));

    let mut event2 = event1.clone();

    let _ = EventSanitizer::sanitize_event(&mut event1).unwrap_or(false);
    let _ = EventSanitizer::sanitize_event(&mut event2).unwrap_or(false);

    let mut event1_copy = event1.clone();
    let was_modified_again = EventSanitizer::sanitize_event(&mut event1_copy).unwrap_or(false);

    prop_assert!(
        !was_modified_again,
        "Second sanitization should not modify already-clean event: {}",
        path
    );
    prop_assert_eq!(
        event1.source,
        event1_copy.source,
        "Source should be stable after sanitization: {}",
        path
    );
    Ok(())
}

#[sinex_prop]
fn test_path_sanitization_removes_dangerous_sequences(
    #[strategy(arb_malicious_path())] malicious_path: String,
) -> TestResult<()> {
    let mut event = Event::test_event(
        EventSource::new(malicious_path.clone()),
        EventType::new("security.test"),
        json!({"path": malicious_path.clone()}),
    );
    event.id = Some(Id::from_ulid(Ulid::new()));

    let _ = EventSanitizer::sanitize_event(&mut event).unwrap_or(false);

    prop_assert!(
        !event.source.contains(".."),
        "Sanitized event source should not contain '..': {} -> {}",
        malicious_path,
        event.source.as_str()
    );
    prop_assert!(
        !event.source.contains('\0'),
        "Sanitized event source should not contain null bytes: {} -> {}",
        malicious_path,
        event.source.as_str()
    );

    if let Some(path_val) = event.payload.get("path").and_then(|v| v.as_str()) {
        prop_assert!(
            !path_val.contains(".."),
            "Sanitized payload path should not contain '..': {} -> {}",
            malicious_path,
            path_val
        );
    }
    Ok(())
}

#[sinex_prop]
fn test_path_validation_handles_unicode_safely(
    #[strategy(proptest::string::string_regex("[\\u{0}-\\u{FFFF}]{1,50}").unwrap())]
    unicode_path: String,
) -> TestResult<()> {
    let result = validate_path(&unicode_path);
    match result {
        Ok(validated) => {
            prop_assert!(
                validated.as_str().len() <= unicode_path.len() + 100,
                "Unicode normalization should not explode size"
            );
        }
        Err(e) => {
            prop_assert!(
                matches!(e, ValidationError::Path(_)),
                "Unicode path rejection should report a path error: {:?}",
                e
            );
        }
    }
    Ok(())
}

#[sinex_prop]
fn test_safe_content_preservation_in_events(
    #[strategy(proptest::string::string_regex("[a-zA-Z0-9_. /-]{1,100}").unwrap())]
    safe_string: String,
) -> TestResult<()> {
    let mut event = Event::test_event(
        EventSource::new(safe_string.clone()),
        EventType::new("safe.test"),
        json!({"content": safe_string.clone()}),
    );
    event.id = Some(Id::from_ulid(Ulid::new()));

    let original_alphanum: String = safe_string
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();

    let _ = EventSanitizer::sanitize_event(&mut event).unwrap_or(false);

    let sanitized_source_alphanum: String = event
        .source
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect();

    prop_assert_eq!(
        original_alphanum,
        sanitized_source_alphanum,
        "Alphanumeric characters should be preserved in source: '{}' -> '{}'",
        safe_string,
        event.source.as_str()
    );
    Ok(())
}

#[sinex_prop]
fn test_path_length_limits_enforced(
    #[strategy(1usize..10000usize)] path_length: usize,
) -> TestResult<()> {
    let long_path = "a".repeat(path_length);
    let result = validate_path(&long_path);

    if path_length > 4096 {
        prop_assert!(
            result.is_err(),
            "Path of length {} should be rejected",
            path_length
        );
        if let Err(e) = result {
            prop_assert!(
                matches!(e, ValidationError::Path(_)),
                "Should fail with path validation error for length {}",
                path_length
            );
        }
    } else {
        match result {
            Ok(validated) => {
                prop_assert!(
                    validated.as_str().len() <= 4096,
                    "Validated path should respect length limits"
                );
            }
            Err(e) => {
                prop_assert!(
                    matches!(e, ValidationError::Path(_)),
                    "Unexpected error type for length {}: {:?}",
                    path_length,
                    e
                );
            }
        }
    }
    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Check if a path would escape the intended root directory
fn path_escapes_root(path: &camino::Utf8Path) -> bool {
    let mut depth = 0i32;

    for component in path.components() {
        match component {
            camino::Utf8Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return true; // Escaped root
                }
            }
            camino::Utf8Component::Normal(_) => depth += 1,
            camino::Utf8Component::RootDir => depth = 0,
            _ => {}
        }
    }

    false
}

// =============================================================================
// Unit Tests for Property Test Helpers
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;
    use proptest::strategy::ValueTree;

    #[sinex_test]
    fn test_path_escape_detection() -> TestResult {
        // Test the helper function itself
        assert!(path_escapes_root(&camino::Utf8Path::new(
            "../../etc/passwd"
        )));
        assert!(path_escapes_root(&camino::Utf8Path::new("../../../root")));
        assert!(!path_escapes_root(&camino::Utf8Path::new(
            "/home/user/file.txt"
        )));
        assert!(!path_escapes_root(&camino::Utf8Path::new(
            "./local/file.txt"
        )));
        assert!(!path_escapes_root(&camino::Utf8Path::new("relative/path")));

        Ok(())
    }

    #[sinex_test]
    fn test_path_generators() -> TestResult {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test malicious path generator
        let malicious = arb_malicious_path()
            .new_tree(&mut runner)
            .unwrap()
            .current();
        assert!(!malicious.is_empty());

        // Test file path generator
        let file_path = arb_file_path().new_tree(&mut runner).unwrap().current();
        assert!(!file_path.is_empty());

        // Test edge case generator
        let edge_case = arb_edge_case_path()
            .new_tree(&mut runner)
            .unwrap()
            .current();
        // Edge cases can be empty, but they should stay within documented bounds
        assert!(edge_case.len() <= 5000);

        Ok(())
    }
}

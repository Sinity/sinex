use proptest::prelude::*;
use sinex_core::db::sanitization::EventSanitizer;
use sinex_core::types::validation::{validate_path, ValidationError};
use sinex_test_utils::prelude::*;
use std::path::{Path, PathBuf};

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

#[sinex_test]
fn test_validate_path_neutralizes_traversal() -> color_eyre::eyre::Result<()> {
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        fn property_validate_path_neutralizes_traversal(
            malicious_path in arb_malicious_path()
        ) {
            // Property: Malicious paths should either be rejected or neutralized
            match validate_path(&malicious_path) {
                Ok(validated) => {
                    // If accepted, should not contain dangerous sequences
                    let validated_str = validated.as_str();
                    
                    // Should not have effective parent directory traversal
                    prop_assert!(
                        !path_escapes_root(&validated), 
                        "Validated path should not escape root: {} -> {}", 
                        malicious_path, validated_str
                    );
                    
                    // Should not contain null bytes
                    prop_assert!(
                        !validated_str.contains('\0'), 
                        "Validated path should not contain null bytes: {}", 
                        validated_str
                    );
                }
                Err(_) => {
                    // Rejection is acceptable for malicious paths
                    prop_assert!(true);
                }
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_validate_path_preserves_legitimate_paths() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_validate_path_preserves_legitimate_paths(
            legitimate_path in arb_file_path()
        ) {
            // Property: Legitimate paths should remain functionally equivalent
            if !legitimate_path.contains("..") && !legitimate_path.contains('\0') {
                match validate_path(&legitimate_path) {
                    Ok(validated) => {
                        // Should preserve essential path structure
                        if let Some(filename) = Path::new(&legitimate_path).file_name() {
                            if let Some(validated_filename) = validated.file_name() {
                                prop_assert_eq!(
                                    filename.to_string_lossy(), 
                                    validated_filename,
                                    "Filename should be preserved: {} -> {}", 
                                    legitimate_path, validated.as_str()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        // Some complex legitimate paths might be rejected - that's acceptable
                        // for security, but the error should be reasonable
                        prop_assert!(
                            matches!(e, ValidationError::Path(_)), 
                            "Should fail with path error for legitimate path: {} -> {:?}", 
                            legitimate_path, e
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[sinex_test] 
fn test_path_sanitization_is_idempotent() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_path_sanitization_is_idempotent(
            path in prop_oneof![arb_file_path(), arb_malicious_path(), arb_edge_case_path()]
        ) {
            // Property: Sanitizing twice should yield the same result
            let sanitized_once = EventSanitizer::sanitize_path_traversal(&path);
            let sanitized_twice = EventSanitizer::sanitize_path_traversal(&sanitized_once);
            
            prop_assert_eq!(
                sanitized_once, sanitized_twice,
                "Path sanitization should be idempotent: {} -> {} -> {}",
                path, sanitized_once, sanitized_twice
            );
        }
    }
    Ok(())
}

#[sinex_test]
fn test_path_sanitization_removes_dangerous_sequences() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_path_sanitization_removes_dangerous_sequences(
            malicious_path in arb_malicious_path()
        ) {
            // Property: Sanitized paths should not contain known dangerous patterns
            let sanitized = EventSanitizer::sanitize_path_traversal(&malicious_path);
            
            // Should not contain effective ".." sequences
            prop_assert!(
                !sanitized.contains(".."),
                "Sanitized path should not contain '..': {} -> {}",
                malicious_path, sanitized
            );
            
            // Should not contain encoded traversal
            prop_assert!(
                !sanitized.contains("%2e%2e") && !sanitized.contains("%252e%252e"),
                "Sanitized path should not contain encoded '..' sequences: {} -> {}",
                malicious_path, sanitized
            );
            
            // Should not contain null bytes
            prop_assert!(
                !sanitized.contains('\0'),
                "Sanitized path should not contain null bytes: {} -> {}",
                malicious_path, sanitized
            );
        }
    }
    Ok(())
}

#[sinex_test]
fn test_path_validation_handles_unicode_safely() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_path_validation_handles_unicode_safely(
            unicode_path in "[\\u{0}-\\u{FFFF}]{1,50}"
        ) {
            // Property: Unicode paths should be handled without crashes
            let result = validate_path(&unicode_path);
            
            // Should not panic
            match result {
                Ok(validated) => {
                    // Valid unicode should be preserved or safely normalized
                    prop_assert!(validated.as_str().len() <= unicode_path.len() + 100); // Allow for normalization
                }
                Err(_) => {
                    // Rejection is fine for problematic unicode
                    prop_assert!(true);
                }
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_conservative_sanitization_preserves_safe_content() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_conservative_sanitization_preserves_safe_content(
            safe_string in "[a-zA-Z0-9_. /-]{1,100}"
        ) {
            // Property: Safe ASCII content should be mostly preserved
            let sanitized = EventSanitizer::sanitize_string_conservative(&safe_string);
            
            // Should contain the same alphanumeric characters
            let original_alphanum: String = safe_string.chars()
                .filter(|c| c.is_ascii_alphanumeric()).collect();
            let sanitized_alphanum: String = sanitized.chars()
                .filter(|c| c.is_ascii_alphanumeric()).collect();
                
            prop_assert_eq!(
                original_alphanum, sanitized_alphanum,
                "Alphanumeric characters should be preserved: '{}' -> '{}'",
                safe_string, sanitized
            );
        }
    }
    Ok(())
}

#[sinex_test]
fn test_path_length_limits_enforced() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_path_length_limits_enforced(
            path_length in 1usize..10000usize
        ) {
            // Property: Very long paths should be rejected
            let long_path = "a".repeat(path_length);
            let result = validate_path(&long_path);
            
            if path_length > 4096 {
                // Should be rejected for being too long
                prop_assert!(
                    result.is_err(),
                    "Path of length {} should be rejected", path_length
                );
                if let Err(e) = result {
                    prop_assert!(
                        matches!(e, ValidationError::Path(_)),
                        "Should fail with path validation error for length {}", path_length
                    );
                }
            } else {
                // Should be acceptable if within limits
                match result {
                    Ok(validated) => {
                        prop_assert!(
                            validated.as_str().len() <= 4096,
                            "Validated path should respect length limits"
                        );
                    }
                    Err(_) => {
                        // May still fail for other reasons - that's fine
                        prop_assert!(true);
                    }
                }
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

    #[sinex_test]
    fn test_path_escape_detection() -> color_eyre::eyre::Result<()> {
        // Test the helper function itself
        assert!(path_escapes_root(&camino::Utf8Path::new("../../etc/passwd")));
        assert!(path_escapes_root(&camino::Utf8Path::new("../../../root")));
        assert!(!path_escapes_root(&camino::Utf8Path::new("/home/user/file.txt")));
        assert!(!path_escapes_root(&camino::Utf8Path::new("./local/file.txt")));
        assert!(!path_escapes_root(&camino::Utf8Path::new("relative/path")));
        
        Ok(())
    }

    #[sinex_test]
    fn test_path_generators() -> color_eyre::eyre::Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        
        // Test malicious path generator
        let malicious = arb_malicious_path().new_tree(&mut runner).unwrap().current();
        assert!(!malicious.is_empty());
        
        // Test file path generator  
        let file_path = arb_file_path().new_tree(&mut runner).unwrap().current();
        assert!(!file_path.is_empty());
        
        // Test edge case generator
        let edge_case = arb_edge_case_path().new_tree(&mut runner).unwrap().current();
        // Edge cases can be empty, so just verify it doesn't crash
        assert!(true);
        
        Ok(())
    }
}
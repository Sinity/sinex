//! Fuzz target for JSON validation and security checks.
//!
//! Tests functions that protect against JSON-based attacks:
//! - `validate_json` - Size and depth validation
//! - `check_json_depth` - Stack overflow prevention
//! - `check_json_size` - Memory exhaustion prevention
//! - `check_json_expansion` - Billion laughs prevention

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

/// Arbitrary input for JSON fuzzing.
#[derive(Debug, Arbitrary)]
struct JsonInput {
    /// Raw JSON string to parse and validate
    json_str: String,
    /// Max depth for depth checking
    max_depth: u8,
    /// Max size for size checking
    max_size: u16,
}

/// Generate deeply nested JSON for stress testing.
fn generate_nested_json(depth: usize) -> String {
    if depth == 0 {
        return r#"{"leaf": true}"#.to_string();
    }
    format!(r#"{{"nested": {}}}"#, generate_nested_json(depth - 1))
}

/// Generate wide JSON with many keys.
fn generate_wide_json(width: usize) -> String {
    let entries: Vec<String> = (0..width).map(|i| format!(r#""key{}": {}"#, i, i)).collect();
    format!("{{{}}}", entries.join(", "))
}

fuzz_target!(|input: JsonInput| {
    // Test validate_json with arbitrary string
    let _ = sinex_core::types::validation::validate_json(&input.json_str);

    // If we can parse it as JSON, run more targeted tests
    if let Ok(value) = serde_json::from_str::<Value>(&input.json_str) {
        // Test check_json_depth with varying limits
        let max_depth = (input.max_depth as usize).max(1);
        let _ =
            sinex_core::db::security::SecurityValidator::check_json_depth(&value, max_depth);

        // Test check_json_size with varying limits
        let max_size = (input.max_size as usize).max(1);
        let _ =
            sinex_core::db::security::SecurityValidator::check_json_size(&value, max_size);

        // Test check_json_expansion
        let _ = sinex_core::types::validation::check_json_expansion(&value);

        // Test validate_json_value
        let _ = sinex_core::types::validation::validate_json_value(&value);
    }

    // Test with generated deeply nested JSON
    if input.max_depth > 0 && input.max_depth < 50 {
        let nested = generate_nested_json(input.max_depth as usize);
        let _ = sinex_core::types::validation::validate_json(&nested);
    }

    // Test with generated wide JSON
    if input.max_size > 0 && input.max_size < 2000 {
        let wide = generate_wide_json(input.max_size as usize);
        let _ = sinex_core::types::validation::validate_json(&wide);
    }
});

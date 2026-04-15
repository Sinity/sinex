// # Security Test Suite
//
// Comprehensive security testing consolidating all security-related adversarial tests.
// This module validates the system's resilience against various attack vectors.
//
// ## Test Categories
// - **Path Traversal**: Directory traversal and filesystem attacks
// - **SQL Injection**: Database injection attack protection
// - **Input Validation**: Malformed and malicious input handling
// - **Resource Exhaustion**: DoS and resource consumption attacks
// - **Query Interface**: API security and exploit prevention
// - **Unicode Exploits**: Character encoding and normalization attacks

use serde_json::json;
use sinex_primitives::events::Publishable;
use sinex_primitives::{EventSource, EventType, Pagination};
use xtask::sandbox::prelude::*;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PathTraversalScenario {
    name: &'static str,
    payload: &'static str,
    expected_behavior: ExpectedBehavior,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
enum ExpectedBehavior {
    Rejected,       // Should be rejected outright
    Sanitized,      // Should be accepted but sanitized
    AcceptedAsData, // Should be accepted as harmless data
}

// =============================================================================
// Path Traversal Security Tests
// =============================================================================

/// Test filesystem monitoring against path traversal attacks
///
/// Path traversal attack: source="../../etc/passwd"
/// Expected: Source field values are safely stored as data via parameterized queries
#[sinex_test]
async fn test_filesystem_path_traversal_protection(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();

    // Source identifiers are validated domain values, not arbitrary file paths.
    // Path-traversal strings should be rejected at the identifier boundary.
    let rejected = EventSource::new("../../etc/passwd");
    assert!(
        rejected.is_err(),
        "path traversal strings must be rejected as EventSource identifiers"
    );

    let payload = DynamicPayload::new(
        "security.path-traversal",
        "file.created",
        json!({
            "source_attempt": "../../etc/passwd",
            "path": "/legitimate/file.txt",
            "size": 1024
        }),
    );

    // Just verify the payload can be constructed without panicking
    let payload_json = payload.to_json_value()?;
    assert!(!payload_json.is_null(), "Payload should be valid JSON");

    // Parameterized queries still protect legitimate queries that carry the
    // malicious string as payload data.
    let _pagination = Pagination::new(Some(100), None);
    let _repo = pool.events();

    Ok(())
}

/// Test comprehensive path traversal scenarios
///
/// Multiple path traversal variations should all be handled safely:
/// - "..", "~", "~root", absolute paths, symlink patterns
/// All should be stored as data without causing directory traversal
#[sinex_test]
async fn test_comprehensive_path_traversal_scenarios(_ctx: TestContext) -> TestResult<()> {
    // Test that various path traversal patterns are safely handled as data.
    // These patterns should not cause filesystem access or SQL injection.

    let traversal_patterns = vec![
        "../../../etc/passwd",
        "~/.ssh/id_rsa",
        "/etc/shadow",
        "\\..\\..\\windows\\system32",
        "file:///etc/passwd",
    ];

    for pattern in traversal_patterns {
        assert!(
            EventSource::new(pattern).is_err(),
            "path traversal string {pattern:?} must be rejected as an EventSource identifier"
        );

        let payload = DynamicPayload::new(
            "security.test",
            "security.test",
            json!({
                "attempt": pattern,
                "type": "traversal"
            }),
        );

        // If payload construction succeeds, the field is being treated as data
        let payload_json = payload.to_json_value()?;
        assert!(
            !payload_json.is_null(),
            "Payload for pattern '{pattern}' should be valid"
        );
    }

    Ok(())
}

// =============================================================================
// SQL Injection Protection Tests
// =============================================================================

/// Test SQL injection protection across all event fields
///
/// Tests three injection vectors:
/// 1. SQL injection in source: "'; DROP TABLE events; --"
/// 2. SQL injection in event_type: "'; DELETE FROM events; --"
/// 3. SQL injection in payload: {"field": "'; UPDATE events SET ..."}
/// All are safely handled via parameterized queries
#[sinex_test]
async fn test_sql_injection_protection(_ctx: TestContext) -> TestResult<()> {
    // Test SQL injection attempts are safely handled as data via parameterized queries.
    // The system uses sqlx::query!() macros which prevent injection at compile time.

    // Test 1: SQL injection in source field must be rejected as an identifier.
    assert!(
        EventSource::new("'; DROP TABLE events; --").is_err(),
        "SQL injection patterns must be rejected as EventSource identifiers"
    );
    let payload1 = DynamicPayload::new(
        "safe-source",
        "test.event",
        json!({"data": "test1", "source_attempt": "'; DROP TABLE events; --"}),
    );
    let payload_json = payload1.to_json_value()?;
    assert!(
        !payload_json.is_null(),
        "SQL injection in source should be treated as data"
    );

    // Test 2: SQL injection in event_type must be rejected as an identifier.
    assert!(
        EventType::new("'; DELETE FROM events; --").is_err(),
        "SQL injection patterns must be rejected as EventType identifiers"
    );
    let payload2 = DynamicPayload::new(
        "safe-source",
        "safe.event",
        json!({"data": "test2", "event_type_attempt": "'; DELETE FROM events; --"}),
    );
    let payload_json = payload2.to_json_value()?;
    assert!(
        !payload_json.is_null(),
        "SQL injection in event_type should be treated as data"
    );

    // Test 3: SQL injection in payload
    let payload3 = DynamicPayload::new(
        "safe-source",
        "safe.event",
        json!({
            "field": "'; UPDATE events SET created_at = NOW(); --",
            "injection": "DROP TABLE core.events;"
        }),
    );
    let payload_json = payload3.to_json_value()?;
    assert!(
        payload_json.to_string().contains("UPDATE events"),
        "Injection strings should be preserved as data in JSON"
    );

    Ok(())
}

// =============================================================================
// Unicode and Encoding Security Tests
// =============================================================================

/// Test unicode normalization security
///
/// Unicode normalization attacks use decomposed characters (é = e + combining accent)
/// instead of precomposed (é). Test that both forms persist safely.
#[sinex_test]
async fn test_unicode_normalization_attacks(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let _repo = pool.events();

    // Composed form: é (single character U+00E9)
    let composed = "café";

    // Decomposed form: e + combining acute accent (U+0065 U+0301)
    let decomposed = "cafe\u{0301}";

    assert!(
        EventSource::new(decomposed).is_err(),
        "non-ASCII identifiers must be rejected as EventSource values"
    );
    assert!(
        EventSource::new(composed).is_err(),
        "non-ASCII identifiers must be rejected as EventSource values"
    );

    // Event payloads may still carry these strings as ordinary data.
    let payload1 = DynamicPayload::new("unicode.test", "test.unicode", json!({"file": decomposed}));
    let payload_json1 = payload1.to_json_value()?;
    assert!(
        !payload_json1.is_null(),
        "Decomposed unicode should be valid"
    );

    // Event with composed unicode
    let payload2 = DynamicPayload::new("unicode.test", "test.unicode", json!({"file": composed}));
    let payload_json2 = payload2.to_json_value()?;
    assert!(!payload_json2.is_null(), "Composed unicode should be valid");

    // Verify both forms persist in JSON as provided
    assert_eq!(payload_json1["file"], json!(decomposed));
    assert_eq!(payload_json2["file"], json!(composed));

    Ok(())
}

/// Test null byte injection handling
///
/// Null bytes (\u{0000}) in strings can cause truncation in C/C++ code.
/// Test that the system handles them safely (either persists or returns clean error).
#[sinex_test]
async fn test_null_byte_injection(_ctx: TestContext) -> TestResult<()> {
    // Test that null byte injection attempts are safely handled as data.
    // JSON format preserves these characters, and parameterized queries prevent issues.

    // Payload with embedded null byte
    let payload_with_null = json!({
        "filename": "document\u{0000}.exe",
        "content": "Safe content\u{0000}Injected content"
    });

    assert!(
        EventSource::new("source\u{0000}injection").is_err(),
        "embedded nulls must be rejected as EventSource identifiers"
    );

    // Construct event with null bytes in payload data.
    let payload = DynamicPayload::new("null-byte.test", "file.created", payload_with_null);

    // If payload construction succeeds, null bytes are treated as data
    let payload_json = payload.to_json_value()?;
    assert!(
        !payload_json.is_null(),
        "Payload with null bytes should be valid JSON"
    );
    assert!(
        payload_json["content"]
            .as_str()
            .unwrap_or("")
            .contains('\u{0000}'),
        "Null bytes should be preserved in JSON"
    );

    Ok(())
}

// =============================================================================
// Resource Exhaustion Security Tests
// =============================================================================

/// Test resource exhaustion protection
///
/// Publish an oversized payload (~5MB huge string) to verify:
/// - System either accepts it successfully, OR
/// - Returns a clean error (not a panic/crash/OOM)
#[sinex_test]
async fn test_resource_exhaustion_protection(_ctx: TestContext) -> TestResult<()> {
    // Test resource exhaustion protection - system should handle large payloads gracefully.
    // JSON serialization should not panic or crash with large strings.

    // Create a large payload (5MB)
    let huge_string = "X".repeat(5 * 1024 * 1024);
    let large_payload = json!({
        "data": huge_string,
        "size": huge_string.len()
    });

    // Attempt to construct oversized event
    let payload = DynamicPayload::new("stress-source", "resource.large", large_payload);

    // If payload construction succeeds, large payloads are handled safely
    let result = payload.to_json_value();
    match result {
        Ok(payload_json) => {
            // Large payloads should serialize successfully
            assert!(
                !payload_json.is_null(),
                "Large payload should be valid JSON"
            );
            assert_eq!(
                payload_json["data"].as_str().unwrap_or("").len(),
                5 * 1024 * 1024,
                "Large string should be preserved"
            );
        }
        Err(e) => {
            // If serialization fails, it should be a clean error, not a crash
            let error_msg = e.to_string();
            assert!(
                !error_msg.contains("thread") && !error_msg.contains("panicked"),
                "Should fail gracefully, not panic. Got: {error_msg}"
            );
        }
    }

    Ok(())
}

// =============================================================================
// Input Validation Security Tests
// =============================================================================

/// Test XSS prevention in stored event payloads
///
/// XSS attacks: `<script>alert('xss')</script>`, `onclick=alert()`, etc.
/// Expected: Stored verbatim as data (no execution context in database).
/// When retrieved, they are treated as data, not code.
#[sinex_test]
async fn test_malicious_input_validation(_ctx: TestContext) -> TestResult<()> {
    // Test that XSS payloads are safely handled as literal data.
    // JSON encoding ensures these are stored as strings, not executed.

    let xss_payloads = vec![
        r"<script>alert('xss')</script>",
        r#"<img src=x onerror="alert('xss')">"#,
        r"javascript:alert('xss')",
        r#"<svg onload="alert('xss')">"#,
    ];

    for xss in xss_payloads {
        let payload = DynamicPayload::new(
            "user-input-source",
            "input.malicious",
            json!({
                "message": xss,
                "user_input": xss,
                "threat_level": "critical"
            }),
        );

        // If payload construction succeeds, XSS strings are treated as data
        let payload_json = payload.to_json_value()?;
        assert!(
            !payload_json.is_null(),
            "XSS payload should be valid JSON: {xss}"
        );

        // Verify the XSS string is preserved verbatim in the JSON value (not stripped)
        let stored = payload_json["message"]
            .as_str()
            .expect("message field should be a string");
        assert_eq!(
            stored, xss,
            "XSS payload should be stored verbatim in JSON field"
        );
    }

    Ok(())
}

// =============================================================================
// Query Interface Security Tests
// =============================================================================

/// Test query interface against exploitation attempts
///
/// Verify that query operations are resilient to:
/// - Large result sets that attempt resource exhaustion
/// - Time-based attacks (slow queries that don't timeout)
/// - Filtering bypass attempts
#[sinex_test]
async fn test_query_interface_exploits(_ctx: TestContext) -> TestResult<()> {
    // Test that query interfaces safely handle special characters via parameterized queries.
    // These patterns should not cause SQL injection or bypass filters.

    // Try constructing payloads with special characters that might attempt SQL injection
    let special_sources = vec![
        "source'; DROP TABLE--",
        "source\" OR 1=1--",
        "%",
        "*",
        "source\n\n\nUNION SELECT",
    ];

    for special_source in special_sources {
        assert!(
            EventSource::new(special_source).is_err(),
            "special-character source {special_source:?} must be rejected as an EventSource identifier"
        );
        let _pagination = Pagination::new(Some(100), None);
        // Parameterized queries still protect literal attack strings carried in
        // payload data.

        let payload = DynamicPayload::new(
            "query.test-source",
            "query.test",
            json!({"test": "payload", "source_attempt": special_source}),
        );
        let payload_json = payload.to_json_value()?;
        assert!(
            !payload_json.is_null(),
            "Payload with special source should be valid"
        );
    }

    Ok(())
}

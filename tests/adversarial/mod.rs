// Adversarial testing module
//
// This module contains comprehensive security and edge case tests designed
// to stress-test the Sinex system under adverse conditions. Tests include
// security attacks, resource exhaustion, race conditions, and boundary cases.
//
// # Test Categories
// - **Security Tests**: SQL injection, privilege escalation, input validation
// - **Resource Exhaustion**: Memory, CPU, disk, network limits
// - **Race Conditions**: Concurrent access, timing attacks
// - **Boundary Cases**: Large payloads, edge values, invalid data
// - **Network Issues**: Distributed system edge cases
// - **State Violations**: Invalid state transitions

use serde_json::json;
use sinex_types::events::RawEvent;

#[allow(dead_code)]
// Boundary tests for system limits
pub mod boundary_test;

// Concurrency and race condition tests
pub mod concurrency_test;

// ULID edge case and boundary testing
pub mod ulid_edge_cases_test;

// Other adversarial tests have been consolidated or are being migrated

/// Common utilities for adversarial testing
pub mod utils {
    use serde_json::json;
    use sinex_types::events::RawEvent;

    /// Create malicious payload for testing
    pub fn create_malicious_payload(attack_type: &str) -> serde_json::Value {
        match attack_type {
            "sql_injection" => json!({
                "user_input": "'; DROP TABLE core.events; --",
                "data": "<script>alert('xss')</script>"
            }),
            "large_payload" => {
                let large_data = "x".repeat(10_000_000); // 10MB
                json!({ "data": large_data })
            }
            "deeply_nested" => create_deeply_nested_json(100),
            "unicode_attack" => json!({
                "data": "\u{0000}\u{0001}\u{0002}\u{0003}\u{FEFF}\u{FFFE}\u{FFFF}"
            }),
            _ => json!({ "generic_attack": true }),
        }
    }

    /// Create deeply nested JSON for stack overflow tests
    fn create_deeply_nested_json(depth: usize) -> serde_json::Value {
        if depth == 0 {
            json!("bottom")
        } else {
            json!({ "level": depth, "nested": create_deeply_nested_json(depth - 1) })
        }
    }

    /// Generate test data with specific characteristics
    /// Note: This function has been simplified due to test infrastructure changes
    pub fn generate_adversarial_events(count: usize, attack_pattern: &str) -> Vec<serde_json::Value> {
        (0..count)
            .map(|_i| {
                json!({
                    "source": "adversarial_test",
                    "event_type": format!("{}.attack", attack_pattern),
                    "payload": create_malicious_payload(attack_pattern),
                    "version": "attack_v1.0"
                })
            })
            .collect()
    }
}

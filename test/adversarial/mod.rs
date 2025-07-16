//! Adversarial testing module
//!
//! This module contains comprehensive security and edge case tests designed
//! to stress-test the Sinex system under adverse conditions. Tests include
//! security attacks, resource exhaustion, race conditions, and boundary cases.

#![allow(dead_code)]
//!
//! # Test Categories
//! - **Security Tests**: SQL injection, privilege escalation, input validation
//! - **Resource Exhaustion**: Memory, CPU, disk, network limits
//! - **Race Conditions**: Concurrent access, timing attacks
//! - **Boundary Cases**: Large payloads, edge values, invalid data
//! - **Network Issues**: Distributed system edge cases
//! - **State Violations**: Invalid state transitions

// Boundary tests for system limits
pub mod boundary_test;

// Concurrency and race condition tests
pub mod concurrency_test;

// Other adversarial tests have been consolidated or are being migrated

/// Common utilities for adversarial testing
pub mod utils {
    use crate::common::prelude::*;

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
    pub fn generate_adversarial_events(count: usize, attack_pattern: &str) -> Vec<RawEvent> {
        (0..count)
            .map(|_i| {
                crate::common::events::generic_adversarial_event(
                    "adversarial_test",
                    &format!("{}.attack", attack_pattern),
                    create_malicious_payload(attack_pattern),
                    Some("attack_v1.0"),
                )
            })
            .collect()
    }
}

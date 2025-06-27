//! Adversarial testing module
//!
//! This module contains comprehensive security and edge case tests designed
//! to stress-test the Sinex system under adverse conditions. Tests include
//! security attacks, resource exhaustion, race conditions, and boundary cases.
//!
//! # Test Categories
//! - **Security Tests**: SQL injection, privilege escalation, input validation
//! - **Resource Exhaustion**: Memory, CPU, disk, network limits
//! - **Race Conditions**: Concurrent access, timing attacks
//! - **Boundary Cases**: Large payloads, edge values, invalid data
//! - **Network Issues**: Distributed system edge cases
//! - **State Violations**: Invalid state transitions

// Comprehensive security test that consolidates all attack scenarios
pub mod comprehensive_security_test;

// Time and ULID-based attacks
pub mod time_ulid_attacks_test;

// Database boundary and stress tests
pub mod database_boundary_test;

// General security attack vectors
pub mod security_attacks_test;

// Concurrency and race condition tests
pub mod race_conditions_test;

// Resource exhaustion scenarios
pub mod resource_exhaustion_test;

// Agent lifecycle chaos testing
pub mod agent_lifecycle_chaos_test;

// Configuration reload attack vectors
pub mod config_reload_attacks_test;

// Filesystem edge cases and boundary tests
pub mod filesystem_edge_cases_test;

// Advanced time-based attack patterns
pub mod advanced_time_attacks_test;

// Sophisticated JSON payload attacks
pub mod sophisticated_json_attacks_test;

// State machine violation tests
pub mod state_machine_violations_test;

// Network and distributed system issues
pub mod network_distributed_issues_test;

// Query interface exploitation tests
pub mod query_interface_exploits_test;

// Worker coordination and synchronization tests
pub mod worker_coordination_test;

// Numeric overflow and boundary bugs
pub mod numeric_overflow_bugs_test;

// Realistic security threat simulations
pub mod realistic_security_threats_test;

// Input validation and sanitization tests
pub mod input_validation_test;

/// Common utilities for adversarial testing
pub mod utils {
    use crate::common::prelude::*;

    /// Create malicious payload for testing
    pub fn create_malicious_payload(attack_type: &str) -> serde_json::Value {
        match attack_type {
            "sql_injection" => json!({
                "user_input": "'; DROP TABLE raw.events; --",
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

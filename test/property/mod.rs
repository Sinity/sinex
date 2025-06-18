//! Property-based tests using proptest
//!
//! These tests use proptest to verify properties that should hold across
//! a wide range of inputs, providing more comprehensive testing than
//! example-based tests.

pub mod ulid_ordering_property_tests;
pub mod work_queue_property_tests;
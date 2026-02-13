//! Test-only event payloads for infrastructure testing.
//!
//! These replace DynamicPayload in tests where the payload content
//! doesn't matter but typed validation should still happen.

#![cfg(any(test, feature = "testing"))]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

/// Generic test event — use when you need any event and don't care about the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "test", event_type = "test.event")]
pub struct TestEventPayload {
    pub value: String,
}

impl TestEventPayload {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn default_val() -> Self {
        Self {
            value: "test".into(),
        }
    }
}

/// Test event from source alpha — for multi-source routing tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "test.alpha", event_type = "test.alpha_event")]
pub struct TestAlphaPayload {
    pub value: String,
}

impl TestAlphaPayload {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

/// Test event from source beta — for multi-source routing tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "test.beta", event_type = "test.beta_event")]
pub struct TestBetaPayload {
    pub value: String,
}

impl TestBetaPayload {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

/// Test event with a numeric value — for ordering/aggregation tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "test", event_type = "test.numeric")]
pub struct TestNumericPayload {
    pub value: i64,
    pub label: String,
}

impl TestNumericPayload {
    pub fn new(value: i64, label: impl Into<String>) -> Self {
        Self {
            value,
            label: label.into(),
        }
    }
}

/// Test event with arbitrary JSON data — for schema/payload tests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "test", event_type = "test.structured")]
pub struct TestStructuredPayload {
    pub data: serde_json::Value,
    pub tag: String,
}

impl TestStructuredPayload {
    pub fn new(data: serde_json::Value, tag: impl Into<String>) -> Self {
        Self {
            data,
            tag: tag.into(),
        }
    }
}

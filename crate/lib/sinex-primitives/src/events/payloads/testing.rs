//! Test-only event payloads for infrastructure testing.
//!
//! These replace `DynamicPayload` in tests where the payload content
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

    #[must_use]
    pub fn default_val() -> Self {
        Self {
            value: "test".into(),
        }
    }
}


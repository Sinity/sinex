//! Test-only event payloads for infrastructure testing.
//!
//! These replace `DynamicPayload` in tests where the payload content
//! doesn't matter but typed validation should still happen.

#![cfg(any(test, feature = "testing"))]

// Currently empty — test payloads removed as unused.
// Add test-only payloads here when test infrastructure needs
// typed validation without depending on production payloads.

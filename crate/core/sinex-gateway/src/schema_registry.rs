//! Static (source, `event_type`) registry built from the `EventPayload`
//! inventory at startup (#1172, AC-4 — "schema-as-code").
//!
//! Every event-emitting RPC must validate the `(source, event_type)` pair
//! against this registry before persisting an event. Unknown pairs are
//! rejected with a `SinexError::validation` rather than reaching the DB and
//! being routed to DLQ.

use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::events::schema_registry::get_all_payloads;
use std::collections::HashSet;
use std::sync::OnceLock;

/// In-memory registry of every `(source, event_type)` pair declared via
/// `#[derive(EventPayload)]` and collected by `inventory::collect!`.
///
/// Constructed lazily on first access (typically gateway startup) and never
/// rebuilt. The inventory is build-time — adding new payloads requires a
/// rebuild, not a hot reload.
#[derive(Debug)]
pub struct SchemaRegistry {
    pairs: HashSet<(String, String)>,
}

impl SchemaRegistry {
    /// Build the registry by walking the `EventPayload` inventory.
    #[must_use]
    pub fn from_inventory() -> Self {
        let pairs = get_all_payloads()
            .map(|info| (info.source.to_string(), info.event_type.to_string()))
            .collect();
        Self { pairs }
    }

    /// Look up a `(source, event_type)` pair.
    #[must_use]
    pub fn contains(&self, source: &str, event_type: &str) -> bool {
        self.pairs
            .contains(&(source.to_string(), event_type.to_string()))
    }

    /// Number of registered pairs (mostly for diagnostics / startup logs).
    #[must_use]
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    /// Whether the inventory is empty (should never be true in production).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Validate a typed `(EventSource, EventType)` pair, returning a typed
    /// error suitable for direct propagation from RPC handlers.
    pub fn validate(&self, source: &EventSource, event_type: &EventType) -> Result<()> {
        if self.contains(source.as_str(), event_type.as_str()) {
            Ok(())
        } else {
            Err(
                SinexError::validation("unknown event (source, event_type) pair")
                    .with_context("source", source.as_str())
                    .with_context("event_type", event_type.as_str()),
            )
        }
    }
}

/// Process-wide registry initialised on first call.
pub fn registry() -> &'static SchemaRegistry {
    static REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();
    REGISTRY.get_or_init(SchemaRegistry::from_inventory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn registry_includes_known_payloads() -> TestResult<()> {
        let reg = registry();
        // The inventory is sizeable in this workspace; a non-zero population
        // is the only durable invariant we can pin at this layer without
        // hard-coding a moving target.
        assert!(!reg.is_empty(), "schema registry should be non-empty");
        Ok(())
    }

    #[sinex_test]
    async fn unknown_pair_rejected() -> TestResult<()> {
        let reg = registry();
        let err = reg
            .validate(
                &EventSource::from_static("__definitely_not_a_real_source__"),
                &EventType::from_static("__nope__"),
            )
            .expect_err("unknown pair should be rejected");
        assert!(
            err.to_string().contains("unknown event"),
            "expected validation error, got {err}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn known_gateway_pair_accepted() -> TestResult<()> {
        let reg = registry();
        // GatewayRequestStatsPayload is registered as
        // (sinex.gateway, request.stats); skip if the workspace surface ever
        // changes underneath us, but keep the assertion strict otherwise.
        assert!(
            reg.contains("sinex.gateway", "request.stats"),
            "registry must include sinex.gateway/request.stats"
        );
        Ok(())
    }
}

//! Transport semantics catalog for NATS publish paths.
//!
//! See [`docs/transport.md`](../../docs/transport.md) for the full rationale,
//! drain protocol, and DLQ / processing-failure / local-recovery-spool
//! boundary documentation.
//!
//! # Quick reference
//!
//! | Class | Subject pattern | On local failure | Drain on SIGTERM |
//! |---|---|---|---|
//! | [`Class::Critical`] | `{env}.sinex.events.raw.>` | retry → raw-ingest DLQ | wait for in-flight ACKs |
//! | [`Class::Derived`] | `{env}.sinex.events.raw.>` (same lane) | retry → processing-failure stream | wait for in-flight ACKs |
//! | [`Class::SourceMaterial`] | `{env}.source_material.frames.>` | retry caller operation | wait for ACKs before event anchors publish |
//! | [`Class::Confirmation`] | `{env}.events.confirmations.>` | retry with backoff → durability-gap warn | best-effort flush |
//! | [`Class::Invalidation`] | `{env}.sinex.derived.invalidation` | JetStream-backed; best-effort warn | best-effort flush |
//! | [`Class::Control`] | `{env}.sinex.control.>` / request-reply | timeout error | drop pending |
//! | [`Class::Telemetry`] | `{env}.sinex.events.raw.>` (telemetry lane) | drop with warn | best-effort flush |

use crate::nats::{NATS_TRAFFIC_CLASS_HEADER, NatsTrafficClass, insert_traffic_class_header};

/// Header used to carry the semantic [`Class`] beside the wire traffic class.
///
/// The wire-level traffic class intentionally has fewer variants than the
/// semantic catalog. This header preserves publisher intent for observability,
/// replay diagnostics, and future policy checks.
pub const SINEX_TRANSPORT_CLASS_HEADER: &str = "Sinex-Transport-Class";

/// Canonical publish-class catalog.
///
/// Every NATS publish site in the workspace declares one of these classes.
/// The class determines QoS, retry budget, failure routing, and drain
/// behavior on SIGTERM / NixOS service restart / test shutdown.
///
/// ## Class semantics
///
/// ### [`Class::Critical`] — provenance-bearing event payloads
///
/// Raw event batches from ingestors. These are the ground-truth records.
/// Loss means lost provenance history.
///
/// - **Subject pattern**: `{env}.sinex.events.raw.{source}.{event_type}`
/// - **QoS**: JetStream with `Nats-Msg-Id` idempotency header; `AckAll` after
///   ack from server.
/// - **Retry budget**: semaphore-bounded (100 permits); on NATS error, caller
///   retries up to the node's configured retry limit.
/// - **Failure routing**: permanent NATS failure → local recovery spool
///   (`sinex_event_recovery_spool.jsonl`); at-most once per-run the spool
///   is replayed on next startup.
/// - **Drain on SIGTERM**: wait for all in-flight JetStream ack futures before
///   shutting down (bounded by `DEFAULT_PUBLISH_ACK_TIMEOUT`).
///
/// ### [`Class::Derived`] — automaton synthesis outputs
///
/// Synthesis events produced by derived nodes. Same subject plane as critical
/// but semantically distinct: a derived event can be replayed from its parents
/// if lost; a critical event cannot be replayed without its source material.
///
/// - **Subject pattern**: `{env}.sinex.events.raw.{source}.{event_type}`
/// - **QoS**: JetStream with `Nats-Msg-Id` idempotency header.
/// - **Retry budget**: semaphore-bounded (100 permits); exhausted retries →
///   processing-failure stream.
/// - **Failure routing**: `events.processing_failures.{node}.{event_id}` —
///   **not** the raw-ingest DLQ. Derived failures are re-runnable; raw-ingest
///   DLQ is operator-reviewed.
/// - **Drain on SIGTERM**: wait for in-flight ACKs; checkpoint saved before
///   exit.
///
/// ### [`Class::SourceMaterial`] — ordered material lifecycle frames
///
/// Begin/slice/end frames that make material provenance replayable. Event
/// anchors depend on these frames reaching the material assembler.
///
/// - **Subject pattern**: `{env}.source_material.frames.*`
/// - **QoS**: JetStream, ordered stream, slice idempotency headers.
/// - **Retry budget**: caller operation propagates publish failure and retries
///   according to the node's material acquisition policy.
/// - **Failure routing**: no raw-event DLQ; material acquisition fails before
///   dependent events can be truthfully published.
/// - **Drain on SIGTERM**: wait for ACKs before considering anchors durable.
///
/// ### [`Class::Confirmation`] — persistence acknowledgement signals
///
/// Per-event ACK signals from ingestd to derived-node adapters. Loss causes
/// duplicate processing (not data loss); automata re-check against DB state.
///
/// - **Subject pattern**: `{env}.events.confirmations.{event_id}`
/// - **QoS**: JetStream with idempotency header; best-effort semantics.
/// - **Retry budget**: up to 3 attempts with exponential backoff; exhausted
///   retries → durable retry queue (`events.confirmation_retries.*`) or
///   durability-gap warning.
/// - **Failure routing**: confirmation durability-gap → warn log + counter;
///   no DLQ (confirmations are re-derivable from DB).
/// - **Drain on SIGTERM**: flush remaining confirmations; if flush fails, log
///   durability-gap warning. Event data is safe (already persisted).
///
/// ### [`Class::Invalidation`] — scope invalidation signals
///
/// Fan-out signals to derived nodes when persisted facts change (replay,
/// archival). JetStream-backed; consumers have durable subscriptions.
///
/// - **Subject pattern**: `{env}.sinex.derived.invalidation`
/// - **QoS**: JetStream (stream: `{BASE}_DERIVED_INVALIDATIONS`).
/// - **Retry budget**: JetStream guarantees delivery to active consumers.
///   Publish failures → error log; the operation that caused the invalidation
///   must decide whether to abort or continue.
/// - **Failure routing**: JetStream provides delivery; the publish caller
///   propagates errors up to the replay/archive operation.
/// - **Drain on SIGTERM**: no special drain needed — JetStream holds messages
///   for offline consumers.
///
/// ### [`Class::Control`] — lifecycle and coordination traffic
///
/// Request-reply coordination: leadership handoff, heartbeat ready-signals,
/// scan commands, replay control responses.
///
/// - **Subject pattern**: `{env}.sinex.control.nodes.{id}.*`,
///   `{env}.sinex.control.replay.progress.{op}`, direct request-reply
///   subjects.
/// - **QoS**: Core NATS (not JetStream); at-most-once. Request-reply with
///   timeout (`REPLAY_CONTROL_SUBSCRIBE_ATTEMPT_TIMEOUT`).
/// - **Retry budget**: none — timeouts surface as errors to the caller.
/// - **Failure routing**: timeout or NATS error → returned as `SinexError`;
///   caller decides (abort, retry at application level, or log and continue).
/// - **Drain on SIGTERM**: drop pending; control messages are idempotent or
///   recoverable at application level.
///
/// ### [`Class::Telemetry`] — self-observation events
///
/// Internal metrics, health, and operational data emitted by components.
/// Loss is acceptable; gaps in telemetry do not affect correctness.
///
/// - **Subject pattern**: `{env}.sinex.events.raw.sinex.{metric_type}` (same
///   raw-event plane, separate semaphore lane).
/// - **QoS**: JetStream with idempotency header; smaller semaphore budget
///   (16 permits) so telemetry cannot crowd out critical traffic.
/// - **Retry budget**: none — `emit_*` methods are fire-and-observe; failures
///   are logged at warn level and dropped.
/// - **Failure routing**: drop with warn log.
/// - **Drain on SIGTERM**: best-effort flush; no wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Provenance-bearing raw event payloads from ingestors.
    /// Loss = lost provenance history. Failure routes to local recovery spool.
    Critical,

    /// Synthesis events produced by automata.
    /// Loss is recoverable via replay. Failure routes to processing-failure stream.
    Derived,

    /// Source-material lifecycle frames.
    /// Loss breaks material provenance and must fail the acquisition operation.
    SourceMaterial,

    /// Persistence acknowledgement signals from ingestd to derived nodes.
    /// Loss causes duplicate processing; best-effort with retry queue.
    Confirmation,

    /// Scope invalidation fan-out to derived nodes.
    /// JetStream-backed; delivery guaranteed to active consumers.
    Invalidation,

    /// Lifecycle and coordination traffic (handoff, scan, replay control).
    /// Request-reply; at-most-once; timeout error on failure.
    Control,

    /// Self-observation metrics and health events.
    /// Loss acceptable; drop with warn on failure.
    Telemetry,
}

impl Class {
    /// Return the [`NatsTrafficClass`] header value for this publish class.
    ///
    /// `NatsTrafficClass` is the wire-level enum, matching the existing
    /// header taxonomy. `Class` is the semantic catalog (including
    /// `Confirmation` and `Invalidation` which share the
    /// `Control` wire class). This mapping is the authoritative bridge.
    #[must_use]
    pub const fn traffic_class(self) -> NatsTrafficClass {
        match self {
            Self::Critical => NatsTrafficClass::RawEvent,
            Self::Derived => NatsTrafficClass::RawEvent,
            Self::SourceMaterial => NatsTrafficClass::SourceMaterial,
            Self::Confirmation => NatsTrafficClass::Control,
            Self::Invalidation => NatsTrafficClass::Control,
            Self::Control => NatsTrafficClass::Control,
            Self::Telemetry => NatsTrafficClass::Telemetry,
        }
    }

    /// Human-readable label for logging and diagnostics.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Derived => "derived",
            Self::SourceMaterial => "source_material",
            Self::Confirmation => "confirmation",
            Self::Invalidation => "invalidation",
            Self::Control => "control",
            Self::Telemetry => "telemetry",
        }
    }

    /// Whether loss of a publish in this class is data-loss (as opposed to
    /// recoverable or acceptable).
    ///
    /// `true` → every publish must ultimately succeed or be durably spooled.
    /// `false` → loss is recoverable (derived, confirmation) or acceptable
    /// (telemetry), or the caller handles failure externally (control,
    /// invalidation).
    #[must_use]
    pub const fn is_loss_critical(self) -> bool {
        matches!(self, Self::Critical | Self::SourceMaterial)
    }

    /// Whether this class uses JetStream (as opposed to core NATS).
    #[must_use]
    pub const fn uses_jetstream(self) -> bool {
        !matches!(self, Self::Control)
    }

    /// Drain behavior description on SIGTERM / NixOS service restart.
    ///
    /// This is documentation-as-code. The actual drain implementation lives in
    /// the publishing component; this method names the contract.
    #[must_use]
    pub const fn drain_behavior(self) -> &'static str {
        match self {
            Self::Critical => "wait for all in-flight JetStream ACK futures before exit",
            Self::Derived => "wait for in-flight ACKs; save checkpoint before exit",
            Self::SourceMaterial => "wait for ACKs before dependent event anchors publish",
            Self::Confirmation => {
                "best-effort flush; log durability-gap warning on remaining failures"
            }
            Self::Invalidation => "no special drain — JetStream holds for offline consumers",
            Self::Control => "drop pending; control messages are idempotent or app-recoverable",
            Self::Telemetry => "best-effort flush; no wait",
        }
    }

    /// Failure routing description when the publish cannot be completed.
    #[must_use]
    pub const fn failure_routing(self) -> &'static str {
        match self {
            Self::Critical => {
                "local recovery spool (sinex_event_recovery_spool.jsonl) in node work dir"
            }
            Self::Derived => {
                "processing-failure stream (events.processing_failures.{node}.{event_id})"
            }
            Self::SourceMaterial => "material acquisition operation fails before event publish",
            Self::Confirmation => {
                "durable retry queue (events.confirmation_retries.*) or durability-gap warn"
            }
            Self::Invalidation => {
                "error propagated to caller (replay/archive operation decides abort/continue)"
            }
            Self::Control => "error returned to caller (SinexError::network)",
            Self::Telemetry => "drop with warn log",
        }
    }
}

/// Insert only the semantic transport class header.
///
/// Use this when a publish family has a wire-level traffic class that is more
/// specific than the semantic class, such as derived processing failures.
pub fn insert_semantic_transport_class_header(headers: &mut async_nats::HeaderMap, class: Class) {
    headers.insert(SINEX_TRANSPORT_CLASS_HEADER, class.label());
}

/// Insert both the wire traffic class and semantic transport class headers.
pub fn insert_transport_class_headers(headers: &mut async_nats::HeaderMap, class: Class) {
    insert_traffic_class_header(headers, class.traffic_class());
    insert_semantic_transport_class_header(headers, class);
}

/// Whether a header map carries both canonical transport policy headers.
#[must_use]
pub fn has_transport_class_headers(headers: &async_nats::HeaderMap) -> bool {
    headers.get(NATS_TRAFFIC_CLASS_HEADER).is_some()
        && headers.get(SINEX_TRANSPORT_CLASS_HEADER).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_headers_include_wire_and_semantic_classes() {
        let mut headers = async_nats::HeaderMap::new();
        insert_transport_class_headers(&mut headers, Class::SourceMaterial);

        assert!(has_transport_class_headers(&headers));
        assert_eq!(
            headers
                .get(NATS_TRAFFIC_CLASS_HEADER)
                .map(std::string::ToString::to_string)
                .as_deref(),
            Some("source_material")
        );
        assert_eq!(
            headers
                .get(SINEX_TRANSPORT_CLASS_HEADER)
                .map(std::string::ToString::to_string)
                .as_deref(),
            Some("source_material")
        );
    }
}

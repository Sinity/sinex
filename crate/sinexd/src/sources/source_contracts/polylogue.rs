//! Polylogue bridge consumer — `integration.polylogue` source family (#1122).
//!
//! ## Architecture
//!
//! The Polylogue daemon is an **external producer**: it publishes
//! [`EventIntent`] envelopes directly to NATS `JetStream` without
//! depending on the sinex Rust runtime. event_engine picks them up on the standard
//! `{env}.sinex.events.raw.>` stream just like any other source.
//!
//! This module provides:
//! - A source contract for the canonical
//!   [`sinex_primitives::events::payloads::PolylogueSessionIndexedPayload`]
//!   schema Polylogue publishes.
//! - [`register_source_contract!`] and [`register_source_runtime_binding!`] entries
//!   so the source appears in the catalog and in `sinexctl sources list`.
//!
//! There is **no** `register_source!` or `register_source!`
//! here. The Polylogue daemon is the producer; sinexd does not
//! need to run a consumer process for this source. The NixOS module
//! option `sinex.sources.polylogue.enable` (default `false`) gates a future
//! companion service that may perform post-admission enrichment.
//!
//! ## NATS subjects
//!
//! | Subject | Description |
//! |---------|-------------|
//! | `{env}.sinex.events.raw.integration.polylogue.session_indexed` | Session indexed |
//!
//! ## Payload schema
//!
//! | Field | Type | Required | Description |
//! |-------|------|----------|-------------|
//! | `session_id` | string | yes | Stable polylogue session ID |
//! | `origin` | string | yes | AI provider origin (`claude`, `chatgpt`, `codex`, …) |
//! | `title` | string? | no | Session title if present |
//! | `tags` | string[] | yes | User-applied and auto-inferred tags |
//! | `content_hash` | string | yes | SHA-256 hex of canonical session content |
//! | `created_at` | RFC 3339 | yes | When the session was created |
//! | `updated_at` | RFC 3339 | yes | When the session was last updated |
//! | `message_count` | int | yes | Total message count |
//! | `cost_usd` | number? | no | Estimated cost in USD if available |
//! | `model_slug` | string? | no | Primary model used (e.g. `claude-opus-4-5`) |
//!
//! ## Privacy tier
//!
//! **Sensitive** — tags and titles can reflect personal context even though
//! raw session text is not included.
//!
//! ## Occurrence identity
//!
//! `(content_hash, session_id)` — the content hash detects changed
//! sessions; the `session_id` provides the stable external key.

use sinex_macros::SourceMeta;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};

// ─────────────────────────────────────────────────────────────────────────────
// Source contract + binding
// ─────────────────────────────────────────────────────────────────────────────

/// Polylogue daemon external-producer metadata.
///
/// `factory = "none"` is load-bearing: sinexd must not register a parser or
/// source factory for this source because the Polylogue daemon publishes
/// admitted envelopes directly to NATS.
#[derive(SourceMeta)]
#[source_meta(
    id = "integration.polylogue",
    namespace = "integration",
    event_type = "integration.polylogue.session_indexed",
    event_source = "integration.polylogue",
    adapter = "ExternalProducer",
    implementation = "polylogue-daemon",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous, Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(content_hash, session_id)"),
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::External,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::Continuous,
    factory = "none"
)]
pub struct PolylogueExternalProducer;

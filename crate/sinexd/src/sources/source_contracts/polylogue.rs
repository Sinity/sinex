//! Polylogue bridge consumer — `integration.polylogue` source family (#1122).
//!
//! ## Architecture
//!
//! The Polylogue daemon is an **external producer**: it publishes
//! [`EventIntent`] envelopes directly to NATS `JetStream` without
//! depending on the sinex Rust SDK. event_engine picks them up on the standard
//! `{env}.sinex.events.raw.>` stream just like any other source.
//!
//! This module provides:
//! - A source contract for the canonical
//!   [`sinex_primitives::events::payloads::PolylogueConversationIndexedPayload`]
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
//! | `{env}.sinex.events.raw.integration.polylogue.conversation_indexed` | Conversation indexed |
//!
//! ## Payload schema
//!
//! | Field | Type | Required | Description |
//! |-------|------|----------|-------------|
//! | `conversation_id` | string | yes | Stable polylogue conversation ID |
//! | `provider` | string | yes | AI provider (`claude`, `chatgpt`, `codex`, …) |
//! | `title` | string? | no | Conversation title if present |
//! | `tags` | string[] | yes | User-applied and auto-inferred tags |
//! | `content_hash` | string | yes | SHA-256 hex of canonical conversation content |
//! | `created_at` | RFC 3339 | yes | When the conversation was created |
//! | `updated_at` | RFC 3339 | yes | When the conversation was last updated |
//! | `message_count` | int | yes | Total message count |
//! | `cost_usd` | number? | no | Estimated cost in USD if available |
//! | `model_slug` | string? | no | Primary model used (e.g. `claude-opus-4-5`) |
//!
//! ## Privacy tier
//!
//! **Sensitive** — tags and titles can reflect personal context even though
//! raw conversation text is not included.
//!
//! ## Occurrence identity
//!
//! `(content_hash, conversation_id)` — the content hash detects changed
//! conversations; the `conversation_id` provides the stable external key.

use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// ─────────────────────────────────────────────────────────────────────────────
// Source contract + binding
// ─────────────────────────────────────────────────────────────────────────────

register_source_contract! {
    SourceContract {
        id: "integration.polylogue",
        namespace: "integration",
        event_types: &[("integration.polylogue", "integration.polylogue.conversation_indexed")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(content_hash, conversation_id)"),
        access_policy: "personal_ai_conversations",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:integration.polylogue"),
        "integration.polylogue",
        "integration",
    )
    .implementation("polylogue-daemon")
    .adapter("ExternalProducer")
    .output_event_type("integration.polylogue.conversation_indexed")
    .privacy_context("Document")
    .material_policy("external_producer_virtual_material")
    .checkpoint_policy("external_producer")
    .resource_shape("nats_publisher")
    .source_id("integration.polylogue")
    .runner_pack("external")
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("integration_polylogue_source")
    .implementation_mode("external:polylogue-daemon")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

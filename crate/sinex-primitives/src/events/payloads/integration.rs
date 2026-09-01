//! Cross-product integration event payloads.
//!
//! Polylogue observations carry only typed identity and evidence coordinates.
//! Provider-native and normalized transcript bytes remain in registered source
//! materials and are never copied into an event payload.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

macro_rules! polylogue_observation {
    ($(#[$meta:meta])* $name:ident, $event_type:literal) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
        #[event_payload(
            source = "integration.polylogue",
            event_type = $event_type
        )]
        pub struct $name {
            pub protocol_version: String,
            pub semantics_version: u16,
            pub manifest_digest: String,
            pub revision_id: String,
            pub session_id: String,
            pub origin: String,
            pub native_id: String,
            pub record_id: String,
            pub record_kind: String,
            pub material_id: String,
            pub segment_index: i32,
            pub line_index: u64,
            pub seq: u64,
            pub record_sha256: String,
        }
    };
}

polylogue_observation!(
    /// A session summary record observed in a verified Polylogue revision.
    PolylogueSessionObservedPayload,
    "integration.polylogue.session.observed"
);
polylogue_observation!(
    /// A typed session-topology relation observed in a verified revision.
    PolylogueLineageObservedPayload,
    "integration.polylogue.lineage.observed"
);
polylogue_observation!(
    /// A provider-usage record observed in a verified revision.
    PolylogueUsageObservedPayload,
    "integration.polylogue.usage.observed"
);
polylogue_observation!(
    /// A normalized message record observed in a verified revision.
    PolylogueMessageObservedPayload,
    "integration.polylogue.message.observed"
);
polylogue_observation!(
    /// A normalized message-block record observed in a verified revision.
    PolylogueBlockObservedPayload,
    "integration.polylogue.block.observed"
);
polylogue_observation!(
    /// An attachment reference observed in a verified revision.
    PolylogueAttachmentObservedPayload,
    "integration.polylogue.attachment.observed"
);
polylogue_observation!(
    /// A session lifecycle event observed in a verified revision.
    PolylogueSessionEventObservedPayload,
    "integration.polylogue.session_event.observed"
);

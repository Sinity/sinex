//! Sinex's shared types. Every other crate speaks in these: identifiers, the
//! three clocks, record locations, equivalence keys, typed payloads, events and
//! the batches producers hand to admission. Nothing here does IO.

// The derives in `sinex-macros` name `::sinex_model`; this lets them work here too.
extern crate self as sinex_model;

pub mod batch;
pub mod clock;
pub mod event;
pub mod event_types;
pub mod id;
pub mod json_identity;
pub mod kinds;
pub mod manifest;
pub mod media_effects;
pub mod model_calls;
pub mod payload;
pub mod privacy;
pub mod query;
pub mod source;
pub mod time_settings;

pub use batch::{
    ArchiveReason, ArchiveRequest, Batch, BatchKind, DerivationHistory, FrameRefusal, IngressFrame,
    InputRef, MaterialRef, ObservationFacts, OwnerCoverage, ReaderMemory, RecordOutcome,
    ReplayScope, RevisionChange, SourceProgress, SourceRef, WrappedDataKey,
};
pub use clock::{TimeQuality, Timestamp, TsOrig};
pub use event::{BuildError, Event, Origin, ReadingOrder, RecordLocation, RecordState};
pub use id::{EventId, InputId, MaterialId, ObservationId, OperationId, RevisionId, RunId};
pub use kinds::{FieldHint, KindDeclaration, KindRef, Search, Validation};
pub use manifest::{Manifest, MaterialRole, Member};
pub use payload::{
    DbCheck, EquivalenceKey, EventPayload, KeyKind, OwnedBlob, Payloads, ReplaceRule,
};
pub use privacy::Class;
pub use sinex_macros::{DbCheck, EventPayload};
pub use source::{ProjectionCatalog, SourceDeclaration};

/// Re-exports the derives' generated code relies on.
#[doc(hidden)]
pub mod __private {
    pub use schemars;
    pub use serde_json;
}

mod input_requirements;
pub use input_requirements::{InputRequirement, InputUnavailableReason, UnavailableInput};

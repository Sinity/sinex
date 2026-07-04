//! Admission, persistence, and confirmation.
//!
//! The event engine is the sole writer to `core.events`. It consumes
//! admitted event intents from NATS, validates them, persists them through
//! the `sinex_db` repository layer, and publishes confirmation events back
//! to NATS so derived automata and SSE subscribers can react.

pub mod admission;
pub mod config;
pub mod jetstream_consumer;
pub mod material_assembler;
pub mod material_ready_set;
pub mod policy;
pub mod prelude;
pub mod schema_sync;
pub mod service;
pub mod validator;

pub use admission::{
    AdmissionBatchPlan, AdmissionDecision, AdmissionPersistResult, AdmissionRejection,
    AdmissionRejectionKind, AdmissionService, AdmittedEvent, CandidateEvent,
    CandidateEventMetadata,
};
pub use config::EventEngineConfig;
pub use jetstream_consumer::JetStreamConsumer;
pub use material_assembler::MaterialAssembler;
pub use material_ready_set::MaterialReadySet;
pub use service::IngestService;
pub use sinex_db::repositories::schema_management::SchemaSyncResult;
pub use sinex_db::validation::SchemaInfo;
pub use sinex_primitives::error::{Result, Result as EventEngineResult, SinexError};
pub use sinex_primitives::nats::{JetStreamEventLane, JetStreamTopology};
pub use validator::{IngestEventValidator, ValidationResult};

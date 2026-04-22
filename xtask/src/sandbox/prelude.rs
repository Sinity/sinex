pub use color_eyre::eyre::{Error, Result, WrapErr, bail, ensure, eyre};
pub use futures::future::BoxFuture;
pub use serde_json::{Value as JsonValue, json};
pub use sinex_db::{DbPool, DbPoolExt};
pub use sinex_primitives::prelude::*;
pub use sinex_primitives::{
    DynamicPayload, Event, EventSource, EventType, Id, SinexError, Timestamp, Uuid,
};
pub use std::sync::LazyLock as Lazy;

pub type EventId = Id<Event>;
pub use sqlx::Postgres;
pub use std::sync::Arc;
pub use std::time::Duration;
pub use tokio::time::sleep;

// Proptest re-exports
pub use proptest::prelude::*;

// Macro re-exports
pub use xtask_macros::{sinex_bench, sinex_prop, sinex_proptest, sinex_serial_test, sinex_test};

pub use super::assertions::EventAssert;
pub use super::context::{Sandbox, SandboxFailureSnapshot};
pub use super::db::cleanup_config::{CleanupConfig, CleanupMethod, TableCleanupStrategy};
pub use super::db::{reset_database, verify_clean_state};
pub use super::evidence::{
    DbEvidenceSummary, DirectoryEvidenceSummary, EVIDENCE_SCHEMA_VERSION, EvidenceArtifactRef,
    EvidenceBundle, EvidenceCapture, EvidenceCaptureLevel, EvidenceCollectorKind,
    EvidenceCollectorRegistration, EvidenceCollectorStatus, EvidenceRuntimeSnapshot,
    EvidenceTimelineEvent, FileEvidenceSummary, LogEvidenceSummary, NatsConsumerEvidence,
    NatsEvidenceSummary, NatsStreamEvidence, ProofMetadata, SourceMaterialEvidenceRow,
    TestEvidence,
};
pub use super::fs::{EnvGuard, TestTempEnv, prepare_test_temp_env};
pub use super::nats::{EphemeralNats, EphemeralNatsBuilder, TlsConfig};
pub use super::orchestrator::{
    TestIngestdConfig, TestIngestdHandle, start_test_ingestd_with_config,
};
pub use super::preflight::*;
pub use super::timing::{Timeouts, TimingUtils, WaitHelpers};

// Pipeline coordination
pub use super::coordination::{PipelineNamespace, PipelineScope};
pub use super::events::EventPublisher;
pub use super::nats::EventOverrides;

// Full-stack test fixture
pub use super::stack::{TEST_RPC_TOKEN, TestCoreStack};

// Chaos testing re-exports
pub use super::chaos::{
    ChaosContext, ChaosEventProcessor, ChaosEventResult, ChaosMetrics, ChaosMetricsSnapshot,
    ChaosScenarios, ChaosTestBuilder,
};

// Type aliases
pub type TestContext = Sandbox;
pub use super::TestResult;

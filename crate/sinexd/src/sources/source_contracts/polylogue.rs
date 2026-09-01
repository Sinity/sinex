//! Polylogue external-producer contract for material-backed observations.
//!
//! Polylogue stages exact provider and normalized material before publishing
//! content-free `EventIntent` children. The event payloads identify the
//! verified revision and its exact segment/line anchor.

use sinex_macros::SourceMeta;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};

/// Polylogue is an external producer. Its seven event types are declared on
/// one source contract so the catalog has one authority and one recovery lane.
#[derive(SourceMeta)]
#[source_meta(
    id = "integration.polylogue",
    namespace = "integration",
    event_type = "integration.polylogue.session.observed",
    event_types = "integration.polylogue.lineage.observed, integration.polylogue.usage.observed, integration.polylogue.message.observed, integration.polylogue.block.observed, integration.polylogue.attachment.observed, integration.polylogue.session_event.observed",
    event_source = "integration.polylogue",
    adapter = "ExternalProducer",
    implementation = "polylogue-daemon",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous, Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(revision_id, record_id)"),
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::External,
    checkpoint_family = CheckpointFamily::LiveObservation,
    runtime_shape = RuntimeShape::Continuous,
    recovery_policy = sinex_primitives::source_contracts::SourceRecoveryPolicy::EXTERNAL_PRODUCER,
    factory = "none"
)]
pub struct PolylogueExternalProducer;

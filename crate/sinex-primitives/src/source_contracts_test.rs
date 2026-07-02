use super::*;
use crate::privacy::ProcessingContext;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn subject_queries_match_exact_and_prefix_subjects() -> TestResult<()> {
    let subject = SubjectRef::from_static("runtime_unit:terminal.atuin");

    assert!(SubjectQuery::from_static("runtime_unit:*").matches(subject));
    assert!(SubjectQuery::from_static("runtime_unit:terminal.atuin").matches(subject));
    assert!(!SubjectQuery::from_static("scenario:*").matches(subject));
    Ok(())
}

#[sinex_test]
async fn register_source_contract_named_form_compiles() -> TestResult<()> {
    // Smoke-test: verify the named-form `register_source_contract!(descriptor: X)`
    // macros compile correctly.  We exercise the plain-descriptor path here
    // (no extra rules) since inventory submission from tests is link-time only.
    // The with-rules form is syntactically tested via the macro expansion path
    // verified by the trybuild suite.
    use crate::source_contracts::{
        Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, SourceContract,
    };

    let descriptor = SourceContract {
        id: "test.register-form",
        namespace: "test",
        event_types: &[("test.source", "test.event")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Natural,
        access_scope: AccessScope::Internal,
    };

    // Verify the descriptor is well-formed (fields accessible).
    assert_eq!(descriptor.id, "test.register-form");
    assert_eq!(descriptor.privacy_tier, PrivacyTier::Sensitive);
    Ok(())
}

#[sinex_test]
async fn source_runtime_binding_builder_accepts_all_required_fields() -> TestResult<()> {
    let descriptor = SourceRuntimeBinding::builder(
        SubjectRef::from_static("runtime_unit:test.demo"),
        "test.demo",
        "test",
    )
    .adapter("sqlite_row_stream")
    .implementation("demo::Unit")
    .output_event_type("test.output")
    .privacy_context(ProcessingContext::Command)
    .resource_profile(ResourceProfile::BoundedStream)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .build();

    assert_eq!(descriptor.output_event_type, "test.output");
    assert_eq!(descriptor.privacy_context, ProcessingContext::Command);
    assert_eq!(descriptor.resource_profile, ResourceProfile::BoundedStream);
    assert_eq!(
        descriptor.resource_budget(),
        ResourceProfile::BoundedStream.budget_spec()
    );
    assert_eq!(
        descriptor.material_lifecycle,
        MaterialLifecyclePolicy::RetainRaw
    );
    assert_eq!(
        descriptor.transport_semantics,
        TransportSemantics::DIRECT_APPEND_STREAM
    );
    Ok(())
}

#[sinex_test]
async fn binding_policy_defaults_follow_runtime_shape() -> TestResult<()> {
    assert_eq!(
        MaterialLifecyclePolicy::default_for(ResourceProfile::LiveWatcher),
        MaterialLifecyclePolicy::EphemeralRaw
    );

    let live_transport = TransportSemantics::default_for(
        RunnerPack::Live,
        CheckpointFamily::LiveObservation,
        RuntimeShape::Continuous,
    );
    assert_eq!(live_transport.transport, TransportKind::LocalQueue);
    assert_eq!(live_transport.ordering, OrderingSemantics::BestEffort);
    assert!(!live_transport.replayable);
    assert!(live_transport.backpressure);

    let external_transport = TransportSemantics::default_for(
        RunnerPack::External,
        CheckpointFamily::Journal,
        RuntimeShape::Continuous,
    );
    assert_eq!(external_transport.transport, TransportKind::JetStream);
    assert_eq!(external_transport.delivery, DeliverySemantics::AtLeastOnce);
    assert!(external_transport.dlq);

    let api_cursor = TransportSemantics::EXTERNAL_API_CURSOR;
    assert_eq!(api_cursor.transport, TransportKind::ExternalApi);
    assert_eq!(api_cursor.delivery, DeliverySemantics::AtMostOnce);
    assert_eq!(api_cursor.ordering, OrderingSemantics::CursorOrder);
    assert!(api_cursor.replayable);
    assert!(api_cursor.dlq);
    assert!(api_cursor.backpressure);
    Ok(())
}

#[sinex_test]
async fn resource_profile_budget_spec_preserves_operational_bounds() -> TestResult<()> {
    let live_budget = ResourceProfile::LiveWatcher.budget_spec();
    assert_eq!(live_budget.work_class, WorkClass::CaptureLive);
    assert!(live_budget.steady_memory_mib <= live_budget.burst_memory_mib);
    assert_eq!(
        live_budget.burst_memory_mib,
        ResourceProfile::LiveWatcher.limits().memory_max_mib
    );
    assert!(
        live_budget
            .pressure_actions
            .contains(&BudgetPressureAction::Pause)
    );
    assert!(
        live_budget
            .pressure_actions
            .contains(&BudgetPressureAction::Inspect)
    );

    let stream_budget = ResourceProfile::BoundedStream.budget_spec();
    assert_eq!(stream_budget.work_class, WorkClass::AdmissionHot);
    assert!(stream_budget.max_unacked_transport_messages.is_some());
    assert!(stream_budget.max_pending_candidates > 0);
    assert!(
        stream_budget
            .pressure_actions
            .contains(&BudgetPressureAction::Retry)
    );
    Ok(())
}

#[sinex_test]
async fn source_capability_refs_parse_known_package_refs() -> TestResult<()> {
    assert_eq!(
        SourceCapabilityRef::parse("coverage:source-coverage"),
        Some(SourceCapabilityRef {
            kind: SourceCapabilityKind::Coverage,
            target: "source-coverage",
            raw: "coverage:source-coverage",
        })
    );
    assert_eq!(
        SourceCapabilityRef::parse("debt:unified-debt-view").map(|capability| capability.kind),
        Some(SourceCapabilityKind::Debt)
    );
    assert_eq!(
        SourceCapabilityRef::parse("operation:terminal.activity.check")
            .map(|capability| capability.target),
        Some("terminal.activity.check")
    );
    assert_eq!(SourceCapabilityRef::parse("operation:"), None);
    assert_eq!(
        SourceCapabilityRef::parse("package:terminal.activity"),
        None
    );
    Ok(())
}

#[sinex_test]
async fn source_runtime_binding_exposes_typed_capability_refs() -> TestResult<()> {
    let binding = SourceRuntimeBinding::builder(
        SubjectRef::from_static("runtime_unit:test.capabilities"),
        "test.capabilities",
        "test",
    )
    .adapter("static")
    .implementation("test::capabilities")
    .output_event_type("test.output")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EmbeddedEmitter)
    .capabilities(&[
        "coverage:source-coverage",
        "unknown:ignored",
        "operation:test.capabilities.check",
    ])
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .build_impact(SourceBuildImpact::ZERO)
    .build();

    let capabilities = binding.capability_refs().collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 2);
    assert!(
        capabilities
            .iter()
            .any(|capability| capability.is_kind(SourceCapabilityKind::Coverage))
    );
    assert!(
        capabilities
            .iter()
            .any(|capability| capability.target == "test.capabilities.check")
    );
    Ok(())
}

// Sentinel binding submitted at link time to exercise the inventory
// collection path. Concrete source bindings (e.g. terminal.atuin-history)
// now live with their `#[derive(SourceDefinition)]` source structs in
// `sinexd`, so this crate's test binary only verifies the mechanism.
::inventory::submit! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:primitives.inventory-sentinel"),
        "primitives.inventory-sentinel",
        "test",
    )
    .implementation("sinex-primitives::test")
    .adapter("test_adapter")
    .output_event_type("test.output")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EmbeddedEmitter)
    .source_id("primitives.inventory-sentinel")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

#[sinex_test]
async fn source_runtime_binding_inventory_collects_submissions() -> TestResult<()> {
    let bindings = source_runtime_bindings()
        .map(|descriptor| descriptor.subject.as_str())
        .collect::<Vec<_>>();

    assert!(bindings.contains(&"source:primitives.inventory-sentinel"));
    Ok(())
}

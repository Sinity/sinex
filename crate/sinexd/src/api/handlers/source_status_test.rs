#![allow(clippy::unwrap_used)]

use super::*;
use sinex_primitives::Id;
use sinex_primitives::domain::{HealthStatus, ModuleName, SourceIdentifier};
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::rpc::{method_catalog, methods};
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape, SourceBuildImpact, SubjectRef,
};
use sinex_primitives::views::{ActionAvailability, ActionAvailabilityState, ActionSideEffect};
use std::collections::{BTreeMap, BTreeSet};
use xtask::sandbox::sinex_test;

fn session_record(
    lifecycle_state: &str,
    private_mode_blocked: bool,
) -> sinex_db::repositories::SourceSessionStateRecord {
    sinex_db::repositories::SourceSessionStateRecord {
        id: uuid::Uuid::now_v7(),
        source_id: "media.screen-ocr".to_string(),
        mode_id: "source:media.screen-ocr.live-session".to_string(),
        session_scope: "default".to_string(),
        operation_id: uuid::Uuid::now_v7(),
        result_status: sinex_primitives::domain::OperationStatus::Success,
        lifecycle_state: lifecycle_state.to_string(),
        visibility_state: "idle".to_string(),
        private_mode_blocked,
        runtime_state_ref: "media.session_runtime.observed:test".to_string(),
        coverage_ref: "coverage:media.screen-ocr.live_session".to_string(),
        debt_ref: "debt:media.screen-ocr.live_session".to_string(),
        requested_by: Some("operator".to_string()),
        reason: Some("operator stepped away".to_string()),
        detail: serde_json::json!({}),
        observed_at: time::OffsetDateTime::UNIX_EPOCH,
        updated_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

#[sinex_test]
async fn session_control_caveat_reports_operator_posture() -> xtask::sandbox::TestResult<()> {
    let paused = session_control_caveat(&session_record("paused", false));
    assert!(paused.message.contains("capture suspended"));
    assert!(paused.message.contains("paused"));
    assert!(paused.message.contains("reason=operator stepped away"));

    let enabled = session_control_caveat(&session_record("enabled", false));
    assert!(enabled.message.contains("capture active"));

    // The per-session private flag suspends even when lifecycle is enabled.
    let private = session_control_caveat(&session_record("enabled", true));
    assert!(private.message.contains("capture suspended"));
    assert!(private.message.contains("private_mode_blocked=true"));
    Ok(())
}

static CONTRACT: SourceContract = SourceContract {
    id: "fixture.source",
    namespace: "fixture",
    event_types: &[("fixture", "fixture.event")],
    privacy_tier: PrivacyTier::Sensitive,
    horizons: &[Horizon::Historical],
    retention: RetentionPolicy::Forever,
    occurrence_identity: OccurrenceIdentity::Anchor,
    access_scope: AccessScope::StagedExport,
};

static BINDING: SourceRuntimeBinding = SourceRuntimeBinding::builder(
    SubjectRef::from_static("source:fixture.source"),
    "fixture.source",
    "fixture",
)
.implementation("sinexd")
.adapter("StaticFileAdapter")
.output_event_type("fixture.event")
.privacy_context(ProcessingContext::Command)
.resource_profile(ResourceProfile::BoundedFile)
.source_id("fixture.source")
.runner_pack(RunnerPack::SinexdSource)
.checkpoint_family(CheckpointFamily::AppendStream)
.runtime_shape(RuntimeShape::OnDemand)
.build_impact(SourceBuildImpact::ZERO)
.build();

fn assert_action_rpc_methods_are_cataloged(
    source_id: &str,
    actions: &[ActionAvailability],
) -> xtask::TestResult<()> {
    let catalog = method_catalog()
        .into_iter()
        .map(|method| method.name)
        .collect::<BTreeSet<_>>();
    let missing = actions
        .iter()
        .filter_map(|action| {
            action
                .rpc_method
                .as_deref()
                .filter(|method| !catalog.contains(method))
                .map(|method| format!("{} -> {}", action.id, method))
        })
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "source {source_id} actions reference unknown RPC methods: {missing:?}"
    );
    Ok(())
}

#[sinex_test]
async fn source_coverage_view_marks_ready_when_catalog_material_and_events_exist()
-> xtask::TestResult<()> {
    let now = OffsetDateTime::now_utc();
    let mut events = HashMap::new();
    events.insert(
        ("fixture".to_string(), "fixture.event".to_string()),
        SourceEventAggregateRow {
            source: "fixture".to_string(),
            event_type: "fixture.event".to_string(),
            event_count: 3,
            last_event_at: Some(now),
        },
    );
    let mut materials = HashMap::new();
    materials.insert(
        "fixture.source".to_string(),
        SourceMaterialAggregateRow {
            source_identifier: "fixture.source".to_string(),
            material_count: 2,
            last_material_at: Some(now),
        },
    );

    let view = source_coverage_view(
        &CONTRACT,
        &[&BINDING],
        &events,
        &materials,
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    assert_eq!(view.readiness, SourceCoverageReadiness::Ready);
    assert_eq!(view.continuity, SourceCoverageContinuity::Active);
    assert_eq!(view.event_count, 3);
    assert_eq!(view.material_count, 2);
    assert!(view.gaps.is_empty());
    assert_eq!(view.privacy.tier, "sensitive");
    assert_eq!(view.privacy.context, "command");
    let budget = view
        .resource_budget
        .as_ref()
        .expect("resource budget expected");
    assert_eq!(budget.resource_profile, "bounded_file");
    assert_eq!(budget.work_class, "bulk_import");
    assert!(budget.pressure_actions.contains(&"inspect".to_string()));
    assert!(
        view.actions
            .iter()
            .any(|action| action.id == "sources.readiness")
    );
    assert_action_rpc_methods_are_cataloged(CONTRACT.id, &view.actions)?;
    Ok(())
}

#[sinex_test]
async fn material_aggregates_roll_up_material_scoped_source_identifiers() -> xtask::TestResult<()> {
    let older = OffsetDateTime::UNIX_EPOCH;
    let newer = older + time::Duration::seconds(10);
    let first_material_id = Id::<SourceMaterial>::from_uuid(Uuid::now_v7());
    let second_material_id = Id::<SourceMaterial>::from_uuid(Uuid::now_v7());

    let aggregates = material_aggregates_by_logical_source(vec![
        SourceMaterialAggregateRow {
            source_identifier: SourceIdentifier::new("fixture.source", Some(first_material_id))
                .to_wire(),
            material_count: 2,
            last_material_at: Some(older),
        },
        SourceMaterialAggregateRow {
            source_identifier: SourceIdentifier::new("fixture.source", Some(second_material_id))
                .to_wire(),
            material_count: 3,
            last_material_at: Some(newer),
        },
    ]);

    let aggregate = aggregates
        .get("fixture.source")
        .expect("material-scoped rows should roll up to logical source");
    assert_eq!(aggregate.material_count, 5);
    assert_eq!(aggregate.last_material_at, Some(newer));
    assert_eq!(aggregate.source_identifier, "fixture.source");
    Ok(())
}

#[sinex_test]
async fn status_view_request_filters_contracts_by_source_and_family() -> xtask::TestResult<()> {
    let by_source = matching_source_contracts(&SourcesStatusViewRequest {
        source: Some("browser.history".to_string()),
        family: None,
        exact_counts: false,
    });
    assert_eq!(by_source.len(), 1);
    assert_eq!(by_source[0].id, "browser.history");

    let by_family = matching_source_contracts(&SourcesStatusViewRequest {
        source: None,
        family: Some("browser".to_string()),
        exact_counts: false,
    });
    let source_ids = by_family
        .iter()
        .map(|contract| contract.id)
        .collect::<BTreeSet<_>>();
    assert!(source_ids.contains("browser.history"));
    assert!(source_ids.contains("raindrop-bookmarks"));
    assert!(!source_ids.contains("terminal.atuin-history"));
    Ok(())
}

#[sinex_test]
async fn source_event_pairs_deduplicates_declared_event_pairs() -> xtask::TestResult<()> {
    let (sources, event_types) = source_event_pairs(&[&CONTRACT, &CONTRACT]);

    assert_eq!(sources, vec!["fixture".to_string()]);
    assert_eq!(event_types, vec!["fixture.event".to_string()]);
    Ok(())
}

#[sinex_test]
async fn source_status_module_names_include_runtime_aliases() -> xtask::TestResult<()> {
    let contracts = matching_source_contracts(&SourcesStatusViewRequest {
        source: Some("browser.history".to_string()),
        family: None,
        exact_counts: false,
    });

    let module_names = source_status_module_names(&contracts);

    assert!(module_names.contains(&"browser.history".to_string()));
    assert_eq!(
        module_names.iter().collect::<BTreeSet<_>>().len(),
        module_names.len()
    );
    Ok(())
}

#[sinex_test]
async fn source_coverage_view_surfaces_missing_material_caveat() -> xtask::TestResult<()> {
    let events = HashMap::new();
    let materials = HashMap::new();

    let view = source_coverage_view(
        &CONTRACT,
        &[&BINDING],
        &events,
        &materials,
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    assert_eq!(view.readiness, SourceCoverageReadiness::MissingMaterial);
    assert_eq!(view.continuity, SourceCoverageContinuity::Gapped);
    assert!(view.gaps.iter().any(|gap| gap.kind == "missing_material"));
    assert!(
        view.caveats
            .iter()
            .any(|caveat| caveat.id == "source.material.match.logical_id")
    );
    Ok(())
}

#[sinex_test]
async fn runtime_bridge_coverage_surfaces_unobserved_bridge_and_declared_actions()
-> xtask::TestResult<()> {
    static BRIDGE_CAPABILITIES: &[&str] = &[
        "coverage:source-coverage",
        "operation:terminal.activity.check",
        "operation:terminal.activity.reconnect",
        "operation:terminal.activity.pause",
        "operation:terminal.activity.resume",
        "operation:terminal.activity.drain",
        "operation:terminal.activity.inspect",
    ];
    static BRIDGE_CONTRACT: SourceContract = SourceContract {
        id: "terminal.kitty-osc-live",
        namespace: "terminal",
        event_types: &[("shell.kitty", "command.executed")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_scope: AccessScope::RuntimeBridge {
            surface: "kitty_osc",
        },
    };
    let bridge_binding = SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:terminal.kitty-osc-live"),
        "terminal.kitty-osc-live",
        "terminal",
    )
    .implementation("live-capture")
    .adapter("UnixSocketStreamAdapter")
    .output_event_type("command.executed")
    .privacy_context(ProcessingContext::Command)
    .resource_profile(ResourceProfile::LiveWatcher)
    .capabilities(BRIDGE_CAPABILITIES)
    .source_id("terminal.kitty-osc-live")
    .runner_pack(RunnerPack::Live)
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .build();

    let view = source_coverage_view(
        &BRIDGE_CONTRACT,
        &[&bridge_binding],
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    let caveat = view
        .caveats
        .iter()
        .find(|caveat| caveat.id == "source.runtime_bridge.unobserved")
        .expect("bridge caveat expected");
    assert!(
        caveat.message.contains("kitty_osc"),
        "runtime bridge caveat should name the unobserved bridge surface"
    );
    assert_eq!(
        caveat
            .ref_
            .as_ref()
            .map(|ref_| (&ref_.kind, ref_.id.as_str())),
        Some((&SinexObjectKind::SourceDriver, "terminal.kitty-osc-live"))
    );

    let check = view
        .actions
        .iter()
        .find(|action| action.id == "terminal.activity.check")
        .expect("check action expected");
    assert_eq!(check.state, ActionAvailabilityState::Enabled);
    assert_eq!(
        check.command_hint.as_deref(),
        Some("sinexctl sources status terminal.kitty-osc-live --format json")
    );

    let pause = view
        .actions
        .iter()
        .find(|action| action.id == "terminal.activity.pause")
        .expect("pause action expected");
    assert_eq!(pause.state, ActionAvailabilityState::Enabled);
    assert_eq!(pause.side_effect, ActionSideEffect::Admin);
    assert!(pause.requires_confirmation);
    assert_eq!(pause.rpc_method.as_deref(), Some("runtime.drain"));
    assert_eq!(
        pause.command_hint.as_deref(),
        Some("sinexctl runtime drain terminal-source --reason source-paused")
    );

    let resume = view
        .actions
        .iter()
        .find(|action| action.id == "terminal.activity.resume")
        .expect("resume action expected");
    assert_eq!(resume.state, ActionAvailabilityState::Enabled);
    assert_eq!(resume.side_effect, ActionSideEffect::Admin);
    assert!(resume.requires_confirmation);
    assert_eq!(resume.rpc_method.as_deref(), Some("runtime.resume"));
    assert_eq!(
        resume.command_hint.as_deref(),
        Some("sinexctl runtime resume terminal-source")
    );

    let drain = view
        .actions
        .iter()
        .find(|action| action.id == "terminal.activity.drain")
        .expect("drain action expected");
    assert_eq!(drain.state, ActionAvailabilityState::Enabled);
    assert_eq!(drain.side_effect, ActionSideEffect::Admin);
    assert!(drain.requires_confirmation);
    assert_eq!(drain.rpc_method.as_deref(), Some("runtime.drain"));
    assert_eq!(
        drain.command_hint.as_deref(),
        Some("sinexctl runtime drain terminal-source --reason source-coverage")
    );

    let reconnect = view
        .actions
        .iter()
        .find(|action| action.id == "terminal.activity.reconnect")
        .expect("reconnect action expected");
    assert_eq!(reconnect.state, ActionAvailabilityState::Enabled);
    assert_eq!(reconnect.side_effect, ActionSideEffect::Admin);
    assert!(reconnect.requires_confirmation);
    assert_eq!(reconnect.rpc_method.as_deref(), Some("runtime.resume"));
    assert_eq!(
        reconnect.command_hint.as_deref(),
        Some("sinexctl runtime resume terminal-source")
    );
    assert_action_rpc_methods_are_cataloged(BRIDGE_CONTRACT.id, &view.actions)?;
    Ok(())
}

#[sinex_test]
async fn media_package_operations_surface_operator_actions() -> xtask::TestResult<()> {
    let contract = all_source_contracts()
        .find(|contract| contract.id == "media.audio-transcript")
        .expect("media audio contract expected");
    let bindings = source_runtime_bindings()
        .filter(|binding| binding.source_id == "media.audio-transcript")
        .collect::<Vec<_>>();

    let view = source_coverage_view(
        contract,
        &bindings,
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    let import_transcript = view
        .actions
        .iter()
        .find(|action| action.id == "media.audio-transcript.import-transcript")
        .expect("transcript import action expected");
    assert_eq!(
        import_transcript.command_hint.as_deref(),
        Some("sinexctl sources stage <path> --binding source:media.audio-transcript --format json")
    );
    assert_eq!(
        import_transcript.rpc_method.as_deref(),
        Some("sources.stage")
    );
    assert_eq!(import_transcript.side_effect, ActionSideEffect::Write);
    assert_eq!(import_transcript.state, ActionAvailabilityState::Enabled);

    let import_bundle = view
        .actions
        .iter()
        .find(|action| action.id == "media.audio-transcript.import-bundle")
        .expect("audio bundle import action expected");
    assert_eq!(
        import_bundle.command_hint.as_deref(),
        Some(
            "sinexctl sources stage <path> --binding source:media.audio-transcript.audio-bundle-staged --format json"
        )
    );
    assert_eq!(import_bundle.rpc_method.as_deref(), Some("sources.stage"));
    assert_eq!(import_bundle.side_effect, ActionSideEffect::Write);
    assert_eq!(import_bundle.state, ActionAvailabilityState::Enabled);

    let replay = view
        .actions
        .iter()
        .find(|action| action.id == "media.audio-transcript.replay")
        .expect("replay action expected");
    assert_eq!(
        replay.command_hint.as_deref(),
        Some("sinexctl ops replay plan --source media.audio-transcript")
    );
    assert_eq!(
        replay.rpc_method.as_deref(),
        Some(methods::REPLAY_CREATE_OPERATION)
    );
    assert_eq!(replay.side_effect, ActionSideEffect::Write);

    let export = view
        .actions
        .iter()
        .find(|action| action.id == "media.audio-transcript.export")
        .expect("export action expected");
    assert_eq!(
        export.command_hint.as_deref(),
        Some("sinexctl privacy export --source media.audio-transcript --output <file>")
    );

    let run_model = view
        .actions
        .iter()
        .find(|action| action.id == "media.audio-transcript.run-model")
        .expect("model action expected");
    assert_eq!(run_model.state, ActionAvailabilityState::Enabled);
    assert_eq!(run_model.side_effect, ActionSideEffect::Admin);
    assert_eq!(run_model.rpc_method.as_deref(), Some("ops.start"));
    assert_eq!(
        run_model.command_hint.as_deref(),
        Some(
            "sinexctl ops start media.audio-transcript.run-model --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.local-model-batch\"}' --format json"
        )
    );

    let pause = view
        .actions
        .iter()
        .find(|action| action.id == "media.audio-transcript.pause")
        .expect("media pause action expected");
    assert_eq!(pause.state, ActionAvailabilityState::Enabled);
    assert_eq!(pause.side_effect, ActionSideEffect::Admin);
    assert_eq!(pause.rpc_method.as_deref(), Some("ops.start"));
    assert_eq!(
        pause.command_hint.as_deref(),
        Some(
            "sinexctl ops start media.audio-transcript.pause --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.live-session\"}' --format json"
        )
    );

    let delete = view
        .actions
        .iter()
        .find(|action| action.id == "media.audio-transcript.delete-material")
        .expect("delete material action expected");
    assert_eq!(delete.state, ActionAvailabilityState::Enabled);
    assert_eq!(delete.side_effect, ActionSideEffect::Destructive);
    assert!(delete.requires_confirmation);
    assert_eq!(delete.rpc_method.as_deref(), Some("ops.start"));
    assert_eq!(
        delete.command_hint.as_deref(),
        Some(
            "sinexctl ops start media.audio-transcript.delete-material --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.audio-bundle-staged\"}' --format json"
        )
    );
    let local_model_mode = view
        .modes
        .iter()
        .find(|mode| mode.mode_id == "source:media.audio-transcript.local-model-batch")
        .expect("audio local model mode expected");
    assert_eq!(
        local_model_mode.implementation,
        "local-transcription-worker"
    );
    assert_eq!(local_model_mode.adapter, "LocalProcessWorker");
    assert_eq!(local_model_mode.runtime_shape, "on_demand");
    assert_eq!(local_model_mode.material_lifecycle, "derived_only");
    assert_eq!(local_model_mode.transport, "direct");
    assert_eq!(local_model_mode.resource_budget.work_class, "bulk_import");
    assert!(
        local_model_mode
            .actions
            .iter()
            .any(|action| action.id == "media.audio-transcript.run-model"
                && action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start media.audio-transcript.run-model --scope '{\"source_id\":\"media.audio-transcript\",\"mode_id\":\"source:media.audio-transcript.local-model-batch\"}' --format json"
                    ))
    );
    let live_audio_mode = view
        .modes
        .iter()
        .find(|mode| mode.mode_id == "source:media.audio-transcript.live-session")
        .expect("audio live mode expected");
    assert!(live_audio_mode.proposed);
    assert_eq!(live_audio_mode.runner_pack, "live");
    assert_eq!(live_audio_mode.runtime_shape, "continuous");
    assert_eq!(live_audio_mode.material_lifecycle, "ephemeral_raw");
    assert_eq!(live_audio_mode.transport, "local_queue");
    assert!(live_audio_mode.backpressure);

    let screen_contract = all_source_contracts()
        .find(|contract| contract.id == "media.screen-ocr")
        .expect("media screen contract expected");
    let screen_bindings = source_runtime_bindings()
        .filter(|binding| binding.source_id == "media.screen-ocr")
        .collect::<Vec<_>>();
    let screen_view = source_coverage_view(
        screen_contract,
        &screen_bindings,
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    let import_ocr = screen_view
        .actions
        .iter()
        .find(|action| action.id == "media.screen-ocr.import-ocr")
        .expect("OCR import action expected");
    assert_eq!(
        import_ocr.command_hint.as_deref(),
        Some("sinexctl sources stage <path> --binding source:media.screen-ocr --format json")
    );
    assert_eq!(import_ocr.rpc_method.as_deref(), Some("sources.stage"));
    assert_eq!(import_ocr.side_effect, ActionSideEffect::Write);
    assert_eq!(import_ocr.state, ActionAvailabilityState::Enabled);

    let import_screenshots = screen_view
        .actions
        .iter()
        .find(|action| action.id == "media.screen-ocr.import-screenshots")
        .expect("screenshot import action expected");
    assert_eq!(
        import_screenshots.command_hint.as_deref(),
        Some(
            "sinexctl sources stage <path> --binding source:media.screen-ocr.screenshot-ocr-staged --format json"
        )
    );
    assert_eq!(
        import_screenshots.rpc_method.as_deref(),
        Some("sources.stage")
    );
    assert_eq!(import_screenshots.side_effect, ActionSideEffect::Write);
    assert_eq!(import_screenshots.state, ActionAvailabilityState::Enabled);

    let import_video = screen_view
        .actions
        .iter()
        .find(|action| action.id == "media.screen-ocr.import-video")
        .expect("screen-video import action expected");
    assert_eq!(
        import_video.command_hint.as_deref(),
        Some(
            "sinexctl sources stage <path> --binding source:media.screen-ocr.video-staged --format json"
        )
    );
    assert_eq!(import_video.rpc_method.as_deref(), Some("sources.stage"));
    assert_eq!(import_video.side_effect, ActionSideEffect::Write);
    assert_eq!(import_video.state, ActionAvailabilityState::Enabled);

    let run_ocr = screen_view
        .actions
        .iter()
        .find(|action| action.id == "media.screen-ocr.run-ocr")
        .expect("run OCR action expected");
    assert_eq!(run_ocr.state, ActionAvailabilityState::Enabled);
    assert_eq!(run_ocr.rpc_method.as_deref(), Some("ops.start"));
    assert_eq!(
        run_ocr.command_hint.as_deref(),
        Some(
            "sinexctl ops start media.screen-ocr.run-ocr --scope '{\"source_id\":\"media.screen-ocr\",\"mode_id\":\"source:media.screen-ocr.local-model-batch\"}' --format json"
        )
    );

    let capture_region = screen_view
        .actions
        .iter()
        .find(|action| action.id == "media.screen-ocr.capture-region")
        .expect("capture-region action expected");
    assert_eq!(capture_region.state, ActionAvailabilityState::Enabled);
    assert_eq!(capture_region.side_effect, ActionSideEffect::Admin);
    assert_eq!(capture_region.rpc_method.as_deref(), Some("ops.start"));
    assert_eq!(
        capture_region.command_hint.as_deref(),
        Some(
            "sinexctl ops start media.screen-ocr.capture-region --scope '{\"source_id\":\"media.screen-ocr\",\"mode_id\":\"source:media.screen-ocr.on-demand-region\"}' --format json"
        )
    );

    let record_video = screen_view
        .actions
        .iter()
        .find(|action| action.id == "media.screen-ocr.record-video")
        .expect("record-video action expected");
    assert_eq!(record_video.state, ActionAvailabilityState::Enabled);
    assert_eq!(record_video.side_effect, ActionSideEffect::Admin);
    assert_eq!(record_video.rpc_method.as_deref(), Some("ops.start"));
    assert_eq!(
        record_video.command_hint.as_deref(),
        Some(
            "sinexctl ops start media.screen-ocr.record-video --scope '{\"source_id\":\"media.screen-ocr\",\"mode_id\":\"source:media.screen-ocr.on-demand-video\"}' --format json"
        )
    );
    let video_mode = screen_view
        .modes
        .iter()
        .find(|mode| mode.mode_id == "source:media.screen-ocr.video-staged")
        .expect("screen-video staged mode expected");
    assert_eq!(video_mode.implementation, "staged-screen-video-bundle");
    assert_eq!(
        video_mode.output_event_type,
        "media.screen.video_segment_observed"
    );
    assert_eq!(video_mode.material_lifecycle, "retain_raw");
    assert_eq!(video_mode.transport, "direct");
    assert!(
        video_mode
            .actions
            .iter()
            .any(|action| action.id == "media.screen-ocr.import-video"
                && action.rpc_method.as_deref() == Some("sources.stage"))
    );

    assert_action_rpc_methods_are_cataloged("media.audio-transcript", &view.actions)?;
    assert_action_rpc_methods_are_cataloged("media.screen-ocr", &screen_view.actions)?;

    Ok(())
}

#[sinex_test]
async fn email_package_operations_surface_operator_actions() -> xtask::TestResult<()> {
    let contract = all_source_contracts()
        .find(|contract| contract.id == "email.mailbox")
        .expect("email mailbox contract expected");
    let bindings = source_runtime_bindings()
        .filter(|binding| binding.source_id == "email.mailbox")
        .collect::<Vec<_>>();

    let view = source_coverage_view(
        contract,
        &bindings,
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    let authorize_gmail = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl ops start email.mailbox.authorize --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json",
                )
        })
        .expect("Gmail authorize action expected");
    assert_eq!(authorize_gmail.state, ActionAvailabilityState::Enabled);
    assert_eq!(authorize_gmail.side_effect, ActionSideEffect::Admin);
    assert_eq!(authorize_gmail.rpc_method.as_deref(), Some("ops.start"));
    assert_eq!(authorize_gmail.label, "Authorize Gmail");

    let authorize_imap = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl ops start email.mailbox.authorize --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.imap-scheduled-sync\"}' --format json",
                )
        })
        .expect("IMAP authorize action expected");
    assert_eq!(authorize_imap.label, "Authorize IMAP");

    let import_rfc822 = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl sources stage <path> --binding source:email.mailbox --format json",
                )
        })
        .expect("RFC822 import action expected");
    assert_eq!(import_rfc822.state, ActionAvailabilityState::Enabled);
    assert_eq!(import_rfc822.side_effect, ActionSideEffect::Write);
    assert_eq!(import_rfc822.rpc_method.as_deref(), Some("sources.stage"));
    assert_eq!(import_rfc822.label, "Import RFC822 Message");

    let import_maildir = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl sources stage <path> --binding source:email.mailbox.maildir-staged --format json",
                )
        })
        .expect("Maildir import action expected");
    assert_eq!(import_maildir.state, ActionAvailabilityState::Enabled);
    assert_eq!(import_maildir.side_effect, ActionSideEffect::Write);
    assert_eq!(import_maildir.rpc_method.as_deref(), Some("sources.stage"));
    assert_eq!(import_maildir.label, "Import Maildir Entry");

    let import_mbox = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl sources stage <path> --binding source:email.mailbox.mbox-staged --format json",
                )
        })
        .expect("MBOX import action expected");
    assert_eq!(import_mbox.state, ActionAvailabilityState::Enabled);
    assert_eq!(import_mbox.side_effect, ActionSideEffect::Write);
    assert_eq!(import_mbox.rpc_method.as_deref(), Some("sources.stage"));
    assert_eq!(import_mbox.label, "Import MBOX");

    let gmail_sync = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json",
                )
        })
        .expect("Gmail sync action expected");
    assert_eq!(gmail_sync.label, "Sync Gmail");

    let imap_sync = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.imap-scheduled-sync\"}' --format json",
                )
        })
        .expect("IMAP sync action expected");
    assert_eq!(imap_sync.label, "Sync IMAP");
    let gmail_mode = view
        .modes
        .iter()
        .find(|mode| mode.mode_id == "source:email.mailbox.gmail-api-scheduled-sync")
        .expect("Gmail scheduled mode expected");
    assert_eq!(gmail_mode.implementation, "gmail-api-scheduled-sync");
    assert_eq!(gmail_mode.adapter, "GmailApiCursorAdapter");
    assert_eq!(gmail_mode.runtime_shape, "scheduled");
    assert_eq!(gmail_mode.material_lifecycle, "external_reference_only");
    assert_eq!(gmail_mode.transport, "external_api");
    assert!(gmail_mode.dlq);
    assert!(gmail_mode.backpressure);
    assert!(
        gmail_mode
            .actions
            .iter()
            .any(|action| action.label == "Sync Gmail"
                && action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json"
                    ))
    );
    let imap_idle = view
        .modes
        .iter()
        .find(|mode| mode.mode_id == "source:email.mailbox.imap-idle-live")
        .expect("IMAP IDLE mode expected");
    assert_eq!(imap_idle.runtime_shape, "continuous");
    assert_eq!(imap_idle.resource_budget.work_class, "capture_live");
    assert!(
        imap_idle
            .actions
            .iter()
            .any(|action| action.label == "Observe IMAP IDLE"
                && action.command_hint.as_deref()
                    == Some(
                        "sinexctl ops start email.mailbox.sync --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.imap-idle-live\"}' --format json"
                    ))
    );
    assert!(
        imap_idle
            .actions
            .iter()
            .any(|action| action.label == "Pause IMAP IDLE")
    );

    assert!(
        view.actions.iter().all(|action| {
            let hint = action.command_hint.as_deref().unwrap_or_default();
            !hint.contains("<email-mode-id>") && !hint.contains("<provider-mode-id>")
        }),
        "email coverage actions should be concrete mode commands"
    );

    let pause = view
        .actions
        .iter()
        .find(|action| {
            action.command_hint.as_deref()
                == Some(
                    "sinexctl ops start email.mailbox.pause --scope '{\"source_id\":\"email.mailbox\",\"mode_id\":\"source:email.mailbox.gmail-api-scheduled-sync\"}' --format json",
                )
        })
        .expect("email pause action expected");
    assert_eq!(pause.state, ActionAvailabilityState::Enabled);
    assert_eq!(pause.side_effect, ActionSideEffect::Admin);
    assert_eq!(pause.rpc_method.as_deref(), Some("ops.start"));

    let check = view
        .actions
        .iter()
        .find(|action| action.id == "email.mailbox.check")
        .expect("email check action expected");
    assert_eq!(check.state, ActionAvailabilityState::Enabled);
    assert_eq!(
        check.command_hint.as_deref(),
        Some("sinexctl sources status email.mailbox --format json")
    );
    assert_eq!(check.side_effect, ActionSideEffect::Read);

    assert_action_rpc_methods_are_cataloged("email.mailbox", &view.actions)?;
    Ok(())
}

#[sinex_test]
async fn email_provider_failure_operation_surfaces_source_coverage_debt_caveat()
-> xtask::TestResult<()> {
    let contract = all_source_contracts()
        .find(|contract| contract.id == "email.mailbox")
        .expect("email mailbox contract expected");
    let bindings = source_runtime_bindings()
        .filter(|binding| binding.source_id == "email.mailbox")
        .collect::<Vec<_>>();
    let operation_id = Uuid::now_v7();
    let mut provider_states = HashMap::new();
    provider_states.insert(
        "source:email.mailbox.gmail-api-scheduled-sync".to_string(),
        EmailProviderOperationState {
            operation_id,
            result_status: "failure".to_string(),
            provider_runtime: Some(serde_json::json!({
                "runtime_observation_contract": {
                    "auth_state": "missing",
                    "network_state": "unknown"
                }
            })),
            provider_failure: Some(serde_json::json!({
                "reason": "Gmail token file is unavailable",
                "coverage_ref": "coverage:email.mailbox.gmail.provider_runtime",
                "debt_ref": "debt:email.mailbox.gmail.provider_runtime",
                "actions": ["email.mailbox.authorize", "email.mailbox.sync"]
            })),
            failure_class: Some("authorization-missing".to_string()),
            required_action: Some("email.mailbox.authorize".to_string()),
            retry_after_secs: None,
            reconnect_state: None,
        },
    );

    let view = source_coverage_view(
        contract,
        &bindings,
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &provider_states,
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    let caveat = view
        .caveats
        .iter()
        .find(|caveat| {
            caveat.id
                == "email.provider_runtime.failed.source:email.mailbox.gmail-api-scheduled-sync"
        })
        .expect("provider runtime failure caveat expected");
    assert!(caveat.message.contains("ended with failure"));
    assert!(caveat.message.contains("auth_state=missing"));
    assert!(caveat.message.contains("network_state=unknown"));
    assert!(
        caveat
            .message
            .contains("failure_class=authorization-missing")
    );
    assert!(
        caveat
            .message
            .contains("required_action=email.mailbox.authorize")
    );
    assert!(
        caveat
            .message
            .contains("debt:email.mailbox.gmail.provider_runtime")
    );
    assert!(caveat.message.contains("Gmail token file is unavailable"));
    let ref_ = caveat
        .ref_
        .as_ref()
        .expect("provider failure caveat should point at the operation");
    assert_eq!(ref_.kind, SinexObjectKind::Operation);
    assert_eq!(ref_.id, operation_id.to_string());
    let gmail_mode = view
        .modes
        .iter()
        .find(|mode| mode.mode_id == "source:email.mailbox.gmail-api-scheduled-sync")
        .expect("Gmail provider mode expected");
    assert_eq!(
        gmail_mode.provider_operation_status.as_deref(),
        Some("failure")
    );
    assert_eq!(gmail_mode.provider_auth_state.as_deref(), Some("missing"));
    assert_eq!(
        gmail_mode.provider_network_state.as_deref(),
        Some("unknown")
    );
    assert_eq!(
        gmail_mode.provider_operation_id.as_deref(),
        Some(ref_.id.as_str())
    );
    assert_eq!(
        gmail_mode.provider_debt_ref.as_deref(),
        Some("debt:email.mailbox.gmail.provider_runtime")
    );
    assert_eq!(
        gmail_mode.provider_failure_class.as_deref(),
        Some("authorization-missing")
    );
    assert_eq!(
        gmail_mode.provider_required_action.as_deref(),
        Some("email.mailbox.authorize")
    );
    Ok(())
}

#[sinex_test]
async fn email_mailbox_projection_surfaces_materialization_debt_and_mode_counts()
-> xtask::TestResult<()> {
    let contract = all_source_contracts()
        .find(|contract| contract.id == "email.mailbox")
        .expect("email mailbox contract expected");
    let bindings = source_runtime_bindings()
        .filter(|binding| binding.source_id == "email.mailbox")
        .collect::<Vec<_>>();
    let mode_id = "source:email.mailbox.mbox-staged".to_string();
    let mut projection_states = HashMap::new();
    projection_states.insert(
        mode_id.clone(),
        EmailMailboxProjectionState {
            message_count: 3,
            thread_count: 2,
            body_bytes: 128,
            attachment_count: 4,
            attachment_observed_count: 1,
            last_observed_at: Timestamp::now(),
        },
    );

    let view = source_coverage_view(
        contract,
        &bindings,
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &HashMap::new(),
        &HashMap::new(),
        &projection_states,
        &HashMap::new(),
        Timestamp::now(),
    );

    let caveat = view
        .caveats
        .iter()
        .find(|caveat| {
            caveat.id
                == "email.mailbox_projection.source:email.mailbox.mbox-staged.materialization_debt"
        })
        .expect("projection materialization debt caveat expected");
    assert!(caveat.message.contains("3 projected message"));
    assert!(caveat.message.contains("128 message body byte"));
    assert!(caveat.message.contains("4 attachment(s) declared"));

    let mode = view
        .modes
        .iter()
        .find(|mode| mode.mode_id == mode_id)
        .expect("mbox staged mode expected");
    assert_eq!(mode.mailbox_projection_message_count, Some(3));
    assert_eq!(mode.mailbox_projection_thread_count, Some(2));
    assert_eq!(mode.mailbox_projection_body_bytes, Some(128));
    assert_eq!(mode.mailbox_projection_attachment_count, Some(4));
    assert_eq!(mode.mailbox_projection_attachment_observed_count, Some(1));
    assert!(mode.mailbox_projection_last_observed_at.is_some());
    let fetch_attachments = mode
        .actions
        .iter()
        .find(|action| {
            action.id == "email.mailbox.fetch-attachments:source:email.mailbox.mbox-staged"
        })
        .expect("projection debt should advertise attachment fetch operation");
    assert_eq!(fetch_attachments.state, ActionAvailabilityState::Enabled);
    assert_eq!(fetch_attachments.side_effect, ActionSideEffect::Write);
    assert_eq!(
        fetch_attachments.rpc_method.as_deref(),
        Some(methods::OPS_START)
    );
    assert!(
        fetch_attachments
            .command_hint
            .as_deref()
            .is_some_and(|hint| {
                hint.contains("email.mailbox.fetch-attachments")
                    && hint.contains("source:email.mailbox.mbox-staged")
            })
    );
    let export = mode
        .actions
        .iter()
        .find(|action| action.id == "email.mailbox.export:source:email.mailbox.mbox-staged")
        .expect("projection debt should advertise scoped mailbox export operation");
    assert_eq!(export.state, ActionAvailabilityState::Enabled);
    assert_eq!(export.side_effect, ActionSideEffect::Write);
    assert_eq!(export.rpc_method.as_deref(), Some(methods::OPS_START));
    let rebuild = mode
        .actions
        .iter()
        .find(|action| {
            action.id == "email.mailbox.rebuild-projection:source:email.mailbox.mbox-staged"
        })
        .expect("projection debt should advertise projection rebuild operation");
    assert_eq!(rebuild.state, ActionAvailabilityState::Enabled);
    assert_eq!(rebuild.side_effect, ActionSideEffect::Write);
    assert_eq!(rebuild.rpc_method.as_deref(), Some(methods::OPS_START));
    Ok(())
}

#[sinex_test]
async fn latest_email_provider_state_prefers_newer_success_over_old_failure()
-> xtask::TestResult<()> {
    let failed_operation_id = Uuid::now_v7();
    let successful_operation_id = Uuid::now_v7();
    let states = email_provider_operation_states_from_rows(vec![
        EmailProviderStateRecord {
            id: Uuid::now_v7(),
            source_id: "email.mailbox".to_string(),
            operation_id: successful_operation_id,
            result_status: sinex_primitives::domain::OperationStatus::Success,
            mode_id: "source:email.mailbox.imap-scheduled-sync".to_string(),
            provider: "imap".to_string(),
            account_binding_ref: "operator-mailbox:imap-primary".to_string(),
            mailbox_scope: "default".to_string(),
            auth_state: "authorized".to_string(),
            network_state: "online".to_string(),
            sync_state: "synced".to_string(),
            rate_limit_state: None,
            runtime_state_ref: "email.provider_runtime.imap".to_string(),
            coverage_ref: "coverage:email.mailbox.imap.provider_runtime".to_string(),
            debt_ref: "debt:email.mailbox.imap.provider_runtime".to_string(),
            failure_class: None,
            required_action: None,
            retry_after_secs: None,
            reconnect_state: None,
            cursor_kind: None,
            cursor_value: None,
            continuity_state: None,
            provider_runtime: serde_json::json!({
                "coverage_ref": "coverage:email.mailbox.imap.provider_runtime",
                "runtime_observation_contract": {
                    "auth_state": "authorized",
                    "network_state": "online",
                    "sync_state": "synced"
                }
            }),
            provider_cursor: None,
            provider_failure: None,
            observed_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
        },
        EmailProviderStateRecord {
            id: Uuid::now_v7(),
            source_id: "email.mailbox".to_string(),
            operation_id: failed_operation_id,
            result_status: sinex_primitives::domain::OperationStatus::Failed,
            mode_id: "source:email.mailbox.imap-scheduled-sync".to_string(),
            provider: "imap".to_string(),
            account_binding_ref: "operator-mailbox:imap-primary".to_string(),
            mailbox_scope: "default".to_string(),
            auth_state: "authorized".to_string(),
            network_state: "error".to_string(),
            sync_state: "failed".to_string(),
            rate_limit_state: None,
            runtime_state_ref: "email.provider_runtime.imap".to_string(),
            coverage_ref: "coverage:email.mailbox.imap.provider_runtime".to_string(),
            debt_ref: "debt:email.mailbox.imap.provider_runtime".to_string(),
            failure_class: Some("network-reconnect".to_string()),
            required_action: Some("email.mailbox.reconnect".to_string()),
            retry_after_secs: None,
            reconnect_state: Some("reconnect-required".to_string()),
            cursor_kind: None,
            cursor_value: None,
            continuity_state: None,
            provider_runtime: serde_json::json!({
                "runtime_observation_contract": {
                    "auth_state": "authorized",
                    "network_state": "error",
                    "sync_state": "failed"
                }
            }),
            provider_cursor: None,
            provider_failure: Some(serde_json::json!({
                "reason": "older IMAP failure",
                "debt_ref": "debt:email.mailbox.imap.provider_runtime"
            })),
            observed_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
        },
    ]);

    let state = states
        .get("source:email.mailbox.imap-scheduled-sync")
        .expect("provider state expected");
    assert_eq!(state.operation_id, successful_operation_id);
    assert_eq!(state.result_status, "success");

    let caveat = email_provider_operation_caveat("source:email.mailbox.imap-scheduled-sync", state);
    assert_eq!(
        caveat.id,
        "email.provider_runtime.observed.source:email.mailbox.imap-scheduled-sync"
    );
    assert!(caveat.message.contains("ended with success"));
    assert!(caveat.message.contains("network_state=online"));
    assert!(!caveat.message.contains("older IMAP failure"));
    assert_eq!(
        caveat.ref_.as_ref().map(|ref_| ref_.id.clone()),
        Some(successful_operation_id.to_string())
    );
    Ok(())
}

#[sinex_test]
async fn runtime_bridge_coverage_uses_runtime_observation_for_last_seen() -> xtask::TestResult<()> {
    let now = Timestamp::now();
    let bridge_binding = terminal_bridge_binding();
    let mut observations = HashMap::new();
    observations.insert(
        "terminal-source".to_string(),
        terminal_bridge_status(now).with_recent_output(now - time::Duration::seconds(30), 7),
    );

    let view = source_coverage_view(
        &terminal_bridge_contract(),
        &[&bridge_binding],
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &observations,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        now,
    );

    assert!(
        !view
            .caveats
            .iter()
            .any(|caveat| caveat.id == "source.runtime_bridge.unobserved"),
        "observed runtime state should replace the static unobserved bridge caveat"
    );
    let observed = view
        .caveats
        .iter()
        .find(|caveat| caveat.id == "source.runtime_bridge.observed")
        .expect("observed runtime caveat expected");
    assert!(observed.message.contains("connected"));
    assert!(observed.message.contains("terminal-source"));
    assert!(observed.message.contains("last heartbeat"));
    assert!(observed.message.contains("last output"));
    assert!(observed.message.contains("recent output count 7"));
    assert!(
        view.gaps
            .iter()
            .all(|gap| gap.kind != "runtime_bridge_disconnected")
    );
    Ok(())
}

#[sinex_test]
async fn source_runtime_observation_requires_health_heartbeat_or_output() -> xtask::TestResult<()> {
    let now = Timestamp::now();
    let mut manifest_only = terminal_bridge_status(now);
    manifest_only.live = false;
    manifest_only.last_heartbeat_at = None;
    manifest_only.current_health = None;
    manifest_only.health_changed_at = None;
    manifest_only.health_reason = None;
    manifest_only.recent_output_count = 0;
    manifest_only.last_output_at = None;

    assert!(
        !source_status_has_runtime_evidence(&manifest_only),
        "static manifest/run metadata must not render as observed runtime evidence"
    );

    let mut heartbeat = manifest_only.clone();
    heartbeat.last_heartbeat_at = Some(now - time::Duration::seconds(5));
    assert!(source_status_has_runtime_evidence(&heartbeat));

    let output = manifest_only.with_recent_output(now - time::Duration::seconds(30), 7);
    assert!(source_status_has_runtime_evidence(&output));

    Ok(())
}

#[sinex_test]
async fn runtime_bridge_coverage_surfaces_disconnected_runtime_observation() -> xtask::TestResult<()>
{
    let now = Timestamp::now();
    let bridge_binding = terminal_bridge_binding();
    let mut status = terminal_bridge_status(now);
    status.live = false;
    status.current_health = Some(HealthStatus::Unhealthy);
    status.health_reason = Some("runtime module disconnected".to_string());
    let mut observations = HashMap::new();
    observations.insert("terminal-source".to_string(), status);

    let view = source_coverage_view(
        &terminal_bridge_contract(),
        &[&bridge_binding],
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &observations,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        now,
    );

    assert!(
        view.gaps
            .iter()
            .any(|gap| gap.kind == "runtime_bridge_disconnected"),
        "disconnected runtime observations should become source coverage gaps"
    );
    let caveat = view
        .caveats
        .iter()
        .find(|caveat| caveat.id == "source.runtime_bridge.disconnected")
        .expect("disconnected caveat expected");
    assert!(caveat.message.contains("disconnected"));
    assert!(caveat.message.contains("last heartbeat"));
    Ok(())
}

#[sinex_test]
async fn runtime_bridge_coverage_surfaces_malformed_frame_health_reason() -> xtask::TestResult<()> {
    let now = Timestamp::now();
    let bridge_binding = terminal_bridge_binding();
    let mut status = terminal_bridge_status(now);
    status.current_health = Some(HealthStatus::Degraded);
    status.health_reason = Some("malformed Kitty OSC frame rejected".to_string());
    let mut observations = HashMap::new();
    observations.insert("terminal-source".to_string(), status);

    let view = source_coverage_view(
        &terminal_bridge_contract(),
        &[&bridge_binding],
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &observations,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        now,
    );

    let caveat = view
        .caveats
        .iter()
        .find(|caveat| caveat.id == "source.runtime_bridge.health")
        .expect("runtime health caveat expected");
    assert!(caveat.message.contains("degraded"));
    assert!(
        caveat
            .message
            .contains("malformed Kitty OSC frame rejected")
    );
    Ok(())
}

#[sinex_test]
async fn runtime_bridge_coverage_surfaces_heartbeat_without_output_as_stalled()
-> xtask::TestResult<()> {
    let now = Timestamp::now();
    let bridge_binding = terminal_bridge_binding();
    let mut observations = HashMap::new();
    observations.insert("terminal-source".to_string(), terminal_bridge_status(now));

    let view = source_coverage_view(
        &terminal_bridge_contract(),
        &[&bridge_binding],
        &HashMap::new(),
        &HashMap::new(),
        &healthy_confirmation_buffer(),
        &observations,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        now,
    );

    assert!(
        view.gaps
            .iter()
            .any(|gap| gap.kind == "runtime_bridge_stalled"),
        "heartbeating bridge with no recent output should become coverage debt"
    );
    let caveat = view
        .caveats
        .iter()
        .find(|caveat| caveat.id == "source.runtime_bridge.stalled")
        .expect("stalled caveat expected");
    assert!(caveat.message.contains("heartbeating"));
    assert!(caveat.message.contains("no recent source output"));
    Ok(())
}

#[sinex_test]
async fn source_coverage_view_surfaces_attributed_confirmation_pressure() -> xtask::TestResult<()> {
    let mut confirmation_buffer = healthy_confirmation_buffer();
    confirmation_buffer.status = HealthStatus::Degraded;
    confirmation_buffer.approximate_payload_bytes = 1536;
    confirmation_buffer.approximate_payload_bytes_by_kind = BTreeMap::from([
        ("fixture:fixture.event".to_string(), 1024),
        ("other.source:other.event".to_string(), 512),
    ]);

    let view = source_coverage_view(
        &CONTRACT,
        &[&BINDING],
        &HashMap::new(),
        &HashMap::new(),
        &confirmation_buffer,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    let caveat = view
        .caveats
        .iter()
        .find(|caveat| caveat.id == "source.pressure.confirmation_buffer.retained_payload")
        .expect("pressure caveat expected");
    assert!(
        caveat.message.contains("1024 byte(s)"),
        "source-local caveat should report only bytes attributed to the source contract"
    );
    assert!(
        view.actions
            .iter()
            .any(|action| action.id == "runtime.health.inspect"),
        "source-local pressure should expose the runtime health inspection action"
    );
    Ok(())
}

#[sinex_test]
async fn source_coverage_view_does_not_localize_unattributed_pressure() -> xtask::TestResult<()> {
    let mut confirmation_buffer = healthy_confirmation_buffer();
    confirmation_buffer.status = HealthStatus::Degraded;
    confirmation_buffer.approximate_payload_bytes = 512;
    confirmation_buffer.approximate_payload_bytes_by_kind =
        BTreeMap::from([("other.source:other.event".to_string(), 512)]);

    let view = source_coverage_view(
        &CONTRACT,
        &[&BINDING],
        &HashMap::new(),
        &HashMap::new(),
        &confirmation_buffer,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        Timestamp::now(),
    );

    assert!(
        !view
            .caveats
            .iter()
            .any(|caveat| caveat.id == "source.pressure.confirmation_buffer.retained_payload"),
        "unattributed/global pressure must stay in runtime health instead of becoming source-local"
    );
    assert!(
        !view
            .actions
            .iter()
            .any(|action| action.id == "runtime.health.inspect"),
        "global pressure without source ownership should not create source-local actions"
    );
    Ok(())
}

fn healthy_confirmation_buffer() -> ConfirmationBufferHealth {
    ConfirmationBufferHealth {
        status: HealthStatus::Healthy,
        connected: true,
        memory_owner: crate::api::service_container::ConfirmationBufferMemoryOwner::None,
        pressure_level: sinex_primitives::RuntimePressureLevel::Nominal,
        runtime_action: sinex_primitives::RuntimePressureAction::Admit,
        observed_buffers: 0,
        pending_count: 0,
        timed_out_retained_count: 0,
        rejected_count: 0,
        late_confirmation_count: 0,
        retained_payload_bytes: 0,
        approximate_payload_bytes: 0,
        active_payload_bytes: 0,
        timed_out_retained_payload_bytes: 0,
        approximate_payload_bytes_by_kind: BTreeMap::new(),
        detail: "confirmation buffers nominal".to_string(),
    }
}

fn terminal_bridge_contract() -> SourceContract {
    SourceContract {
        id: "terminal.kitty-osc-live",
        namespace: "terminal",
        event_types: &[("shell.kitty", "command.executed")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_scope: AccessScope::RuntimeBridge {
            surface: "kitty_osc",
        },
    }
}

fn terminal_bridge_binding() -> SourceRuntimeBinding {
    static BRIDGE_CAPABILITIES: &[&str] = &[
        "coverage:source-coverage",
        "operation:terminal.activity.check",
        "operation:terminal.activity.reconnect",
        "operation:terminal.activity.pause",
        "operation:terminal.activity.resume",
        "operation:terminal.activity.drain",
        "operation:terminal.activity.inspect",
    ];
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:terminal.kitty-osc-live"),
        "terminal.kitty-osc-live",
        "terminal",
    )
    .implementation("live-capture")
    .adapter("UnixSocketStreamAdapter")
    .output_event_type("command.executed")
    .privacy_context(ProcessingContext::Command)
    .resource_profile(ResourceProfile::LiveWatcher)
    .capabilities(BRIDGE_CAPABILITIES)
    .source_id("terminal.kitty-osc-live")
    .runner_pack(RunnerPack::Live)
    .checkpoint_family(CheckpointFamily::LiveObservation)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

trait SourceStatusTestExt {
    fn with_recent_output(self, last_output_at: Timestamp, recent_output_count: i64) -> Self;
}

impl SourceStatusTestExt for SourceStatus {
    fn with_recent_output(mut self, last_output_at: Timestamp, recent_output_count: i64) -> Self {
        self.last_output_at = Some(last_output_at);
        self.recent_output_count = recent_output_count;
        self
    }
}

fn terminal_bridge_status(now: Timestamp) -> SourceStatus {
    SourceStatus {
        module_name: ModuleName::new("terminal-source"),
        version: "1.0.0".to_string(),
        description: Some("Kitty OSC live terminal bridge".to_string()),
        manifest_status: "running".to_string(),
        live: true,
        service_name: Some("sinexd".to_string()),
        instance_id: Some("fixture-instance".to_string()),
        module_run_id: None,
        host: Some("fixture-host".to_string()),
        run_status: Some("running".to_string()),
        started_at: Some(now - time::Duration::seconds(3600)),
        last_heartbeat_at: Some(now - time::Duration::seconds(5)),
        current_health: Some(HealthStatus::Healthy),
        health_changed_at: Some(now - time::Duration::seconds(5)),
        health_reason: Some("bridge connected".to_string()),
        recent_output_count: 0,
        last_output_at: None,
    }
}

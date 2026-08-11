use sinex_primitives::{
    DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID, DESKTOP_FOCUS_SESSION_DERIVATION_ID,
    DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID, DESKTOP_PROJECT_CONTEXT_DERIVATION_ID,
    DERIVATION_SPECS, DerivationInputScope, DerivationOperationHook,
    EMAIL_ATTACHMENT_INDEX_DERIVATION_ID, EMAIL_BODY_TEXT_PROJECTION_DERIVATION_ID,
    EMAIL_THREAD_PROJECTION_DERIVATION_ID, FreshnessPolicy, InvalidationTrigger,
    MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION_ID, MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION_ID,
    MEDIA_TEXT_INDEX_PROJECTION_DERIVATION_ID, OutputKind, TASK_CURRENT_OBJECTS_DERIVATION_ID,
    affected_derivations, derivations_for_output, find_derivation_spec,
    task_domain::{TASK_REDUCER_INPUT_EVENT_TYPES, TASK_REDUCER_SPEC},
};
use std::collections::BTreeSet;
use xtask::sandbox::prelude::*;

/// Registry-wide invariants that hold across EVERY `DerivationSpec`,
/// regardless of family — proving these once here (rather than re-asserting
/// per-family below) is what keeps the per-family tests focused on what's
/// actually distinct about each family (event types, output kind,
/// disclosure refs) instead of re-checking the same cross-cutting shape N
/// times.
#[sinex_test]
async fn derivation_registry_invariants_hold_across_all_specs() -> TestResult<()> {
    // No two specs share a declaration id — the registry is the source of
    // truth `find_derivation_spec` linearly searches, so a duplicate id
    // would make lookup silently return the wrong spec.
    let ids: Vec<_> = DERIVATION_SPECS.iter().map(|spec| spec.id).collect();
    let unique_ids: BTreeSet<_> = ids.iter().copied().collect();
    assert_eq!(
        ids.len(),
        unique_ids.len(),
        "duplicate DerivationSpec.id in DERIVATION_SPECS: {ids:?}"
    );

    // Every registered derivation must be able to say what happens to it on
    // replay and on redaction — these are the two invalidation triggers the
    // architecture treats as universal (CLAUDE.md: "Replay is not
    // idempotent by design"; "Privacy/redaction is a presentation feature"
    // still requires every derived output to know it must be rebuilt). A
    // spec missing either is a silent invalidation-planning gap.
    for spec in DERIVATION_SPECS {
        assert!(
            spec.invalidates_on(InvalidationTrigger::Replay),
            "{} does not invalidate on Replay",
            spec.id
        );
        assert!(
            spec.invalidates_on(InvalidationTrigger::Redaction),
            "{} does not invalidate on Redaction",
            spec.id
        );
        // Every spec must document what happens to it under an
        // invalidation-planning query, i.e. it must find itself.
        assert_eq!(
            find_derivation_spec(spec.id).map(|found| found.id),
            Some(spec.id)
        );
    }

    // Negative paths: unknown id/output must not panic or silently match
    // an unrelated spec.
    assert!(find_derivation_spec("derivation:does.not.exist@v1").is_none());
    assert_eq!(
        derivations_for_output("does.not.exist").collect::<Vec<_>>().len(),
        0
    );
    Ok(())
}

/// The `desktop.*` family (context/focus-session/project-context/
/// notification-pressure) had zero coverage before this lane — a real gap,
/// not a redundant re-check of the task/email/media families above.
#[sinex_test]
async fn desktop_derivations_declare_ephemeral_and_projection_outputs() -> TestResult<()> {
    let ephemeral = find_derivation_spec(DESKTOP_CONTEXT_CURRENT_VIEW_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing desktop context-view derivation spec"))?;
    assert_eq!(ephemeral.output_kind, OutputKind::EphemeralView);
    assert_eq!(ephemeral.freshness_policy, FreshnessPolicy::RefreshOnRead);
    assert!(
        !ephemeral.invalidates_on(InvalidationTrigger::Archive),
        "an ephemeral view has nothing durable for Archive to invalidate"
    );

    let projection_specs = [
        (
            DESKTOP_FOCUS_SESSION_DERIVATION_ID,
            "desktop.focus_session",
        ),
        (
            DESKTOP_PROJECT_CONTEXT_DERIVATION_ID,
            "desktop.project_context",
        ),
    ];
    for (id, output_id) in projection_specs {
        let spec = find_derivation_spec(id)
            .ok_or_else(|| color_eyre::eyre::eyre!("missing desktop derivation spec: {id}"))?;
        assert_eq!(spec.output_id, output_id);
        assert_eq!(spec.output_kind, OutputKind::ProjectionRow);
        assert_eq!(spec.freshness_policy, FreshnessPolicy::RebuildOnInputChange);
        assert!(spec.invalidates_on(InvalidationTrigger::Archive));
    }

    let pressure = find_derivation_spec(DESKTOP_NOTIFICATION_PRESSURE_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing notification-pressure derivation spec"))?;
    assert_eq!(pressure.output_kind, OutputKind::ProjectionRow);
    match pressure.input_scope {
        DerivationInputScope::EventTypes {
            domain_id,
            event_types,
        } => {
            assert_eq!(domain_id, "desktop.notification");
            assert!(event_types.contains(&"notification.sent"));
            assert!(event_types.contains(&"notification.closed"));
        }
        other => panic!("notification pressure should use EventTypes scope, got {other:?}"),
    }
    // Unlike the other desktop specs, notification-pressure does NOT
    // invalidate on Archive (verified against the live const above) —
    // pinned here so a future edit to that const is caught either way.
    assert!(!pressure.invalidates_on(InvalidationTrigger::Archive));

    Ok(())
}

#[sinex_test]
async fn task_projection_declares_derivation_contract() -> TestResult<()> {
    let spec = find_derivation_spec(TASK_CURRENT_OBJECTS_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing task derivation spec"))?;

    assert_eq!(spec.output_id, "domain.current_objects");
    assert_eq!(spec.output_kind, OutputKind::ProjectionRow);
    assert_eq!(spec.output_kind, TASK_REDUCER_SPEC.output_kind);
    assert_eq!(spec.freshness_policy, FreshnessPolicy::RebuildOnInputChange);
    assert!(
        spec.operation_hooks
            .contains(&DerivationOperationHook::Rebuild)
    );
    assert!(
        spec.operation_hooks
            .contains(&DerivationOperationHook::Explain)
    );
    Ok(())
}

#[sinex_test]
async fn email_derivations_declare_projection_outputs_and_invalidation() -> TestResult<()> {
    let expected = [
        (
            EMAIL_THREAD_PROJECTION_DERIVATION_ID,
            "core.email_mailbox_projection.thread",
            "email.thread.observed",
        ),
        (
            EMAIL_BODY_TEXT_PROJECTION_DERIVATION_ID,
            "core.email_mailbox_projection.body_text",
            "email.message.received",
        ),
        (
            EMAIL_ATTACHMENT_INDEX_DERIVATION_ID,
            "core.email_mailbox_projection.attachment_index",
            "email.attachment.observed",
        ),
    ];

    for (id, output_id, event_type) in expected {
        let spec = find_derivation_spec(id)
            .ok_or_else(|| color_eyre::eyre::eyre!("missing email derivation spec: {id}"))?;
        assert_eq!(spec.output_id, output_id);
        assert_eq!(spec.output_kind, OutputKind::ProjectionRow);
        assert_eq!(spec.freshness_policy, FreshnessPolicy::RebuildOnInputChange);
        assert!(spec.invalidates_on(InvalidationTrigger::Replay));
        assert!(spec.invalidates_on(InvalidationTrigger::Redaction));
        assert!(spec.invalidates_on(InvalidationTrigger::DisclosurePolicyChange));
        assert!(
            spec.operation_hooks
                .contains(&DerivationOperationHook::Rebuild)
        );
        assert!(
            spec.operation_hooks
                .contains(&DerivationOperationHook::Explain)
        );
        assert!(
            spec.operation_hooks
                .contains(&DerivationOperationHook::Redact)
        );
        match spec.input_scope {
            DerivationInputScope::EventTypes {
                domain_id,
                event_types,
            } => {
                assert_eq!(domain_id, "email.mailbox");
                assert!(
                    event_types.contains(&event_type),
                    "{id} should depend on {event_type}, got {event_types:?}"
                );
            }
            other => panic!("email derivation should use email.mailbox EventTypes, got {other:?}"),
        }

        let output_ids: Vec<_> = derivations_for_output(output_id)
            .map(|spec| spec.id)
            .collect();
        assert_eq!(output_ids, vec![id]);
    }

    let source_material_change_ids: Vec<_> =
        affected_derivations(InvalidationTrigger::SourceMaterialChange)
            .map(|spec| spec.id)
            .collect();
    assert!(source_material_change_ids.contains(&EMAIL_THREAD_PROJECTION_DERIVATION_ID));
    assert!(source_material_change_ids.contains(&EMAIL_BODY_TEXT_PROJECTION_DERIVATION_ID));
    assert!(source_material_change_ids.contains(&EMAIL_ATTACHMENT_INDEX_DERIVATION_ID));
    Ok(())
}

#[sinex_test]
async fn derivation_contract_keeps_input_scope_with_projection_spec() -> TestResult<()> {
    let spec = find_derivation_spec(TASK_CURRENT_OBJECTS_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing task derivation spec"))?;

    match spec.input_scope {
        sinex_primitives::DerivationInputScope::EventTypes {
            domain_id,
            event_types,
        } => {
            assert_eq!(domain_id, TASK_REDUCER_SPEC.domain_id);
            assert_eq!(event_types, TASK_REDUCER_INPUT_EVENT_TYPES);
        }
        other => panic!("task derivation should use event input scope, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn invalidation_planning_reports_affected_derivations() -> TestResult<()> {
    let replay_ids: Vec<_> = affected_derivations(InvalidationTrigger::Replay)
        .map(|spec| spec.id)
        .collect();
    assert!(replay_ids.contains(&TASK_CURRENT_OBJECTS_DERIVATION_ID));

    let redaction_ids: Vec<_> = affected_derivations(InvalidationTrigger::Redaction)
        .map(|spec| spec.id)
        .collect();
    assert!(redaction_ids.contains(&TASK_CURRENT_OBJECTS_DERIVATION_ID));

    let output_ids: Vec<_> = derivations_for_output("domain.current_objects")
        .map(|spec| spec.id)
        .collect();
    assert_eq!(output_ids, vec![TASK_CURRENT_OBJECTS_DERIVATION_ID]);
    Ok(())
}

#[sinex_test]
async fn media_derivations_declare_artifact_projection_outputs_and_invalidation() -> TestResult<()>
{
    let transcript = find_derivation_spec(MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing media transcript derivation spec"))?;
    assert_eq!(transcript.output_id, "media.audio.transcript_artifact");
    assert_eq!(transcript.output_kind, OutputKind::Artifact);
    assert_eq!(
        transcript.disclosure_policy_ref,
        Some("operator.media.audio-transcript.default")
    );
    assert!(
        transcript
            .operation_hooks
            .contains(&DerivationOperationHook::Redact)
    );
    assert!(transcript.invalidates_on(InvalidationTrigger::SourceMaterialChange));
    match transcript.input_scope {
        DerivationInputScope::EventTypes {
            domain_id,
            event_types,
        } => {
            assert_eq!(domain_id, "media.audio");
            assert!(event_types.contains(&"media.audio.transcript_segment_observed"));
            assert!(event_types.contains(&"media.audio.transcription_run_observed"));
        }
        other => {
            panic!("audio transcript artifact should use media.audio EventTypes, got {other:?}")
        }
    }

    let ocr = find_derivation_spec(MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing media OCR derivation spec"))?;
    assert_eq!(ocr.output_id, "media.screen.ocr_artifact");
    assert_eq!(ocr.output_kind, OutputKind::Artifact);
    assert!(ocr.invalidates_on(InvalidationTrigger::DisclosurePolicyChange));
    match ocr.input_scope {
        DerivationInputScope::EventTypes {
            domain_id,
            event_types,
        } => {
            assert_eq!(domain_id, "media.screen");
            assert!(event_types.contains(&"media.screen.ocr_segment_observed"));
            assert!(event_types.contains(&"media.screen.ocr_run_observed"));
        }
        other => panic!("screen OCR artifact should use media.screen EventTypes, got {other:?}"),
    }

    let text_index = find_derivation_spec(MEDIA_TEXT_INDEX_PROJECTION_DERIVATION_ID)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing media text index derivation spec"))?;
    assert_eq!(text_index.output_id, "media.text_index_projection");
    assert_eq!(text_index.output_kind, OutputKind::ProjectionRow);
    assert!(
        text_index
            .operation_hooks
            .contains(&DerivationOperationHook::Rebuild)
    );

    let output_ids: Vec<_> = derivations_for_output("media.text_index_projection")
        .map(|spec| spec.id)
        .collect();
    assert_eq!(output_ids, vec![MEDIA_TEXT_INDEX_PROJECTION_DERIVATION_ID]);

    let source_material_change_ids: Vec<_> =
        affected_derivations(InvalidationTrigger::SourceMaterialChange)
            .map(|spec| spec.id)
            .collect();
    assert!(source_material_change_ids.contains(&MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION_ID));
    assert!(source_material_change_ids.contains(&MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION_ID));
    assert!(source_material_change_ids.contains(&MEDIA_TEXT_INDEX_PROJECTION_DERIVATION_ID));

    Ok(())
}

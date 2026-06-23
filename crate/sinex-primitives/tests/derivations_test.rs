use sinex_primitives::{
    DerivationInputScope, DerivationOperationHook, EMAIL_ATTACHMENT_INDEX_DERIVATION_ID,
    EMAIL_BODY_TEXT_PROJECTION_DERIVATION_ID, EMAIL_THREAD_PROJECTION_DERIVATION_ID,
    FreshnessPolicy, InvalidationTrigger, MEDIA_AUDIO_TRANSCRIPT_ARTIFACT_DERIVATION_ID,
    MEDIA_SCREEN_OCR_ARTIFACT_DERIVATION_ID, MEDIA_TEXT_INDEX_PROJECTION_DERIVATION_ID, OutputKind,
    TASK_CURRENT_OBJECTS_DERIVATION_ID, affected_derivations, derivations_for_output,
    find_derivation_spec,
    task_domain::{TASK_REDUCER_INPUT_EVENT_TYPES, TASK_REDUCER_SPEC},
};
use xtask::sandbox::prelude::*;

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

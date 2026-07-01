use super::{
    dlq_operator_action, dlq_pending_sequence_span, dlq_pressure_level, dlq_pressure_signal,
    parse_retry_count_header, payload_preview,
};
use crate::api::handlers::query::event_card_list_with_policy;
use crate::event_engine::policy::PolicyEngine;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::error::ErrorClass;
use sinex_primitives::events::DynamicPayload;
use sinex_primitives::query::QueryResultEvent;
use sinex_primitives::views::PrivacyStateKind;
use sinex_primitives::{Id, RuntimePressureAction, RuntimePressureLevel, SourceMaterial, Uuid};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn parse_retry_count_header_defaults_when_missing() -> TestResult<()> {
    assert_eq!(parse_retry_count_header(None)?, 0);
    Ok(())
}

#[sinex_test]
async fn parse_retry_count_header_rejects_invalid_value() -> TestResult<()> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Retry-Count", "not-a-number");

    let error = parse_retry_count_header(Some(&headers))
        .expect_err("invalid Retry-Count header should fail honestly");

    assert_eq!(error.error_class(), ErrorClass::DataError);
    assert!(error.to_string().contains("Retry-Count header is invalid"));
    assert!(error.to_string().contains("not-a-number"));
    Ok(())
}

#[sinex_test]
async fn dlq_list_pressure_classifies_empty_warning_and_critical_depth() -> TestResult<()> {
    assert_eq!(dlq_pressure_level(0, 10), RuntimePressureLevel::Nominal);
    assert_eq!(dlq_pressure_level(10, 10), RuntimePressureLevel::Warning);
    assert_eq!(dlq_pressure_level(11, 10), RuntimePressureLevel::Critical);

    Ok(())
}

#[sinex_test]
async fn dlq_list_pressure_reports_sequence_span_and_action() -> TestResult<()> {
    assert_eq!(dlq_pending_sequence_span(0, 4, 9), 0);
    assert_eq!(dlq_pending_sequence_span(2, 4, 9), 6);
    assert_eq!(dlq_pending_sequence_span(2, 9, 4), 0);

    assert_eq!(dlq_operator_action(0), ("none", "raw-ingest DLQ is empty"));
    assert_eq!(
        dlq_operator_action(1),
        (
            "ops dlq cleanup-plan --all-retained",
            "classify retained failures before running paced requeue or purge"
        )
    );

    Ok(())
}

#[sinex_test]
async fn dlq_pressure_signal_carries_runtime_action_and_batch_limit() -> TestResult<()> {
    let pressure = dlq_pressure_signal(11, 4096, 10);

    assert_eq!(pressure.pressure_level, RuntimePressureLevel::Critical);
    assert_eq!(pressure.runtime_action, RuntimePressureAction::Throttle);
    assert_eq!(pressure.pending_messages, 11);
    assert_eq!(pressure.pending_bytes, 4096);
    assert_eq!(pressure.retry_batch_size, 10);
    assert_eq!(
        pressure.recommended_action,
        "ops dlq cleanup-plan --all-retained"
    );
    assert!(pressure.reason.contains("paced requeue or purge"));
    Ok(())
}

#[sinex_test]
async fn payload_preview_truncates_without_breaking_unicode(ctx: TestContext) -> TestResult<()> {
    let payload = "żółw".repeat(80);
    let policy = PolicyEngine::noop(ctx.pool().clone());
    let preview = payload_preview(&payload, 200, &policy).await;
    assert!(preview.text.ends_with("..."));
    assert_eq!(preview.text.chars().count(), 203);
    assert!(!preview.redacted);
    assert!(preview.caveats.is_empty());
    Ok(())
}

#[sinex_test]
async fn payload_preview_keeps_structured_error_before_bulky_context(
    ctx: TestContext,
) -> TestResult<()> {
    let dlq_payload = json!({
        "context": {
            "error": format!(
                "Database error: Failed to insert blob metadata (finalization_stage: upsert_blob)\n{}",
                "duplicate key ".repeat(40)
            )
        },
        "material_id": "019f16cc-dd56-7ab3-8aff-ea3f29e79932",
        "error": "material_persist_failed",
        "failed_at": "2026-06-30T04:32:32.690891732Z"
    })
    .to_string();
    let policy = PolicyEngine::noop(ctx.pool().clone());

    let preview = payload_preview(&dlq_payload, 200, &policy).await;

    assert!(
        preview
            .text
            .starts_with("{\"error\":\"material_persist_failed\""),
        "preview should keep the classifier before truncation: {}",
        preview.text
    );
    assert!(preview.text.contains("\"material_id\""));
    assert!(preview.text.ends_with("..."));
    assert!(!preview.redacted);
    assert!(preview.caveats.is_empty());
    Ok(())
}

#[sinex_test]
async fn payload_preview_keeps_nested_material_corruption_reason(
    ctx: TestContext,
) -> TestResult<()> {
    let dlq_payload = json!({
        "material_id": "019f17d7-2ba6-7b01-9f0a-017a3d025d14",
        "error": "material assembly corruption detected",
        "context": {
            "assembled_bytes": 1825,
            "buffered_offsets": (0..80).collect::<Vec<_>>(),
            "end": {
                "content_hash": "9b73f5cebf7da60a8648c445f2df2f5539d0f2a6e860a4352bb6e1b309444944",
                "ended_at": "2026-06-30T09:23:32.490226739Z"
            },
            "expected_bytes": 1284,
            "expected_slices": 3,
            "reason": "assembled_bytes=1825 exceeds expected_bytes=1284",
            "slice_count": 4
        },
        "failed_at": "2026-06-30T09:23:34.712417662Z"
    })
    .to_string();
    let policy = PolicyEngine::noop(ctx.pool().clone());

    let preview = payload_preview(&dlq_payload, 260, &policy).await;

    assert!(preview.text.contains("\"context\":{\"reason\""));
    assert!(
        preview
            .text
            .contains("assembled_bytes=1825 exceeds expected_bytes=1284"),
        "preview should expose the nested corruption reason before truncation: {}",
        preview.text
    );
    assert!(preview.text.contains("\"assembled_bytes\":1825"));
    assert!(preview.text.contains("\"expected_bytes\":1284"));
    assert!(preview.text.ends_with("..."));
    Ok(())
}

#[sinex_test]
async fn disclosure_policy_leak_fixture_covers_event_cards_and_dlq(
    ctx: TestContext,
) -> TestResult<()> {
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "event-card-command-secret",
            "test field-scoped disclosure policy for rendered event cards",
            "regex",
            r"evt_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<COMMAND_SECRET>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule(
            "event-card-command-secret",
            Some("shell.history"),
            Some("command.imported"),
            Some("command"),
            0,
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "dlq-preview-secret",
            "test global disclosure policy for untyped DLQ previews",
            "regex",
            r"dlq_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<DLQ_SECRET>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("dlq-preview-secret", None, None, None, 0)
        .await?;
    let policy = PolicyEngine::load(ctx.pool().clone()).await?;
    let command_token = "evt_secret_alpha123";
    let dlq_token = "dlq_secret_bravo456";
    let command = format!("export COMMAND_SECRET={command_token}");

    let material_id = Id::<SourceMaterial>::from_uuid(Uuid::from_u128(0x1693));
    let event = DynamicPayload::new(
        "shell.history",
        "command.imported",
        json!({ "command": command, "cwd": "/home/sinity/private" }),
    )
    .from_material(material_id)
    .build()?;
    let cards = event_card_list_with_policy(
        &[QueryResultEvent {
            event,
            relevance_score: Some(1.0),
            snippet: Some(format!(
                "matched command: export COMMAND_SECRET={command_token}"
            )),
        }],
        &policy,
    )
    .await;
    let cards_json = serde_json::to_string(&cards)?;

    assert!(
        !cards_json.contains(command_token),
        "event-card view must not leak the fixture token: {cards_json}"
    );
    assert!(
        cards_json.contains("<COMMAND_SECRET>"),
        "event-card view must show the operator-owned replacement label: {cards_json}"
    );
    assert_eq!(
        cards.cards[0].privacy_state.state,
        PrivacyStateKind::Redacted
    );
    assert!(
        cards.cards[0]
            .caveats
            .iter()
            .any(|caveat| caveat.id == "policy.disclosure_applied"),
        "event-card redaction must be caveated: {:?}",
        cards.cards[0].caveats
    );

    let dlq_payload = format!(
        r#"{{
        "original_subject": "dev.sinex.events.raw.shell.command",
        "original_payload": {{ "command": "export DLQ_SECRET={dlq_token}" }}
    }}"#
    );
    let preview = payload_preview(&dlq_payload, 400, &policy).await;

    assert!(preview.redacted);
    assert!(
        !preview.text.contains(dlq_token),
        "DLQ preview must not leak the fixture token: {}",
        preview.text
    );
    assert!(
        preview.text.contains("<DLQ_SECRET>"),
        "DLQ preview must show the operator-owned replacement label: {}",
        preview.text
    );
    assert!(
        preview
            .caveats
            .iter()
            .any(|caveat| caveat.id == "policy.disclosure_applied"),
        "DLQ redaction must be caveated: {:?}",
        preview.caveats
    );
    assert!(
        preview.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .is_some_and(|ref_| ref_.id == "db.dlq-preview-secret")),
        "DLQ redaction must name the operator-owned policy rule: {:?}",
        preview.caveats
    );

    Ok(())
}

#[sinex_test]
async fn media_disclosure_policy_covers_event_cards_snippets_and_dlq_previews(
    ctx: TestContext,
) -> TestResult<()> {
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "media-transcript-text",
            "test field-scoped disclosure policy for audio transcript text",
            "regex",
            r"audio_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<MEDIA_TRANSCRIPT>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule(
            "media-transcript-text",
            Some("media.audio"),
            Some("media.audio.transcript_segment_observed"),
            Some("text"),
            0,
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "media-screen-text",
            "test field-scoped disclosure policy for screen OCR text",
            "regex",
            r"screen_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<MEDIA_OCR>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule(
            "media-screen-text",
            Some("media.screen"),
            Some("media.screen.ocr_segment_observed"),
            Some("text"),
            0,
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "media-window-title",
            "test field-scoped disclosure policy for captured window titles",
            "regex",
            r"window_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<MEDIA_WINDOW>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule(
            "media-window-title",
            Some("media.screen"),
            Some("media.screen.ocr_segment_observed"),
            Some("window_title"),
            0,
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "media-dlq-model-log",
            "test global disclosure policy for media worker/model logs in DLQ previews",
            "regex",
            r"model_log_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<MEDIA_MODEL_LOG>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("media-dlq-model-log", None, None, None, 0)
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "media-dlq-ocr-text",
            "test global disclosure policy for media OCR text in DLQ previews",
            "regex",
            r"screen_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<MEDIA_OCR>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("media-dlq-ocr-text", None, None, None, 0)
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "media-dlq-window-title",
            "test global disclosure policy for captured window titles in DLQ previews",
            "regex",
            r"window_secret_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<MEDIA_WINDOW>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("media-dlq-window-title", None, None, None, 0)
        .await?;

    let policy = PolicyEngine::load(ctx.pool().clone()).await?;
    let material_id = Id::<SourceMaterial>::from_uuid(Uuid::from_u128(0x1043));
    let audio_token = "audio_secret_alpha123";
    let screen_token = "screen_secret_bravo456";
    let window_token = "window_secret_charlie789";
    let model_log_token = "model_log_secret_delta000";

    let audio_event = DynamicPayload::new(
        "media.audio",
        "media.audio.transcript_segment_observed",
        json!({
            "segment_index": 1,
            "text": format!("operator said {audio_token} during capture"),
            "start_ms": 0,
            "end_ms": 1200,
            "speaker_label": "operator",
            "language": "en",
            "confidence": 0.98,
            "source_file": "meeting.wav",
            "raw_material_id": "raw-audio-1043",
            "model_id": "whisper-fixture",
            "producer_run_id": "producer-run-audio",
            "timestamp_quality": "media_time",
            "observed_at": "2026-06-23T11:00:00Z"
        }),
    )
    .from_material(material_id)
    .build()?;
    let screen_event = DynamicPayload::new(
        "media.screen",
        "media.screen.ocr_segment_observed",
        json!({
            "segment_index": 2,
            "text": format!("screen showed {screen_token}"),
            "bbox": [10, 20, 300, 80],
            "confidence": 0.91,
            "display_id": "DP-1",
            "window_title": format!("terminal {window_token}"),
            "source_file": "screen.png",
            "raw_material_id": "raw-screen-1043",
            "engine": "tesseract-fixture",
            "producer_run_id": "producer-run-screen",
            "timestamp_quality": "capture_time",
            "observed_at": "2026-06-23T11:00:01Z"
        }),
    )
    .from_material(material_id)
    .build()?;

    let cards = event_card_list_with_policy(
        &[
            QueryResultEvent {
                event: audio_event,
                relevance_score: Some(1.0),
                snippet: Some(format!("audio transcript match: {audio_token}")),
            },
            QueryResultEvent {
                event: screen_event,
                relevance_score: Some(1.0),
                snippet: Some(format!("OCR match {screen_token} in window {window_token}")),
            },
        ],
        &policy,
    )
    .await;
    let cards_json = serde_json::to_string(&cards)?;

    for token in [audio_token, screen_token, window_token] {
        assert!(
            !cards_json.contains(token),
            "media event cards/snippets must not leak fixture token {token}: {cards_json}"
        );
    }
    for replacement in ["<MEDIA_TRANSCRIPT>", "<MEDIA_OCR>", "<MEDIA_WINDOW>"] {
        assert!(
            cards_json.contains(replacement),
            "media event cards should show replacement label {replacement}: {cards_json}"
        );
    }
    assert_eq!(cards.cards.len(), 2);
    for card in &cards.cards {
        assert_eq!(card.privacy_state.state, PrivacyStateKind::Redacted);
        assert!(
            card.caveats
                .iter()
                .any(|caveat| caveat.id == "policy.disclosure_applied"),
            "media card redaction must be caveated: {:?}",
            card.caveats
        );
    }

    let dlq_payload = format!(
        r#"{{
        "original_subject": "dev.sinex.events.raw.media.worker",
        "original_payload": {{
            "source": "media.screen",
            "event_type": "media.screen.ocr_run_observed",
            "stderr": "OCR model failed after logging {model_log_token}",
            "worker_output": {{
                "text": "{screen_token}",
                "window_title": "{window_token}"
            }}
        }}
    }}"#
    );
    let preview = payload_preview(&dlq_payload, 600, &policy).await;

    assert!(preview.redacted);
    for token in [model_log_token, screen_token, window_token] {
        assert!(
            !preview.text.contains(token),
            "media DLQ preview must not leak fixture token {token}: {}",
            preview.text
        );
    }
    for replacement in ["<MEDIA_MODEL_LOG>", "<MEDIA_OCR>", "<MEDIA_WINDOW>"] {
        assert!(
            preview.text.contains(replacement),
            "media DLQ preview must show replacement {replacement}: {}",
            preview.text
        );
    }
    assert!(
        preview
            .caveats
            .iter()
            .any(|caveat| caveat.id == "policy.disclosure_applied"),
        "media DLQ redaction must be caveated: {:?}",
        preview.caveats
    );
    assert!(
        preview.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .is_some_and(|ref_| ref_.id == "db.media-dlq-model-log")),
        "media DLQ redaction must name the model-log policy: {:?}",
        preview.caveats
    );
    assert!(
        preview.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .is_some_and(|ref_| ref_.id == "db.media-dlq-ocr-text")),
        "media DLQ redaction must name the OCR policy: {:?}",
        preview.caveats
    );
    assert!(
        preview.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .is_some_and(|ref_| ref_.id == "db.media-dlq-window-title")),
        "media DLQ redaction must name the window-title policy: {:?}",
        preview.caveats
    );

    Ok(())
}

#[sinex_test]
async fn email_disclosure_policy_covers_subject_recipients_attachments_and_dlq_previews(
    ctx: TestContext,
) -> TestResult<()> {
    // Field-scoped View disclosure: subject (scalar), Bcc (array element),
    // and attachment filename (different event type) must redact in rendered
    // event cards/snippets without an operator opting every field in globally.
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "email-subject-secret",
            "test field-scoped disclosure policy for email subjects",
            "regex",
            r"email_secret_subject_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<EMAIL_SUBJECT>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule(
            "email-subject-secret",
            Some("email"),
            Some("email.message.received"),
            Some("subject"),
            0,
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "email-bcc-secret",
            "test field-scoped disclosure policy for email Bcc recipients",
            "regex",
            r"email_secret_recipient_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<EMAIL_BCC>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule(
            "email-bcc-secret",
            Some("email"),
            Some("email.message.received"),
            Some("bcc"),
            0,
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "email-attachment-name-secret",
            "test field-scoped disclosure policy for email attachment filenames",
            "regex",
            r"email_secret_attach_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<EMAIL_ATTACHMENT>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule(
            "email-attachment-name-secret",
            Some("email"),
            Some("email.attachment.observed"),
            Some("filename"),
            0,
        )
        .await?;
    // Global DLQ disclosure: provider material previews and raw subject/body
    // bytes that surface in a dead-letter preview must redact even though the
    // failed payload is untyped JSON.
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "email-dlq-provider-secret",
            "test global disclosure policy for email provider material in DLQ previews",
            "regex",
            r"email_secret_provider_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<EMAIL_PROVIDER>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("email-dlq-provider-secret", None, None, None, 0)
        .await?;
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "email-dlq-subject-secret",
            "test global disclosure policy for email subjects in DLQ previews",
            "regex",
            r"email_secret_subject_[A-Za-z0-9_]+",
            false,
            "redact",
            Some("<EMAIL_SUBJECT>"),
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("email-dlq-subject-secret", None, None, None, 0)
        .await?;

    let policy = PolicyEngine::load(ctx.pool().clone()).await?;
    let material_id = Id::<SourceMaterial>::from_uuid(Uuid::from_u128(0x1469));
    let subject_token = "email_secret_subject_alpha123";
    let bcc_token = "email_secret_recipient_bravo456";
    let attachment_token = "email_secret_attach_charlie789";
    let provider_token = "email_secret_provider_delta000";

    let message_event = DynamicPayload::new(
        "email",
        "email.message.received",
        json!({
            "message_id": "<msg-1469@example.test>",
            "date": "2026-06-24T09:00:00Z",
            "from": ["sender@example.test"],
            "to": ["primary@example.test"],
            "cc": [],
            "bcc": [format!("hidden+{bcc_token}@example.test")],
            "subject": format!("quarterly numbers {subject_token}"),
            "in_reply_to": null,
            "references": [],
            "list_id": null,
            "folder": "INBOX",
            "source_file": "inbox.mbox",
            "raw_material_id": "raw-email-1469",
            "mailbox_format": "rfc822",
            "maildir_subdir": null,
            "maildir_flags": [],
            "maildir_stable_filename": null,
            "mbox_file": null,
            "mbox_byte_start": null,
            "mbox_byte_end": null,
            "size_bytes": 2048,
            "body_bytes": 1024,
            "attachment_count": 1
        }),
    )
    .from_material(material_id)
    .build()?;
    let attachment_event = DynamicPayload::new(
        "email",
        "email.attachment.observed",
        json!({
            "message_id": "<msg-1469@example.test>",
            "folder": "INBOX",
            "source_file": "inbox.mbox",
            "raw_material_id": "raw-email-1469",
            "mailbox_format": "rfc822",
            "attachment_index": 0,
            "disposition": "attachment",
            "filename": format!("{attachment_token}.pdf"),
            "content_type": "application/pdf",
            "content_id": null,
            "material_policy_ref": "policy.email.attachment.deferred"
        }),
    )
    .from_material(material_id)
    .build()?;

    let cards = event_card_list_with_policy(
        &[
            QueryResultEvent {
                event: message_event,
                relevance_score: Some(1.0),
                snippet: Some(format!("subject match: {subject_token}; bcc {bcc_token}")),
            },
            QueryResultEvent {
                event: attachment_event,
                relevance_score: Some(1.0),
                snippet: Some(format!("attachment match {attachment_token}.pdf")),
            },
        ],
        &policy,
    )
    .await;
    let cards_json = serde_json::to_string(&cards)?;

    for token in [subject_token, bcc_token, attachment_token] {
        assert!(
            !cards_json.contains(token),
            "email event cards/snippets must not leak fixture token {token}: {cards_json}"
        );
    }
    for replacement in ["<EMAIL_SUBJECT>", "<EMAIL_BCC>", "<EMAIL_ATTACHMENT>"] {
        assert!(
            cards_json.contains(replacement),
            "email event cards should show replacement label {replacement}: {cards_json}"
        );
    }
    assert_eq!(cards.cards.len(), 2);
    for card in &cards.cards {
        assert_eq!(card.privacy_state.state, PrivacyStateKind::Redacted);
        assert!(
            card.caveats
                .iter()
                .any(|caveat| caveat.id == "policy.disclosure_applied"),
            "email card redaction must be caveated: {:?}",
            card.caveats
        );
    }

    let dlq_payload = format!(
        r#"{{
        "original_subject": "dev.sinex.events.raw.email.message",
        "original_payload": {{
            "source": "email",
            "event_type": "email.message.received",
            "subject": "quarterly numbers {subject_token}",
            "provider_material": {{
                "source": "imap_provider_body_snapshot",
                "raw_message_preview": "From: ceo@example.test\nSecret token {provider_token}"
            }}
        }}
    }}"#
    );
    let preview = payload_preview(&dlq_payload, 600, &policy).await;

    assert!(preview.redacted);
    for token in [subject_token, provider_token] {
        assert!(
            !preview.text.contains(token),
            "email DLQ preview must not leak fixture token {token}: {}",
            preview.text
        );
    }
    for replacement in ["<EMAIL_SUBJECT>", "<EMAIL_PROVIDER>"] {
        assert!(
            preview.text.contains(replacement),
            "email DLQ preview must show replacement {replacement}: {}",
            preview.text
        );
    }
    assert!(
        preview
            .caveats
            .iter()
            .any(|caveat| caveat.id == "policy.disclosure_applied"),
        "email DLQ redaction must be caveated: {:?}",
        preview.caveats
    );
    assert!(
        preview.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .is_some_and(|ref_| ref_.id == "db.email-dlq-provider-secret")),
        "email DLQ redaction must name the provider-material policy: {:?}",
        preview.caveats
    );
    assert!(
        preview.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .is_some_and(|ref_| ref_.id == "db.email-dlq-subject-secret")),
        "email DLQ redaction must name the subject policy: {:?}",
        preview.caveats
    );

    Ok(())
}

#[sinex_test]
async fn payload_preview_redacts_raw_dlq_secret_bytes_by_db_policy(
    ctx: TestContext,
) -> TestResult<()> {
    ctx.pool()
        .privacy_policy()
        .add_rule(
            "dlq-preview-secret",
            "test rule",
            "regex",
            r"ghp_[A-Za-z0-9_]+",
            false,
            "redact",
            None,
            "default",
        )
        .await?;
    ctx.pool()
        .privacy_policy()
        .bind_field_rule("dlq-preview-secret", None, None, None, 0)
        .await?;
    let policy = PolicyEngine::load(ctx.pool().clone()).await?;
    let token = ["ghp_", "abcdefghijklmnopqrstuvwxyz123456"].concat();
    let payload = format!(
        r#"{{
        "original_subject": "dev.sinex.events.raw.shell.command",
        "original_payload": {{
            "command": "export GITHUB_TOKEN={token}"
        }}
    }}"#
    );

    let preview = payload_preview(&payload, 200, &policy).await;

    assert!(preview.redacted);
    assert!(
        preview
            .caveats
            .iter()
            .any(|caveat| caveat.id == "policy.disclosure_applied"),
        "redaction must be visible to machine clients: {:?}",
        preview.caveats
    );
    assert!(
        preview.caveats.iter().any(|caveat| caveat
            .ref_
            .as_ref()
            .is_some_and(|ref_| ref_.id == "db.dlq-preview-secret")),
        "machine clients must see which policy owned the redaction: {:?}",
        preview.caveats
    );
    assert!(!preview.text.contains(&token));
    assert!(!preview.text.contains("GITHUB_TOKEN=ghp_"));
    Ok(())
}

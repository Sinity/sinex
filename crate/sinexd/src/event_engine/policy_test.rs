    //! Privacy policy engine tests (#1042 Slices 3 + 4).
    //!
    //! Covers:
    //! - Rule loading from DB (`PrivacyPolicyRepository::load_enabled_rules`)
    //! - Action application: Redact (regex) / Suppress (literal) matchers
    //! - Field-path scoping: rule scoped by JSON Pointer
    //! - Source-type scoping: rule applies only to matching `event_source`
    //! - Chokepoint: derived events also go through `redact_batch`
    //! - DLQ stub: `_raw_bytes_base64` absent from stub produced by `route_to_dlq`
    //! - Cache reload: fresh `PolicyEngine::load` picks up newly added DB rule
    //!
    //! These tests are inline because the `sinexd` integration test harness
    //! uses a CI Postgres instance that serves the main-checkout xtask binary;
    //! inline tests run via the package's own test binary and avoid that issue.

    use super::*;
    use crate::event_engine::admission::AdmittedEvent;
    use sinex_db::DbPoolExt;
    use sinex_primitives::{Id, Uuid, events::DynamicPayload};
    use xtask::sandbox::prelude::*;

    // ─── Shared fixture source material UUID ─────────────────────────────────
    // Keep in sync with tests/event_engine/support.rs for cross-test consistency.
    const FIXTURE_SOURCE_MATERIAL_ID: &str = "00000000-0000-7000-8000-000000000000";

    // ─── Helpers ─────────────────────────────────────────────────────────────

    fn make_material_event(
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> sinex_primitives::events::Event<serde_json::Value> {
        let material_id: Uuid = FIXTURE_SOURCE_MATERIAL_ID.parse().expect("valid UUID");
        let material_id = Id::from_uuid(material_id);
        DynamicPayload::new(source, event_type, payload)
            .from_material(material_id)
            .build()
            .expect("test event build should not fail")
    }

    fn admit(event: sinex_primitives::events::Event<serde_json::Value>) -> AdmittedEvent {
        AdmittedEvent {
            event_id: Uuid::now_v7(),
            event,
            metadata: None,
        }
    }

    #[sinex_test]
    async fn disclosure_contexts_cover_operator_destinations() -> TestResult<()> {
        let contexts = [
            DisclosureContext::View,
            DisclosureContext::Export,
            DisclosureContext::Log,
            DisclosureContext::Completion,
            DisclosureContext::Dlq,
            DisclosureContext::Telemetry,
        ];
        let names = contexts
            .into_iter()
            .map(DisclosureContext::as_str)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec!["view", "export", "log", "completion", "dlq", "telemetry"]
        );
        Ok(())
    }

    async fn insert_global_rule(
        pool: &sinex_db::DbPool,
        name: &str,
        matcher_type: &str,
        matcher_value: &str,
        action: &str,
        action_label: Option<&str>,
    ) -> TestResult<()> {
        let repo = pool.privacy_policy();
        repo.add_rule(
            name,
            "test rule",
            matcher_type,
            matcher_value,
            false,
            action,
            action_label,
            "default",
        )
        .await?;
        repo.bind_field_rule(name, None, None, None, 0).await?;
        Ok(())
    }

    async fn insert_scoped_rule(
        pool: &sinex_db::DbPool,
        name: &str,
        matcher_value: &str,
        action_label: &str,
        event_source: &str,
        event_type: &str,
        field_path: &str,
    ) -> TestResult<()> {
        let repo = pool.privacy_policy();
        repo.add_rule(
            name,
            "test scoped disclosure rule",
            "literal",
            matcher_value,
            false,
            "redact",
            Some(action_label),
            "default",
        )
        .await?;
        repo.bind_field_rule(
            name,
            Some(event_source),
            Some(event_type),
            Some(field_path),
            0,
        )
        .await?;
        Ok(())
    }

    // ─── DB rule loading ──────────────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_rule_loading_roundtrip(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        let rules = repo.load_enabled_rules().await?;
        assert!(rules.is_empty(), "expected no rules initially");

        repo.add_rule(
            "rule-enabled",
            "",
            "regex",
            r"SECRET_\w+",
            false,
            "redact",
            None,
            "default",
        )
        .await?;
        repo.bind_field_rule("rule-enabled", None, None, None, 0)
            .await?;

        repo.add_rule(
            "rule-disabled",
            "",
            "literal",
            "x",
            false,
            "redact",
            None,
            "default",
        )
        .await?;
        repo.set_rule_enabled("rule-disabled", false).await?;

        let rules = repo.load_enabled_rules().await?;
        assert_eq!(rules.len(), 1, "only enabled rule should appear");
        assert_eq!(rules[0].rule.name, "rule-enabled");
        assert_eq!(rules[0].rule.matcher_type, "regex");
        assert_eq!(rules[0].rule.action, "redact");
        assert!(
            !rules[0].scopes.is_empty(),
            "global scope should be present"
        );

        Ok(())
    }

    // ─── Action: Redact (regex) ───────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_action_redact_regex(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        insert_global_rule(
            pool,
            "redact-secret",
            "regex",
            r"SECRET_\w+",
            "redact",
            Some("<REDACTED>"),
        )
        .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({ "token": "my SECRET_TOKEN_123 value", "other": "safe" });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        let token_str = result[0].event.payload["token"].as_str().unwrap_or("");
        assert!(
            !token_str.contains("SECRET_TOKEN_123"),
            "secret token should be redacted; got: {token_str}"
        );
        assert!(
            token_str.contains("<REDACTED>"),
            "expected <REDACTED> label; got: {token_str}"
        );
        assert_eq!(result[0].event.payload["other"].as_str(), Some("safe"));
        Ok(())
    }

    // ─── Action: Suppress (literal) ──────────────────────────────────────────

    #[sinex_test]
    async fn privacy_action_suppress_literal(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        insert_global_rule(
            pool,
            "suppress-sensitive",
            "literal",
            "SENSITIVE_VALUE",
            "suppress",
            None,
        )
        .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({ "data": "SENSITIVE_VALUE", "safe": "ok" });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        let data = &result[0].event.payload["data"];
        assert!(
            data.is_null(),
            "suppressed field should be Null; got: {data}"
        );
        assert_eq!(result[0].event.payload["safe"].as_str(), Some("ok"));
        Ok(())
    }

    // ─── Field-path scoping ───────────────────────────────────────────────────

    /// Field scopes use JSON Pointer semantics.
    #[sinex_test]
    async fn privacy_field_scoped_rule(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        repo.add_rule(
            "scope-test",
            "",
            "regex",
            r"SENSITIVE",
            false,
            "redact",
            Some("<SCOPED>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("scope-test", None, None, Some("/secret_field"), 0)
            .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({
            "secret_field": "contains SENSITIVE data",
            "public_field": "also SENSITIVE but not scoped"
        });
        let event = make_material_event("test.source", "test.event", payload);
        let result = engine.redact_batch(vec![admit(event)]).await;

        let secret = result[0].event.payload["secret_field"]
            .as_str()
            .unwrap_or("");
        let public = result[0].event.payload["public_field"]
            .as_str()
            .unwrap_or("");
        assert!(
            !secret.contains("SENSITIVE"),
            "scoped field should be redacted; got: {secret}"
        );
        assert!(
            public.contains("SENSITIVE"),
            "unscoped field must be untouched; got: {public}"
        );
        Ok(())
    }

    // ─── Source-type scoping ──────────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_source_scoped_rule(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = pool.privacy_policy();

        repo.add_rule(
            "source-scope-test",
            "",
            "regex",
            r"PII_\w+",
            false,
            "redact",
            Some("<PII>"),
            "default",
        )
        .await?;
        repo.bind_field_rule("source-scope-test", Some("sensitive.source"), None, None, 0)
            .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;

        let payload_match = serde_json::json!({ "field": "PII_DATA here" });
        let event_match = make_material_event("sensitive.source", "test.event", payload_match);
        let results = engine.redact_batch(vec![admit(event_match)]).await;
        let val = results[0].event.payload["field"].as_str().unwrap_or("");
        assert!(
            !val.contains("PII_DATA"),
            "scoped-source event should be redacted; got: {val}"
        );

        let payload_other = serde_json::json!({ "field": "PII_DATA here" });
        let event_other = make_material_event("other.source", "test.event", payload_other);
        let results_other = engine.redact_batch(vec![admit(event_other)]).await;
        let val_other = results_other[0].event.payload["field"]
            .as_str()
            .unwrap_or("");
        assert!(
            val_other.contains("PII_DATA"),
            "unscoped-source event must be untouched; got: {val_other}"
        );
        Ok(())
    }

    // ─── Media disclosure (#2039 / #1043) ────────────────────────────────────

    #[sinex_test]
    async fn media_audio_transcript_dlq_disclosure_redacts_segment_text_and_material_ref(
        ctx: TestContext,
    ) -> TestResult<()> {
        let pool = ctx.pool();
        insert_scoped_rule(
            pool,
            "media-audio-text-dlq",
            "CALL_TOKEN",
            "<MEDIA_TEXT>",
            "media.audio",
            "media.audio.transcript_segment_observed",
            "/text",
        )
        .await?;
        insert_scoped_rule(
            pool,
            "media-audio-material-dlq",
            "raw-audio-secret-001",
            "<MEDIA_MATERIAL>",
            "media.audio",
            "media.audio.transcript_segment_observed",
            "/raw_material_id",
        )
        .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;
        let event = make_material_event(
            "media.audio",
            "media.audio.transcript_segment_observed",
            serde_json::json!({
                "segment_index": 7,
                "text": "caller said CALL_TOKEN during the meeting",
                "raw_material_id": "raw-audio-secret-001",
                "source_file": "/captures/audio/meeting.wav",
            }),
        );

        let decision = engine
            .disclose_event_payload(&event, DisclosureContext::Dlq)
            .await;

        let disclosed = serde_json::to_string(&decision.value)?;
        assert!(
            decision.changed,
            "media transcript disclosure must change sensitive fields"
        );
        assert_eq!(decision.context, DisclosureContext::Dlq);
        assert!(
            !disclosed.contains("CALL_TOKEN"),
            "DLQ disclosure must not expose transcript text secret: {disclosed}"
        );
        assert!(
            !disclosed.contains("raw-audio-secret-001"),
            "DLQ disclosure must not expose scoped raw material id: {disclosed}"
        );
        assert!(
            disclosed.contains("<MEDIA_TEXT>") && disclosed.contains("<MEDIA_MATERIAL>"),
            "DLQ disclosure should retain visible redaction markers: {disclosed}"
        );
        assert!(
            decision
                .caveats
                .iter()
                .any(|caveat| caveat.policy_ref == "db.media-audio-text-dlq"),
            "operator-visible caveats should name the text policy"
        );
        assert_eq!(
            event.payload["text"].as_str(),
            Some("caller said CALL_TOKEN during the meeting"),
            "presentation-time disclosure must not mutate stored event payload"
        );
        Ok(())
    }

    #[sinex_test]
    async fn media_screen_ocr_view_disclosure_redacts_text_window_path_and_material_ref(
        ctx: TestContext,
    ) -> TestResult<()> {
        let pool = ctx.pool();
        for (name, matcher, label, field_path) in [
            (
                "media-screen-ocr-text-view",
                "SCREEN_SECRET",
                "<OCR_TEXT>",
                "/text",
            ),
            (
                "media-screen-window-view",
                "Secret Window",
                "<WINDOW_TITLE>",
                "/window_title",
            ),
            (
                "media-screen-source-path-view",
                "/home/sinity/private/screen.png",
                "<SOURCE_PATH>",
                "/source_file",
            ),
            (
                "media-screen-material-view",
                "raw-screen-secret-001",
                "<SCREEN_MATERIAL>",
                "/raw_material_id",
            ),
        ] {
            insert_scoped_rule(
                pool,
                name,
                matcher,
                label,
                "media.screen",
                "media.screen.ocr_segment_observed",
                field_path,
            )
            .await?;
        }

        let engine = PolicyEngine::load(pool.clone()).await?;
        let event = make_material_event(
            "media.screen",
            "media.screen.ocr_segment_observed",
            serde_json::json!({
                "segment_index": 3,
                "text": "visible SCREEN_SECRET from a focused app",
                "window_title": "Secret Window",
                "source_file": "/home/sinity/private/screen.png",
                "raw_material_id": "raw-screen-secret-001",
            }),
        );

        let decision = engine
            .disclose_event_payload(&event, DisclosureContext::View)
            .await;
        let disclosed = serde_json::to_string(&decision.value)?;

        assert!(
            decision.changed,
            "OCR view disclosure must change sensitive fields"
        );
        assert_eq!(decision.context, DisclosureContext::View);
        for forbidden in [
            "SCREEN_SECRET",
            "Secret Window",
            "/home/sinity/private/screen.png",
            "raw-screen-secret-001",
        ] {
            assert!(
                !disclosed.contains(forbidden),
                "OCR view disclosure leaked `{forbidden}` in {disclosed}"
            );
        }
        for marker in [
            "<OCR_TEXT>",
            "<WINDOW_TITLE>",
            "<SOURCE_PATH>",
            "<SCREEN_MATERIAL>",
        ] {
            assert!(
                disclosed.contains(marker),
                "OCR view disclosure should include marker `{marker}` in {disclosed}"
            );
        }
        assert_eq!(
            decision.caveats.len(),
            4,
            "each scoped media field policy should surface as an operator caveat"
        );
        assert_eq!(
            event.payload["window_title"].as_str(),
            Some("Secret Window"),
            "presentation-time disclosure must not mutate stored event payload"
        );
        Ok(())
    }

    // ─── Email disclosure (#2039 / #1469) ────────────────────────────────────

    #[sinex_test]
    async fn email_message_export_disclosure_redacts_subject_recipients_and_material_ref(
        ctx: TestContext,
    ) -> TestResult<()> {
        let pool = ctx.pool();
        for (name, matcher, label, field_path) in [
            (
                "email-message-subject-export",
                "M_AND_A_SECRET",
                "<EMAIL_SUBJECT>",
                "/subject",
            ),
            (
                "email-message-bcc-export",
                "hidden-board@example.com",
                "<EMAIL_BCC>",
                "/bcc/0",
            ),
            (
                "email-message-recipient-export",
                "client-private@example.com",
                "<EMAIL_RECIPIENT>",
                "/to/0",
            ),
            (
                "email-message-material-export",
                "raw-email-secret-001",
                "<EMAIL_MATERIAL>",
                "/raw_material_id",
            ),
        ] {
            insert_scoped_rule(
                pool,
                name,
                matcher,
                label,
                "email",
                "email.message.received",
                field_path,
            )
            .await?;
        }

        let engine = PolicyEngine::load(pool.clone()).await?;
        let event = make_material_event(
            "email",
            "email.message.received",
            serde_json::json!({
                "message_id": "export-1@example.com",
                "from": ["Alice <alice@example.com>"],
                "to": ["Client <client-private@example.com>"],
                "cc": [],
                "bcc": ["Board <hidden-board@example.com>"],
                "subject": "M_AND_A_SECRET launch plan",
                "folder": "inbox",
                "source_file": "Maildir/INBOX/cur/1710000005.M5P1.host:2,S",
                "raw_material_id": "raw-email-secret-001",
                "mailbox_format": "maildir-staged",
                "body_bytes": 4096,
                "attachment_count": 0,
            }),
        );

        let decision = engine
            .disclose_event_payload(&event, DisclosureContext::Export)
            .await;
        let disclosed = serde_json::to_string(&decision.value)?;

        assert!(
            decision.changed,
            "email export disclosure must change scoped sensitive fields"
        );
        assert_eq!(decision.context, DisclosureContext::Export);
        for forbidden in [
            "M_AND_A_SECRET",
            "hidden-board@example.com",
            "client-private@example.com",
            "raw-email-secret-001",
        ] {
            assert!(
                !disclosed.contains(forbidden),
                "email export disclosure leaked `{forbidden}` in {disclosed}"
            );
        }
        for marker in [
            "<EMAIL_SUBJECT>",
            "<EMAIL_BCC>",
            "<EMAIL_RECIPIENT>",
            "<EMAIL_MATERIAL>",
        ] {
            assert!(
                disclosed.contains(marker),
                "email export disclosure should include marker `{marker}` in {disclosed}"
            );
        }
        assert_eq!(
            decision.caveats.len(),
            4,
            "each scoped email field policy should surface as an operator caveat"
        );
        assert_eq!(
            event.payload["subject"].as_str(),
            Some("M_AND_A_SECRET launch plan"),
            "presentation-time disclosure must not mutate stored event payload"
        );
        Ok(())
    }

    #[sinex_test]
    async fn email_attachment_dlq_disclosure_redacts_filename_content_id_and_material_ref(
        ctx: TestContext,
    ) -> TestResult<()> {
        let pool = ctx.pool();
        for (name, matcher, label, field_path) in [
            (
                "email-attachment-filename-dlq",
                "signed-secret.pdf",
                "<EMAIL_ATTACHMENT_NAME>",
                "/filename",
            ),
            (
                "email-attachment-content-id-dlq",
                "cid-secret@example.com",
                "<EMAIL_CONTENT_ID>",
                "/content_id",
            ),
            (
                "email-attachment-material-dlq",
                "raw-attachment-secret-001",
                "<EMAIL_ATTACHMENT_MATERIAL>",
                "/raw_material_id",
            ),
        ] {
            insert_scoped_rule(
                pool,
                name,
                matcher,
                label,
                "email",
                "email.attachment.observed",
                field_path,
            )
            .await?;
        }

        let engine = PolicyEngine::load(pool.clone()).await?;
        let event = make_material_event(
            "email",
            "email.attachment.observed",
            serde_json::json!({
                "message_id": "attach-secret@example.com",
                "folder": "legal",
                "source_file": "Maildir/legal/cur/1710000006.M6P1.host:2,S",
                "raw_material_id": "raw-attachment-secret-001",
                "mailbox_format": "maildir-staged",
                "attachment_index": 2,
                "disposition": "attachment",
                "filename": "signed-secret.pdf",
                "content_type": "application/pdf",
                "content_id": "cid-secret@example.com",
                "material_policy_ref": "operator.email-mailbox.attachment-deferred",
            }),
        );

        let decision = engine
            .disclose_event_payload(&event, DisclosureContext::Dlq)
            .await;
        let disclosed = serde_json::to_string(&decision.value)?;

        assert!(
            decision.changed,
            "email attachment DLQ disclosure must change scoped sensitive fields"
        );
        assert_eq!(decision.context, DisclosureContext::Dlq);
        for forbidden in [
            "signed-secret.pdf",
            "cid-secret@example.com",
            "raw-attachment-secret-001",
        ] {
            assert!(
                !disclosed.contains(forbidden),
                "email attachment DLQ disclosure leaked `{forbidden}` in {disclosed}"
            );
        }
        for marker in [
            "<EMAIL_ATTACHMENT_NAME>",
            "<EMAIL_CONTENT_ID>",
            "<EMAIL_ATTACHMENT_MATERIAL>",
        ] {
            assert!(
                disclosed.contains(marker),
                "email attachment DLQ disclosure should include marker `{marker}` in {disclosed}"
            );
        }
        assert_eq!(
            decision.caveats.len(),
            3,
            "each scoped email attachment policy should surface as an operator caveat"
        );
        assert_eq!(
            event.payload["filename"].as_str(),
            Some("signed-secret.pdf"),
            "presentation-time disclosure must not mutate stored event payload"
        );
        Ok(())
    }

    #[sinex_test]
    async fn email_generated_json_disclosure_keeps_scoped_policy_across_destinations(
        ctx: TestContext,
    ) -> TestResult<()> {
        let pool = ctx.pool();
        for (name, matcher, label, field_path) in [
            (
                "email-generated-subject",
                "GENERATED_SUBJECT_SECRET",
                "<EMAIL_SUBJECT>",
                "/messages/0/subject",
            ),
            (
                "email-generated-recipient",
                "generated-recipient@example.test",
                "<EMAIL_RECIPIENT>",
                "/messages/0/to/0",
            ),
            (
                "email-generated-material",
                "generated-material-secret",
                "<EMAIL_MATERIAL>",
                "/material_exports/0/raw_message_preview",
            ),
        ] {
            insert_scoped_rule(
                pool,
                name,
                matcher,
                label,
                "email",
                "email.message.received",
                field_path,
            )
            .await?;
        }

        let engine = PolicyEngine::load(pool.clone()).await?;
        for context in [
            DisclosureContext::Export,
            DisclosureContext::Log,
            DisclosureContext::Completion,
            DisclosureContext::Telemetry,
        ] {
            let generated = serde_json::json!({
                "schema": "sinex.email.mailbox.export.metadata.v1",
                "messages": [{
                    "subject": "GENERATED_SUBJECT_SECRET roadmap",
                    "to": ["generated-recipient@example.test"],
                    "body_bytes": 1024,
                }],
                "material_exports": [{
                    "raw_message_preview": "prefix generated-material-secret suffix",
                    "raw_message_bytes": 2048,
                }],
            });

            let decision = engine
                .disclose_json_value_for_event(
                    generated,
                    context,
                    "email",
                    "email.message.received",
                )
                .await;
            let disclosed = serde_json::to_string(&decision.value)?;
            assert!(
                decision.changed,
                "generated email JSON should be redacted for {context:?}"
            );
            for forbidden in [
                "GENERATED_SUBJECT_SECRET",
                "generated-recipient@example.test",
                "generated-material-secret",
            ] {
                assert!(
                    !disclosed.contains(forbidden),
                    "generated email JSON leaked `{forbidden}` for {context:?}: {disclosed}"
                );
            }
            for marker in ["<EMAIL_SUBJECT>", "<EMAIL_RECIPIENT>", "<EMAIL_MATERIAL>"] {
                assert!(
                    disclosed.contains(marker),
                    "generated email JSON should contain marker `{marker}` for {context:?}: {disclosed}"
                );
            }
            assert_eq!(
                decision.caveats.len(),
                3,
                "each scoped generated email policy should surface for {context:?}"
            );
        }

        let untyped = engine
            .disclose_json_value(
                serde_json::json!({
                    "messages": [{
                        "subject": "GENERATED_SUBJECT_SECRET roadmap",
                        "to": ["generated-recipient@example.test"],
                    }],
                }),
                DisclosureContext::Export,
            )
            .await;
        assert!(
            !untyped.changed,
            "untyped generated JSON must not promote email-scoped rules globally"
        );
        Ok(())
    }

    // ─── Chokepoint: derived events ───────────────────────────────────────────

    #[sinex_test]
    async fn privacy_chokepoint_applies_to_derived_events(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        insert_global_rule(
            pool,
            "derived-redact",
            "regex",
            r"DERIVED_SECRET_\w+",
            "redact",
            Some("<DERIVED>"),
        )
        .await?;

        let engine = PolicyEngine::load(pool.clone()).await?;

        let parent_id: Uuid = Uuid::now_v7();
        let parent_event_id: sinex_primitives::events::EventId = Id::from_uuid(parent_id);
        let payload = serde_json::json!({ "summary": "derived contains DERIVED_SECRET_XYZ here" });
        let derived_event = DynamicPayload::new("sinex.derived", "analytics.insight", payload)
            .from_parents([parent_event_id])
            .expect("valid parent")
            .build()
            .expect("test derived event build should not fail");

        let result = engine.redact_batch(vec![admit(derived_event)]).await;
        let summary = result[0].event.payload["summary"].as_str().unwrap_or("");
        assert!(
            !summary.contains("DERIVED_SECRET_XYZ"),
            "derived event secret should be redacted; got: {summary}"
        );
        assert!(
            summary.contains("<DERIVED>"),
            "expected <DERIVED> label; got: {summary}"
        );
        Ok(())
    }

    // ─── DLQ raw-bytes suppression ────────────────────────────────────────────

    /// Verifies that the stub produced by route_to_dlq (when JSON parse fails)
    /// NEVER contains `_raw_bytes_base64` — only a metadata-only stub.
    #[sinex_test]
    async fn privacy_dlq_raw_bytes_stub_shape(_ctx: TestContext) -> TestResult<()> {
        let parse_err_str = "expected value at line 1 column 1";
        let raw_len: usize = 42;
        let stub = serde_json::json!({
            "_parse_error": parse_err_str,
            "_raw_bytes_suppressed": true,
            "_raw_bytes_len": raw_len,
            "_dlq_note": "raw payload suppressed by privacy chokepoint (#1042)"
        });

        assert!(
            stub.get("_raw_bytes_base64").is_none(),
            "_raw_bytes_base64 must be absent from DLQ stub; got: {stub}"
        );
        assert_eq!(
            stub.get("_raw_bytes_suppressed")
                .and_then(sinex_primitives::JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            stub.get("_raw_bytes_len")
                .and_then(sinex_primitives::JsonValue::as_u64),
            Some(42)
        );
        Ok(())
    }

    // ─── Cache reload ─────────────────────────────────────────────────────────

    #[sinex_test]
    async fn privacy_cache_reload_picks_up_new_rule(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();

        let engine_before = PolicyEngine::load(pool.clone()).await?;
        let payload = serde_json::json!({ "value": "CACHE_SENTINEL_XYZ" });
        let event = make_material_event("s", "t", payload);
        let result_before = engine_before.redact_batch(vec![admit(event)]).await;
        assert_eq!(
            result_before[0].event.payload["value"].as_str(),
            Some("CACHE_SENTINEL_XYZ"),
            "no rule should be applied before DB insert"
        );

        insert_global_rule(
            pool,
            "cache-test",
            "literal",
            "CACHE_SENTINEL_XYZ",
            "redact",
            Some("<CACHED>"),
        )
        .await?;

        let engine_after = PolicyEngine::load(pool.clone()).await?;
        let payload2 = serde_json::json!({ "value": "CACHE_SENTINEL_XYZ" });
        let event2 = make_material_event("s", "t", payload2);
        let result_after = engine_after.redact_batch(vec![admit(event2)]).await;
        let value = result_after[0].event.payload["value"]
            .as_str()
            .unwrap_or("");
        assert!(
            !value.contains("CACHE_SENTINEL_XYZ"),
            "rule should apply after reload; got: {value}"
        );
        assert!(
            value.contains("<CACHED>"),
            "expected <CACHED> label; got: {value}"
        );
        Ok(())
    }

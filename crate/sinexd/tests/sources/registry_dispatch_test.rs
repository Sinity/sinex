//! Integration tests for the registry-driven parser dispatch and source factory.
//!
//! Verifies:
//! 1. `default_parser_dispatch()` is registry-driven (no match arms) and routes
//!    `WeeChat` log lines to the correct parser.
//! 2. The declarative `WeeChatMessageRecord` parser is registered and reachable.
//! 3. Unknown source contracts produce a clear error.
//! 4. The source factory registry has "noop" registered.
//! 5. The fs source exposes one adapter-backed factory plus parser dispatch.
//! 6. Grep probe: no source match arms in dispatch.rs or main.rs.

use sinex_primitives::parser::SourceId;
use sinexd::sources::{
    dispatch::{default_parser_dispatch, find_parser_factory},
    source_factory::{find_source_factory, registered_source_factory_ids},
};
use xtask::sandbox::prelude::*;

fn sui(s: &'static str) -> SourceId {
    SourceId::from_static(s)
}

// ---------------------------------------------------------------------------
// 1. WeeChat imperative parser — registry-driven dispatch round-trip
// ---------------------------------------------------------------------------

/// Verify that "weechat" is in the parser registry and that a well-formed log
/// line dispatches without error.
#[sinex_test]
async fn weechat_parser_registered_and_dispatches() -> TestResult<()> {
    // Factory must be present.
    assert!(
        find_parser_factory(&sui("weechat")).is_some(),
        "parser factory for 'weechat' must be registered"
    );

    // A valid WeeChat log line should parse without error.
    let dispatch = default_parser_dispatch();
    let log_line = b"2024-01-15 14:23:45\tsinity\thello world";
    let result = dispatch("weechat", log_line, None);

    // The imperative parser returns exactly 1 irc.message intent.
    let outcome = result.expect("dispatch must succeed for a valid weechat log line");
    assert_eq!(
        outcome.events.len(),
        1,
        "expected 1 event intent, got {}",
        outcome.events.len()
    );
    assert_eq!(outcome.parser_id, "weechat-log");
    assert_eq!(outcome.events[0].event_type.as_str(), "irc.message");
    assert_eq!(outcome.events[0].payload["nick"], "sinity");
    assert_eq!(outcome.events[0].payload["message"], "hello world");
    Ok(())
}

/// Join events should produce irc.join.
#[sinex_test]
async fn weechat_join_line_produces_irc_join() -> TestResult<()> {
    let dispatch = default_parser_dispatch();
    let log_line = b"2024-06-01 10:00:00\t-->\tuser (~user@host) joined #general";
    let outcome =
        dispatch("weechat", log_line, None).expect("dispatch must succeed for a join line");
    assert_eq!(outcome.events.len(), 1);
    assert_eq!(outcome.events[0].event_type.as_str(), "irc.join");
    assert_eq!(outcome.events[0].payload["channel"], "#general");
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Declarative WeeChatMessageRecord — registered and functional
// ---------------------------------------------------------------------------

/// Verify "weechat.message" is in the registry and produces irc.message events.
#[sinex_test]
async fn weechat_message_declarative_registered() -> TestResult<()> {
    assert!(
        find_parser_factory(&sui("weechat.message")).is_some(),
        "declarative 'weechat.message' parser must be registered"
    );

    let dispatch = default_parser_dispatch();
    let log_line = b"2024-01-15 14:23:45\tsinity\thello world";
    let outcome = dispatch("weechat.message", log_line, None)
        .expect("declarative dispatch must succeed for a valid tab-separated line");

    assert_eq!(outcome.parser_id, "weechat-message-declarative");
    // The declarative parser emits irc.message with raw_timestamp, prefix, message fields.
    assert_eq!(outcome.events.len(), 1);
    let payload = &outcome.events[0].payload;
    assert_eq!(payload["raw_timestamp"], "2024-01-15 14:23:45");
    assert_eq!(payload["prefix"], "sinity");
    assert_eq!(payload["message"], "hello world");
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Unknown source — registry-driven error (no match arm fallback)
// ---------------------------------------------------------------------------

#[sinex_test]
async fn unknown_source_produces_registry_error() -> TestResult<()> {
    let dispatch = default_parser_dispatch();
    let result = dispatch("no-such-source-xyz", b"data", None);
    assert!(result.is_err(), "unknown source must produce an error");
    let err = result.unwrap_err();
    assert!(
        err.contains("unknown source_id 'no-such-source-xyz'"),
        "error must name the unknown source_id, got: {err}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Source factory registry — "noop" is registered
// ---------------------------------------------------------------------------

#[sinex_test]
async fn noop_source_factory_registered() -> TestResult<()> {
    assert!(
        find_source_factory(&sui("noop")).is_some(),
        "source factory for 'noop' must be registered"
    );

    let ids = registered_source_factory_ids();
    assert!(
        ids.iter().any(|id| id.as_str() == "noop"),
        "registered_source_factory_ids() must include 'noop', got: {ids:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 5. Filesystem bridge — adapter-backed runtime plus parser dispatch
// ---------------------------------------------------------------------------

#[sinex_test]
async fn fs_adapter_factory_and_parser_bridge_registered() -> TestResult<()> {
    assert!(
        find_source_factory(&sui("fs")).is_some(),
        "adapter-backed source factory for 'fs' must be registered"
    );
    assert!(
        find_parser_factory(&sui("fs")).is_some(),
        "parser factory for 'fs' must be registered for replay dispatch"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 6. Source registry — descriptor lookup works for weechat
// ---------------------------------------------------------------------------

#[sinex_test]
async fn weechat_descriptor_registered() -> TestResult<()> {
    use sinexd::sources::SourceContractRegistry;
    let registry = SourceContractRegistry::from_inventory();
    assert!(
        registry.find(&sui("weechat")).is_some(),
        "SourceContract for 'weechat' must be registered"
    );
    assert!(
        registry.find(&sui("weechat.message")).is_some(),
        "SourceContract for 'weechat.message' must be registered"
    );
    Ok(())
}

#[sinex_test]
async fn source_meta_weechat_registers_production_and_companion_parsers() -> TestResult<()> {
    use sinex_primitives::source_contracts::{all_source_contracts, source_runtime_bindings};

    let weechat = sui("weechat");
    let weechat_contract = all_source_contracts()
        .find(|contract| contract.id == "weechat")
        .expect("weechat SourceContract must be registered");
    for event_type in ["irc.message", "irc.join", "irc.part", "irc.server_notice"] {
        assert!(
            weechat_contract
                .event_types
                .iter()
                .any(|(source, declared)| *source == "irc" && *declared == event_type),
            "weechat contract must declare irc/{event_type}"
        );
    }
    assert!(
        source_runtime_bindings().any(|binding| {
            binding.source_id == "weechat"
                && binding.adapter == "AppendOnlyFileAdapter"
                && binding.output_event_type == "irc.message"
                && !binding.proposed
        }),
        "weechat must register its live AppendOnlyFileAdapter runtime binding"
    );
    assert!(
        find_source_factory(&weechat).is_some(),
        "weechat production adapter-backed source factory must be registered"
    );
    assert!(
        find_parser_factory(&weechat).is_some(),
        "weechat production parser dispatch must be registered"
    );

    let companion = sui("weechat.message");
    assert!(
        all_source_contracts().any(|contract| {
            contract.id == "weechat.message"
                && contract
                    .event_types
                    .iter()
                    .any(|(source, declared)| *source == "irc" && *declared == "irc.message")
        }),
        "weechat.message companion SourceContract must be registered"
    );
    assert!(
        source_runtime_bindings().any(|binding| {
            binding.source_id == "weechat.message"
                && binding.adapter == "AppendOnlyFileAdapter"
                && binding.output_event_type == "irc.message"
                && !binding.proposed
        }),
        "weechat.message must register its live parser runtime binding"
    );
    assert!(
        find_parser_factory(&companion).is_some(),
        "weechat.message parser-only dispatch must be registered"
    );
    assert!(
        find_source_factory(&companion).is_none(),
        "weechat.message must preserve parser-only behavior"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 7. SourceMeta-migrated terminal sources still register all three sites
// ---------------------------------------------------------------------------

/// The four deferred imperative terminal sources moved their `SourceContract`,
/// `SourceRuntimeBinding`, and `register_source!` factory wiring into
/// `#[derive(SourceMeta)]` (#1727 slice 3) while keeping their hand-written
/// `MaterialParser`. This asserts the derive actually emits all three
/// registration sites for each, so the migration did not silently drop a
/// source from the inventory.
#[sinex_test]
async fn source_meta_terminal_sources_fully_registered() -> TestResult<()> {
    use sinex_primitives::source_contracts::source_runtime_bindings;
    use sinexd::sources::SourceContractRegistry;

    let registry = SourceContractRegistry::from_inventory();

    for id in [
        "terminal.bash-history",
        "terminal.zsh-history",
        "terminal.text-history",
        "terminal.asciinema",
    ] {
        assert!(
            registry.find(&sui(id)).is_some(),
            "SourceContract for '{id}' must be registered by #[derive(SourceMeta)]"
        );
        assert!(
            find_source_factory(&sui(id)).is_some(),
            "source factory for '{id}' must be registered by #[derive(SourceMeta)]"
        );
        assert!(
            find_parser_factory(&sui(id)).is_some(),
            "parser factory for '{id}' must be registered by #[derive(SourceMeta)]"
        );
        assert!(
            source_runtime_bindings().any(|b| b.id == id),
            "SourceRuntimeBinding for '{id}' must be registered by #[derive(SourceMeta)]"
        );
    }
    Ok(())
}

#[sinex_test]
async fn source_meta_external_producer_registers_metadata_without_factory() -> TestResult<()> {
    use sinex_primitives::source_contracts::{all_source_contracts, source_runtime_bindings};

    let source_id = sui("integration.polylogue");
    assert!(
        all_source_contracts().any(|contract| contract.id == "integration.polylogue"),
        "external producer must still register its SourceContract"
    );
    assert!(
        source_runtime_bindings().any(|binding| {
            binding.source_id == "integration.polylogue" && binding.adapter == "ExternalProducer"
        }),
        "external producer must still register its SourceRuntimeBinding"
    );
    assert!(
        find_source_factory(&source_id).is_none(),
        "external producer must not register a source factory"
    );
    assert!(
        find_parser_factory(&source_id).is_none(),
        "external producer must not register a parser factory"
    );
    Ok(())
}

#[sinex_test]
async fn source_meta_media_staged_sources_register_parser_and_factory() -> TestResult<()> {
    use sinex_primitives::source_contracts::{all_source_contracts, source_runtime_bindings};

    for (source_id, event_type) in [
        (
            "media.audio-transcript",
            "media.audio.transcript_segment_observed",
        ),
        ("media.screen-ocr", "media.screen.ocr_segment_observed"),
    ] {
        let source = sui(source_id);
        assert!(
            all_source_contracts().any(|contract| {
                contract.id == source_id
                    && contract
                        .event_types
                        .iter()
                        .any(|(_, declared)| *declared == event_type)
            }),
            "staged media source must register its SourceContract"
        );
        assert!(
            source_runtime_bindings().any(|binding| {
                binding.source_id == source_id
                    && binding.output_event_type == event_type
                    && !binding.proposed
            }),
            "staged media source must register an accepted SourceRuntimeBinding"
        );
        assert!(
            find_source_factory(&source).is_some(),
            "staged media source must register a source factory"
        );
        assert!(
            find_parser_factory(&source).is_some(),
            "staged media source must register a parser factory"
        );
    }
    Ok(())
}

#[sinex_test]
async fn source_meta_email_source_registers_staged_mailbox_modes_and_sent_proposal()
-> TestResult<()> {
    use sinex_primitives::source_contracts::{all_source_contracts, source_runtime_bindings};

    let source_id = sui("email.mailbox");
    let contract = all_source_contracts()
        .find(|contract| contract.id == "email.mailbox")
        .expect("email mailbox SourceContract must be registered");
    assert!(
        contract
            .event_types
            .iter()
            .any(|(_, event_type)| *event_type == "email.message.received"),
        "email contract must declare received messages"
    );
    assert!(
        contract
            .event_types
            .iter()
            .any(|(_, event_type)| *event_type == "email.message.sent"),
        "email contract must declare sent messages"
    );
    assert!(
        contract
            .event_types
            .iter()
            .any(|(_, event_type)| *event_type == "email.attachment.observed"),
        "email contract must declare attachment observations"
    );
    assert!(
        contract
            .event_types
            .iter()
            .any(|(_, event_type)| *event_type == "email.thread.observed"),
        "email contract must declare thread observations"
    );
    assert!(
        contract
            .event_types
            .iter()
            .any(|(_, event_type)| *event_type == "email.sync_cursor.observed"),
        "email contract must declare provider sync cursor observations"
    );
    assert!(
        contract
            .event_types
            .iter()
            .any(|(_, event_type)| *event_type == "email.capture_runtime.observed"),
        "email contract must declare provider runtime observations"
    );

    let bindings: Vec<_> = source_runtime_bindings()
        .filter(|binding| binding.source_id == "email.mailbox")
        .collect();
    assert_eq!(
        bindings.len(),
        7,
        "email mailbox must register staged, sent, Gmail, and IMAP runtime bindings"
    );
    for (subject, event_type) in [
        ("source:email.mailbox", "email.message.received"),
        (
            "source:email.mailbox.maildir-staged",
            "email.message.received",
        ),
        ("source:email.mailbox.mbox-staged", "email.message.received"),
        ("source:email.mailbox.sent", "email.message.sent"),
        (
            "source:email.mailbox.gmail-api-scheduled-sync",
            "email.sync_cursor.observed",
        ),
        (
            "source:email.mailbox.imap-scheduled-sync",
            "email.sync_cursor.observed",
        ),
        (
            "source:email.mailbox.imap-idle-live",
            "email.capture_runtime.observed",
        ),
    ] {
        assert!(
            bindings.iter().any(|binding| {
                binding.subject.as_str() == subject && binding.output_event_type == event_type
            }),
            "email mailbox must register binding {subject} -> {event_type}"
        );
    }
    for subject in [
        "source:email.mailbox",
        "source:email.mailbox.maildir-staged",
        "source:email.mailbox.mbox-staged",
    ] {
        assert!(
            bindings
                .iter()
                .any(|binding| binding.subject.as_str() == subject && !binding.proposed),
            "email staged mode {subject} must be an accepted runnable binding"
        );
    }
    assert!(
        bindings.iter().any(|binding| {
            binding.subject.as_str() == "source:email.mailbox.sent" && binding.proposed
        }),
        "email sent mode remains proposed until its runtime mode is accepted"
    );
    for subject in [
        "source:email.mailbox.gmail-api-scheduled-sync",
        "source:email.mailbox.imap-scheduled-sync",
        "source:email.mailbox.imap-idle-live",
    ] {
        assert!(
            bindings
                .iter()
                .any(|binding| binding.subject.as_str() == subject && binding.proposed),
            "email provider mode {subject} remains proposed until the runtime client is executable"
        );
    }
    assert!(
        find_source_factory(&source_id).is_some(),
        "accepted email staged mode must register a source factory"
    );
    assert!(
        find_parser_factory(&source_id).is_some(),
        "accepted email staged mode must register a parser factory"
    );
    Ok(())
}

#[sinex_test]
async fn source_meta_browser_history_registers_chained_adapter_factory() -> TestResult<()> {
    use sinex_primitives::source_contracts::{all_source_contracts, source_runtime_bindings};

    let source_id = sui("browser.history");
    let contract = all_source_contracts()
        .find(|contract| contract.id == "browser.history")
        .expect("browser history SourceContract must be registered");
    assert!(
        contract
            .event_types
            .iter()
            .any(|(source, event_type)| *source == "webhistory" && *event_type == "page.visited"),
        "browser history contract must declare webhistory/page.visited"
    );
    assert!(
        source_runtime_bindings().any(|binding| {
            binding.source_id == "browser.history"
                && binding.adapter == "ChainedAdapter<SqliteRowAdapter, AppendOnlyFileAdapter>"
                && binding.output_event_type == "page.visited"
                && !binding.proposed
        }),
        "browser history must register live chained-adapter runtime metadata"
    );
    assert!(
        find_source_factory(&source_id).is_some(),
        "browser history must register a source factory"
    );
    assert!(
        find_parser_factory(&source_id).is_some(),
        "browser history must register a parser factory"
    );
    Ok(())
}

#[sinex_test]
async fn source_meta_document_staging_registers_parser_and_driver() -> TestResult<()> {
    use sinex_primitives::source_contracts::{all_source_contracts, source_runtime_bindings};

    let source_id = sui("document.staging");
    let contract = all_source_contracts()
        .find(|contract| contract.id == "document.staging")
        .expect("document staging SourceContract must be registered");
    assert!(
        contract.event_types.iter().any(|(source, event_type)| {
            *source == "document-source" && *event_type == "document.ingested"
        }),
        "document staging contract must declare document-source/document.ingested"
    );
    assert!(
        source_runtime_bindings().any(|binding| {
            binding.source_id == "document.staging"
                && binding.adapter == "DocumentStagingParser"
                && binding.output_event_type == "document.ingested"
                && !binding.proposed
        }),
        "document staging must register live parser runtime metadata"
    );
    assert!(
        find_parser_factory(&source_id).is_some(),
        "document staging must register parser dispatch"
    );
    assert!(
        find_source_factory(&source_id).is_some(),
        "document staging must register the source-driver factory"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 8. #1792 SourcePackage completeness report derives from live inventories
// ---------------------------------------------------------------------------

#[sinex_test]
async fn package_completeness_report_is_keyed_by_package_and_mode() -> TestResult<()> {
    use sinex_primitives::STANDARD_EVENT_ADMISSION_POLICY_ID;
    use sinex_primitives::event_contracts::SHELL_ATUIN_COMMAND_EXECUTED_CONTRACT_ID;
    use sinexd::sources::package_completeness::{
        PackageCompleteness, PackageModeState, build_package_completeness_report,
    };

    let report = build_package_completeness_report();
    assert_eq!(report.schema_version, 2);
    assert!(
        report.summary.package_count >= 1,
        "report must enumerate compiled SourceContract inventory"
    );

    let terminal = report
        .packages
        .get("terminal.atuin-history")
        .expect("terminal.atuin-history package row");
    let mode = terminal
        .modes
        .get("terminal.atuin-history")
        .expect("terminal.atuin-history mode row keyed by mode id");
    assert_eq!(mode.sources.source_contract.id, "terminal.atuin-history");
    assert!(mode.sources.runtime_binding.is_some());
    assert!(mode.sources.parser_manifest.is_some());
    assert!(mode.sources.source_factory_registered);
    assert!(mode.sources.parser_factory_registered);
    assert!(mode.sources.catalog_projection_registered);
    assert!(mode.sources.privacy_coverage_registered);
    assert_eq!(mode.completeness, PackageCompleteness::Incomplete);
    assert_eq!(mode.mode_state, PackageModeState::Incomplete);
    assert!(
        mode.event_contract_refs
            .contains(&SHELL_ATUIN_COMMAND_EXECUTED_CONTRACT_ID.to_string()),
        "Atuin history mode must consume the current EventContract registry"
    );
    assert!(
        mode.admission_policy_refs
            .contains(&STANDARD_EVENT_ADMISSION_POLICY_ID.to_string()),
        "Atuin history mode must consume the current AdmissionPolicy registry"
    );
    assert!(
        mode.event_pairs.iter().any(|pair| {
            pair.source == "shell.atuin"
                && pair.event_type == "command.executed"
                && pair.event_contract_ref.as_deref()
                    == Some(SHELL_ATUIN_COMMAND_EXECUTED_CONTRACT_ID)
        }),
        "Atuin event pair rows should carry the matching EventContract ref"
    );
    assert!(
        !mode
            .missing
            .iter()
            .any(|field| field == "event_contract_refs" || field == "admission_policy_ref"),
        "incomplete package rows must point at current executable gaps, not closed design refs: {:?}",
        mode.missing
    );
    assert!(
        mode.missing.iter().any(|field| field == "operation_refs")
            || mode
                .missing
                .iter()
                .any(|field| field == "coverage_and_debt_views")
            || mode
                .missing
                .iter()
                .any(|field| field == "fixtures_and_tests"),
        "incomplete package rows should point at current executable gaps, not closed design refs: {:?}",
        mode.missing
    );
    assert!(
        !mode
            .missing
            .iter()
            .any(|field| field == "resource_budget_spec"),
        "runtime bindings derive ResourceBudgetSpec from the current ResourceProfile model"
    );
    Ok(())
}

#[sinex_test]
async fn package_completeness_report_consumes_event_admission_and_budget_refs() -> TestResult<()> {
    use sinex_primitives::STANDARD_EVENT_ADMISSION_POLICY_ID;
    use sinex_primitives::event_contracts::{
        BROWSER_PAGE_VISITED_CONTRACT_ID, EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID,
        EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID, EMAIL_MESSAGE_RECEIVED_CONTRACT_ID,
        EMAIL_MESSAGE_SENT_CONTRACT_ID, EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID,
        EMAIL_THREAD_OBSERVED_CONTRACT_ID, MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID,
        MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID,
        MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID, MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
        MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID,
        MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID, MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
        MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID, SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID,
    };
    use sinexd::sources::package_completeness::build_package_completeness_report;

    let report = build_package_completeness_report();
    let mode = report
        .packages
        .get("terminal.bash-history")
        .and_then(|package| package.modes.get("terminal.bash-history"))
        .expect("terminal.bash-history package/mode row");

    assert!(
        mode.event_contract_refs
            .contains(&SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID.to_string()),
        "shell-history mode must consume the current EventContract registry"
    );
    assert!(
        mode.admission_policy_refs
            .contains(&STANDARD_EVENT_ADMISSION_POLICY_ID.to_string()),
        "shell-history mode must consume the current AdmissionPolicy registry"
    );
    assert!(
        mode.event_pairs.iter().any(|pair| {
            pair.source == "shell.history"
                && pair.event_type == "command.imported"
                && pair.event_contract_ref.as_deref()
                    == Some(SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID)
        }),
        "event pair rows should carry the matching EventContract ref"
    );
    assert!(
        mode.sources
            .runtime_binding
            .as_ref()
            .is_some_and(|binding| !binding.resource_budget.is_null()),
        "runtime binding rows should expose the derived ResourceBudgetSpec"
    );

    let browser = report
        .packages
        .get("browser.history")
        .and_then(|package| package.modes.get("browser.history"))
        .expect("browser.history package/mode row");
    assert!(
        browser
            .event_contract_refs
            .contains(&BROWSER_PAGE_VISITED_CONTRACT_ID.to_string()),
        "browser history mode must consume the page-visit EventContract registry row"
    );
    assert!(
        browser
            .admission_policy_refs
            .contains(&STANDARD_EVENT_ADMISSION_POLICY_ID.to_string()),
        "browser history mode must be accepted by the standard admission policy"
    );
    assert!(
        browser.event_pairs.iter().any(|pair| {
            pair.source == "webhistory"
                && pair.event_type == "page.visited"
                && pair.event_contract_ref.as_deref() == Some(BROWSER_PAGE_VISITED_CONTRACT_ID)
        }),
        "browser event pair rows should carry the matching EventContract ref"
    );
    assert!(
        browser
            .sources
            .runtime_binding
            .as_ref()
            .is_some_and(|binding| !binding.resource_budget.is_null()),
        "browser runtime binding rows should expose the derived ResourceBudgetSpec"
    );

    let email = report
        .packages
        .get("email.mailbox")
        .and_then(|package| package.modes.get("email.mailbox"))
        .expect("email.mailbox package/mode row");
    assert!(
        email
            .event_contract_refs
            .contains(&EMAIL_MESSAGE_RECEIVED_CONTRACT_ID.to_string()),
        "email staged mode must consume the received-message EventContract registry row"
    );
    assert!(
        email
            .event_contract_refs
            .contains(&EMAIL_MESSAGE_SENT_CONTRACT_ID.to_string()),
        "email package row must expose the sent-message EventContract registry row"
    );
    assert!(
        email
            .event_contract_refs
            .contains(&EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID.to_string()),
        "email package row must expose the attachment-observed EventContract registry row"
    );
    assert!(
        email
            .event_contract_refs
            .contains(&EMAIL_THREAD_OBSERVED_CONTRACT_ID.to_string()),
        "email package row must expose the thread-observed EventContract registry row"
    );
    assert!(
        email
            .event_contract_refs
            .contains(&EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID.to_string()),
        "email package row must expose the sync-cursor EventContract registry row"
    );
    assert!(
        email
            .event_contract_refs
            .contains(&EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID.to_string()),
        "email package row must expose the capture-runtime EventContract registry row"
    );
    assert!(
        email
            .admission_policy_refs
            .contains(&STANDARD_EVENT_ADMISSION_POLICY_ID.to_string()),
        "email staged mode must be accepted by the standard admission policy"
    );
    assert!(
        email.event_pairs.iter().any(|pair| {
            pair.source == "email"
                && pair.event_type == "email.message.received"
                && pair.event_contract_ref.as_deref() == Some(EMAIL_MESSAGE_RECEIVED_CONTRACT_ID)
        }),
        "email received event pair rows should carry the matching EventContract ref"
    );
    assert!(
        email.event_pairs.iter().any(|pair| {
            pair.source == "email"
                && pair.event_type == "email.attachment.observed"
                && pair.event_contract_ref.as_deref() == Some(EMAIL_ATTACHMENT_OBSERVED_CONTRACT_ID)
        }),
        "email attachment event pair rows should carry the matching EventContract ref"
    );
    assert!(
        email.event_pairs.iter().any(|pair| {
            pair.source == "email"
                && pair.event_type == "email.thread.observed"
                && pair.event_contract_ref.as_deref() == Some(EMAIL_THREAD_OBSERVED_CONTRACT_ID)
        }),
        "email thread event pair rows should carry the matching EventContract ref"
    );
    assert!(
        email.event_pairs.iter().any(|pair| {
            pair.source == "email"
                && pair.event_type == "email.sync_cursor.observed"
                && pair.event_contract_ref.as_deref()
                    == Some(EMAIL_SYNC_CURSOR_OBSERVED_CONTRACT_ID)
        }),
        "email sync-cursor event pair rows should carry the matching EventContract ref"
    );
    assert!(
        email.event_pairs.iter().any(|pair| {
            pair.source == "email"
                && pair.event_type == "email.capture_runtime.observed"
                && pair.event_contract_ref.as_deref()
                    == Some(EMAIL_CAPTURE_RUNTIME_OBSERVED_CONTRACT_ID)
        }),
        "email capture-runtime event pair rows should carry the matching EventContract ref"
    );
    assert!(
        email
            .sources
            .runtime_binding
            .as_ref()
            .is_some_and(|binding| !binding.resource_budget.is_null()),
        "email runtime binding rows should expose the derived ResourceBudgetSpec"
    );

    for (package_id, event_source, event_type, contract_id, package_contract_ids) in [
        (
            "media.audio-transcript",
            "media.audio",
            "media.audio.transcript_segment_observed",
            MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
            &[
                MEDIA_AUDIO_RECORDING_OBSERVED_CONTRACT_ID,
                MEDIA_AUDIO_CAPTURE_SESSION_STARTED_CONTRACT_ID,
                MEDIA_AUDIO_CAPTURE_SESSION_ENDED_CONTRACT_ID,
                MEDIA_AUDIO_TRANSCRIPT_SEGMENT_CONTRACT_ID,
                MEDIA_AUDIO_TRANSCRIPTION_RUN_OBSERVED_CONTRACT_ID,
            ][..],
        ),
        (
            "media.screen-ocr",
            "media.screen",
            "media.screen.ocr_segment_observed",
            MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
            &[
                MEDIA_SCREEN_SCREENSHOT_OBSERVED_CONTRACT_ID,
                MEDIA_SCREEN_OCR_SEGMENT_CONTRACT_ID,
                MEDIA_SCREEN_OCR_RUN_OBSERVED_CONTRACT_ID,
            ][..],
        ),
    ] {
        let media = report
            .packages
            .get(package_id)
            .and_then(|package| package.modes.get(package_id))
            .expect("media staged package/mode row");
        for expected_contract_id in package_contract_ids {
            assert!(
                media
                    .event_contract_refs
                    .contains(&expected_contract_id.to_string()),
                "media package mode {package_id} must consume EventContract {expected_contract_id}"
            );
        }
        assert!(
            media.event_contract_refs.contains(&contract_id.to_string()),
            "media package mode must consume its parser EventContract registry row"
        );
        assert!(
            media
                .admission_policy_refs
                .contains(&STANDARD_EVENT_ADMISSION_POLICY_ID.to_string()),
            "media package mode must be accepted by the standard admission policy"
        );
        assert!(
            media.event_pairs.iter().any(|pair| {
                pair.source == event_source
                    && pair.event_type == event_type
                    && pair.event_contract_ref.as_deref() == Some(contract_id)
            }),
            "media package event pair rows should carry the matching EventContract ref"
        );
        assert!(
            media
                .sources
                .runtime_binding
                .as_ref()
                .is_some_and(|binding| !binding.resource_budget.is_null()),
            "media runtime binding rows should expose the derived ResourceBudgetSpec"
        );
    }

    Ok(())
}

#[sinex_test]
async fn package_completeness_report_consumes_coverage_debt_and_operation_refs() -> TestResult<()> {
    use sinexd::sources::package_completeness::build_package_completeness_report;

    let report = build_package_completeness_report();
    let mode = report
        .packages
        .get("terminal.kitty-osc-live")
        .and_then(|package| package.modes.get("terminal.kitty-osc-live"))
        .expect("terminal.kitty-osc-live package/mode row");

    assert!(
        mode.coverage_debt_refs
            .contains(&"coverage:source-coverage".to_string()),
        "Kitty mode must declare the coverage provider consumed by the package gate"
    );
    assert!(
        mode.coverage_debt_refs
            .contains(&"debt:unified-debt-view".to_string()),
        "Kitty mode must declare the unified debt provider consumed by the package gate"
    );
    assert!(
        mode.operation_refs
            .contains(&"operation:terminal.activity.check".to_string()),
        "Kitty mode must declare operator action refs consumed by the package gate"
    );
    assert!(
        !mode
            .missing
            .iter()
            .any(|field| field == "coverage_and_debt_views"),
        "coverage/debt refs should satisfy the package completeness requirement"
    );
    assert!(
        !mode.missing.iter().any(|field| field == "operations"),
        "operation refs should satisfy the package completeness requirement"
    );

    let browser = report
        .packages
        .get("browser.history")
        .and_then(|package| package.modes.get("browser.history"))
        .expect("browser.history package/mode row");
    assert!(
        browser
            .coverage_debt_refs
            .contains(&"coverage:source-coverage".to_string()),
        "browser history mode must declare the coverage provider consumed by the package gate"
    );
    assert!(
        browser
            .coverage_debt_refs
            .contains(&"debt:unified-debt-view".to_string()),
        "browser history mode must declare the unified debt provider consumed by the package gate"
    );
    assert!(
        browser
            .operation_refs
            .contains(&"operation:browser.web.check".to_string()),
        "browser history mode must declare operator action refs consumed by the package gate"
    );
    assert!(
        !browser
            .missing
            .iter()
            .any(|field| field == "coverage_and_debt_views"),
        "browser coverage/debt refs should satisfy the package completeness requirement"
    );
    assert!(
        !browser.missing.iter().any(|field| field == "operations"),
        "browser operation refs should satisfy the package completeness requirement"
    );

    for mode_id in [
        "email.mailbox",
        "email.mailbox.maildir-staged",
        "email.mailbox.mbox-staged",
    ] {
        let email = report
            .packages
            .get("email.mailbox")
            .and_then(|package| package.modes.get(mode_id))
            .expect("email.mailbox package/mode row");
        assert!(
            email
                .coverage_debt_refs
                .contains(&"coverage:source-coverage".to_string()),
            "email staged mode must declare the coverage provider consumed by the package gate"
        );
        assert!(
            email
                .coverage_debt_refs
                .contains(&"debt:unified-debt-view".to_string()),
            "email staged mode must declare the unified debt provider consumed by the package gate"
        );
        assert!(
            email
                .operation_refs
                .contains(&"operation:email.mailbox.sync".to_string()),
            "email staged mode must declare operator action refs consumed by the package gate"
        );
        assert!(
            !email
                .missing
                .iter()
                .any(|field| field == "coverage_and_debt_views"),
            "email coverage/debt refs should satisfy the package completeness requirement"
        );
        assert!(
            !email.missing.iter().any(|field| field == "operations"),
            "email operation refs should satisfy the package completeness requirement"
        );
    }

    for (package_id, mode_id, operation_ref) in [
        (
            "media.audio-transcript",
            "media.audio-transcript",
            "operation:media.audio-transcript.check",
        ),
        (
            "media.audio-transcript",
            "media.audio-transcript.audio-bundle-staged",
            "operation:media.audio-transcript.import-bundle",
        ),
        (
            "media.screen-ocr",
            "media.screen-ocr",
            "operation:media.screen-ocr.check",
        ),
        (
            "media.screen-ocr",
            "media.screen-ocr.screenshot-ocr-staged",
            "operation:media.screen-ocr.import-screenshots",
        ),
    ] {
        let media = report
            .packages
            .get(package_id)
            .and_then(|package| package.modes.get(mode_id))
            .expect("media staged package/mode row");
        assert!(
            media
                .coverage_debt_refs
                .contains(&"coverage:source-coverage".to_string()),
            "media package mode must declare the coverage provider consumed by the package gate"
        );
        assert!(
            media
                .coverage_debt_refs
                .contains(&"debt:unified-debt-view".to_string()),
            "media package mode must declare the unified debt provider consumed by the package gate"
        );
        assert!(
            media.operation_refs.contains(&operation_ref.to_string()),
            "media package mode must declare operator action refs consumed by the package gate"
        );
        assert!(
            !media
                .missing
                .iter()
                .any(|field| field == "coverage_and_debt_views"),
            "media coverage/debt refs should satisfy the package completeness requirement"
        );
        assert!(
            !media.missing.iter().any(|field| field == "operations"),
            "media operation refs should satisfy the package completeness requirement"
        );
    }

    Ok(())
}

#[sinex_test]
async fn package_completeness_report_distinguishes_proposed_and_manual_modes() -> TestResult<()> {
    use sinexd::sources::package_completeness::{
        PackageModeState, build_package_completeness_report,
    };

    let report = build_package_completeness_report();

    let email = report
        .packages
        .get("email.mailbox")
        .expect("email.mailbox package row");
    assert!(
        email.modes.contains_key("email.mailbox"),
        "received-mail staged mode must be listed"
    );
    assert!(
        email.modes.contains_key("email.mailbox.maildir-staged"),
        "Maildir staged mode must be listed separately"
    );
    assert!(
        email.modes.contains_key("email.mailbox.mbox-staged"),
        "MBOX staged mode must be listed separately"
    );
    assert!(
        email.modes.contains_key("email.mailbox.sent"),
        "sent-mail proposed multi-binding mode must be listed separately"
    );
    for mode_id in [
        "email.mailbox",
        "email.mailbox.maildir-staged",
        "email.mailbox.mbox-staged",
    ] {
        let mode = email.modes.get(mode_id).expect("email staged mode");
        assert_eq!(mode.mode_state, PackageModeState::Accepted);
        assert!(
            mode.missing.is_empty(),
            "accepted email staged mode {mode_id} should satisfy the package gate"
        );
    }
    let sent = email
        .modes
        .get("email.mailbox.sent")
        .expect("sent proposed mode");
    assert_eq!(sent.mode_state, PackageModeState::Proposed);
    assert!(
        sent.sources.catalog_projection_registered,
        "generated source catalog must project every binding mode in multi-binding packages"
    );

    for (package_id, mode_id) in [
        ("media.audio-transcript", "media.audio-transcript"),
        (
            "media.audio-transcript",
            "media.audio-transcript.audio-bundle-staged",
        ),
        ("media.screen-ocr", "media.screen-ocr"),
        ("media.screen-ocr", "media.screen-ocr.screenshot-ocr-staged"),
    ] {
        let mode = report
            .packages
            .get(package_id)
            .and_then(|package| package.modes.get(mode_id))
            .expect("media package mode row");
        assert_eq!(mode.mode_state, PackageModeState::Accepted);
        assert!(
            mode.missing.is_empty(),
            "accepted media package mode should satisfy the package gate"
        );
    }

    let external = report
        .packages
        .get("integration.polylogue")
        .and_then(|package| package.modes.get("integration.polylogue"))
        .expect("integration.polylogue mode row");
    assert_eq!(external.mode_state, PackageModeState::Manual);
    assert_eq!(
        external.manual_reason,
        Some("external_producer_no_local_runtime")
    );
    assert!(!external.sources.parser_factory_registered);
    assert!(!external.sources.source_factory_registered);

    let parser_only = report
        .packages
        .get("weechat.message")
        .and_then(|package| package.modes.get("weechat.message"))
        .expect("weechat.message mode row");
    assert_eq!(parser_only.mode_state, PackageModeState::Manual);
    assert_eq!(
        parser_only.manual_reason,
        Some("parser_only_dispatch_no_source_factory")
    );
    assert!(parser_only.sources.parser_factory_registered);
    assert!(!parser_only.sources.source_factory_registered);
    Ok(())
}

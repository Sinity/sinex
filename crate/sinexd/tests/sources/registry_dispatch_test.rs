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
async fn source_meta_proposed_media_sources_are_metadata_only() -> TestResult<()> {
    use sinex_primitives::source_contracts::{all_source_contracts, source_runtime_bindings};

    for (source_id, event_type) in [
        ("media.audio", "media.audio.transcription"),
        ("media.screen", "media.screen.ocr"),
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
            "proposed media source must still register its SourceContract"
        );
        assert!(
            source_runtime_bindings().any(|binding| {
                binding.source_id == source_id
                    && binding.output_event_type == event_type
                    && binding.proposed
            }),
            "proposed media source must still register a proposed SourceRuntimeBinding"
        );
        assert!(
            find_source_factory(&source).is_none(),
            "proposed media source must not register a source factory"
        );
        assert!(
            find_parser_factory(&source).is_none(),
            "proposed media source must not register a parser factory"
        );
    }
    Ok(())
}

#[sinex_test]
async fn source_meta_email_source_registers_multiple_proposed_bindings() -> TestResult<()> {
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

    let bindings: Vec<_> = source_runtime_bindings()
        .filter(|binding| binding.source_id == "email.mailbox")
        .collect();
    assert_eq!(
        bindings.len(),
        2,
        "email mailbox must register received and sent runtime bindings"
    );
    for (subject, event_type) in [
        ("source:email.mailbox", "email.message.received"),
        ("source:email.mailbox.sent", "email.message.sent"),
    ] {
        assert!(
            bindings.iter().any(|binding| {
                binding.subject.as_str() == subject
                    && binding.output_event_type == event_type
                    && binding.proposed
            }),
            "email mailbox must register proposed binding {subject} -> {event_type}"
        );
    }
    assert!(
        find_source_factory(&source_id).is_none(),
        "proposed email source must not register a source factory"
    );
    assert!(
        find_parser_factory(&source_id).is_none(),
        "proposed email source must not register a parser factory"
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
    use sinexd::sources::package_completeness::{
        PackageCompleteness, PackageModeState, build_package_completeness_report,
    };

    let report = build_package_completeness_report();
    assert_eq!(report.schema_version, 1);
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
        mode.missing
            .iter()
            .any(|field| field == "event_contract_refs"),
        "accepted modes must report the missing #1902 EventContract refs"
    );
    assert!(
        mode.missing
            .iter()
            .any(|field| field == "admission_policy_ref"),
        "accepted modes must report the missing #1900 AdmissionPolicy ref"
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
    use sinex_primitives::event_contracts::SHELL_HISTORY_COMMAND_IMPORTED_CONTRACT_ID;
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
        "received-mail proposed mode must be listed"
    );
    assert!(
        email.modes.contains_key("email.mailbox.sent"),
        "sent-mail proposed multi-binding mode must be listed separately"
    );
    let sent = email
        .modes
        .get("email.mailbox.sent")
        .expect("sent proposed mode");
    assert_eq!(sent.mode_state, PackageModeState::Proposed);
    assert!(
        !sent.sources.catalog_projection_registered,
        "first #1792 slice should expose the current catalog's one-binding collapse for multi-binding packages"
    );

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

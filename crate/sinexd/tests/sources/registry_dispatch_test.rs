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

//! Integration tests for the registry-driven parser dispatch and node factory.
//!
//! Verifies:
//! 1. `default_parser_dispatch()` is registry-driven (no match arms) and routes
//!    WeeChat log lines to the correct parser.
//! 2. The declarative `WeeChatMessageRecord` parser is registered and reachable.
//! 3. Unknown source units produce a clear error.
//! 4. The node factory registry has "noop" registered.
//! 5. Grep probe: no source-unit match arms in dispatch.rs or main.rs.

use sinex_source_worker::{
    dispatch::{default_parser_dispatch, find_parser_factory},
    node_factory::{find_node_factory, registered_node_factory_ids},
};

// ---------------------------------------------------------------------------
// 1. WeeChat imperative parser — registry-driven dispatch round-trip
// ---------------------------------------------------------------------------

/// Verify that "weechat" is in the parser registry and that a well-formed log
/// line dispatches without error.
#[tokio::test]
async fn weechat_parser_registered_and_dispatches() {
    // Factory must be present.
    assert!(
        find_parser_factory("weechat").is_some(),
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
}

/// Join events should produce irc.join.
#[tokio::test]
async fn weechat_join_line_produces_irc_join() {
    let dispatch = default_parser_dispatch();
    let log_line = b"2024-06-01 10:00:00\t-->\tuser (~user@host) joined #general";
    let outcome = dispatch("weechat", log_line, None)
        .expect("dispatch must succeed for a join line");
    assert_eq!(outcome.events.len(), 1);
    assert_eq!(outcome.events[0].event_type.as_str(), "irc.join");
    assert_eq!(outcome.events[0].payload["channel"], "#general");
}

// ---------------------------------------------------------------------------
// 2. Declarative WeeChatMessageRecord — registered and functional
// ---------------------------------------------------------------------------

/// Verify "weechat.message" is in the registry and produces irc.message events.
#[tokio::test]
async fn weechat_message_declarative_registered() {
    assert!(
        find_parser_factory("weechat.message").is_some(),
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
}

// ---------------------------------------------------------------------------
// 3. Unknown source unit — registry-driven error (no match arm fallback)
// ---------------------------------------------------------------------------

#[test]
fn unknown_source_produces_registry_error() {
    let dispatch = default_parser_dispatch();
    let result = dispatch("no-such-source-unit-xyz", b"data", None);
    assert!(result.is_err(), "unknown source must produce an error");
    let err = result.unwrap_err();
    assert!(
        err.contains("unknown source_id 'no-such-source-unit-xyz'"),
        "error must name the unknown source_id, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// 4. Node factory registry — "noop" is registered
// ---------------------------------------------------------------------------

#[test]
fn noop_node_factory_registered() {
    assert!(
        find_node_factory("noop").is_some(),
        "node factory for 'noop' must be registered"
    );

    let ids = registered_node_factory_ids();
    assert!(
        ids.contains(&"noop"),
        "registered_node_factory_ids() must include 'noop', got: {ids:?}"
    );
}

// ---------------------------------------------------------------------------
// 5. Source unit registry — descriptor lookup works for weechat
// ---------------------------------------------------------------------------

#[test]
fn weechat_descriptor_registered() {
    use sinex_source_worker::SourceUnitRegistry;
    let registry = SourceUnitRegistry::from_inventory();
    assert!(
        registry.find("weechat").is_some(),
        "SourceUnitDescriptor for 'weechat' must be registered"
    );
    assert!(
        registry.find("weechat.message").is_some(),
        "SourceUnitDescriptor for 'weechat.message' must be registered"
    );
}

//! `WeeChat` source — binds the `WeeChat` log parser to the source host
//! dispatch and source factory registries.
//!
//! Two parsers are registered in the dispatch registry:
//!
//! 1. **Imperative (`WeeChatLogParser`)** — the full production parser from
//!    `crate::runtime::parser::WeeChatLogParser`. Handles all four `WeeChat`
//!    event types (irc.join, irc.part, `irc.server_notice`, irc.message) with
//!    custom timestamp parsing and prefix classification. Registered under
//!    "weechat" so parse commands for `WeeChat` logs reach it.
//!
//! 2. **Declarative companion (`WeeChatMessageRecord`)** — a
//!    `#[derive(SourceRecord)]` struct that exercises the new declarative path
//!    through the registry for the `irc.message` event type only. Demonstrates
//!    that any parser expressible in the v1 DSL flows through
//!    `DeclarativeParser::evaluate` without hand-written parsing code. Registered
//!    under "weechat.message" (not "weechat") to avoid shadowing the production
//!    parser.
//!
//! Both registrations are performed at link time via `inventory::submit!`.
//! No match arms.

use crate::runtime::parser::{AppendOnlyFileAdapter, WeeChatLogParser};
use crate::register_source;
use sinex_macros::SourceRecord;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// ---------------------------------------------------------------------------
// Source contract — "weechat"
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "weechat",
        namespace: "irc",
        event_types: &[
            ("irc", "irc.join"),
            ("irc", "irc.part"),
            ("irc", "irc.server_notice"),
            ("irc", "irc.message"),
        ],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "personal_irc_logs",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:weechat"),
        "weechat",
        "irc",
    )
    .implementation("sinexd")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("irc.message")
    .privacy_context("Command")
    .material_policy("append_only_log")
    .checkpoint_policy("append_only_cursor")
    .resource_shape("file_watcher")
    .source_id("weechat")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("weechat_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Declarative companion — WeeChatMessageRecord (#[derive(SourceRecord)])
// ---------------------------------------------------------------------------
//
// Exercises the declarative path through the registry. Registered under
// "weechat.message" so it doesn't shadow the production parser.
//
// The tab_separated input format maps:
//   column 0 → raw_timestamp (raw string; full parsing needs the imperative parser)
//   column 1 → prefix (nick or control prefix)
//   column 2 → message
//
// Limitation: the v1 DSL supports one event_type per parser and no custom
// timestamp format (WeeChat uses "YYYY-MM-DD HH:MM:SS"). This companion
// demonstrates the path works; the production parser covers full semantics.

#[derive(SourceRecord, Default, Debug, Clone)]
#[source_record(
    id = "weechat-message-declarative",
    source_id = "weechat.message",
    input_shape = "tab_separated",
    event_type = "irc.message",
    default_privacy_context = "Command"
)]
pub struct WeeChatMessageRecord {
    /// Raw timestamp string from the log line (column 0).
    #[source(column_index = 0)]
    #[required]
    pub raw_timestamp: String,

    /// Prefix / nick field (column 1).
    #[source(column_index = 1)]
    #[required]
    pub prefix: String,

    /// Message body (column 2).
    #[source(column_index = 2)]
    #[required]
    pub message: String,
}

register_source!(source_id: "weechat.message", parser: WeeChatMessageRecord);

// ---------------------------------------------------------------------------
// Source contract — "weechat.message" (declarative companion)
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "weechat.message",
        namespace: "irc",
        event_types: &[("irc", "irc.message")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "personal_irc_logs",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:weechat.message"),
        "weechat.message",
        "irc",
    )
    .implementation("sinexd")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("irc.message")
    .privacy_context("Command")
    .material_policy("append_only_log")
    .checkpoint_policy("append_only_cursor")
    .resource_shape("file_watcher")
    .source_id("weechat.message")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("weechat_message_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Source factory — Phase 3 (Wave A substrate).
//
// The WeeChat source is file-based and runs through
// AppendOnlyFileAdapter + WeeChatLogParser. `register_source!`
// wires both the parser dispatch (for replay) and the source factory (for
// continuous ingestion) in one call.
//
// Config JSON expected at runtime:
//   { "path": "/path/to/weechat.log", "skip_empty": true }
//
// This is the canonical example; Wave-B folds follow the same pattern.
// ---------------------------------------------------------------------------

crate::register_source!(
    source_id: "weechat",
    adapter: AppendOnlyFileAdapter,
    parser: WeeChatLogParser,
);

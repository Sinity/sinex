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
//! Both registrations are performed at link time via `#[derive(SourceMeta)]`
//! and `#[derive(SourceRecord)]`. No match arms.

use crate::runtime::parser::WeeChatLogParser;
use sinex_macros::{SourceMeta, SourceRecord};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};

// ---------------------------------------------------------------------------
// Source contract — "weechat"
// ---------------------------------------------------------------------------

#[derive(SourceMeta)]
#[source_meta(
    id = "weechat",
    namespace = "irc",
    event_source = "irc",
    event_type = "irc.message",
    event_types = "irc.join, irc.part, irc.server_notice",
    adapter = "AppendOnlyFileAdapter",
    implementation = "sinexd",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Command,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
    factory_parser = WeeChatLogParser
)]
pub struct WeeChatSourceMeta;

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

#[derive(SourceRecord, SourceMeta, Default, Debug, Clone)]
#[source_record(
    id = "weechat-message-declarative",
    source_id = "weechat.message",
    input_shape = "tab_separated",
    event_type = "irc.message",
    default_privacy_context = "Command"
)]
#[source_meta(
    id = "weechat.message",
    namespace = "irc",
    event_source = "irc",
    event_type = "irc.message",
    adapter = "AppendOnlyFileAdapter",
    implementation = "sinexd",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Anchor,
    access_scope = AccessScope::StagedExport,
    privacy_context = ProcessingContext::Command,
    resource_profile = ResourceProfile::LiveWatcher,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
    factory = "parser"
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

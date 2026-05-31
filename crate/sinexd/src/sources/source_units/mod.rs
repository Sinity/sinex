//! Source modules — combine mechanisms + parsers per source.
//!
//! Each submodule defines one source unit by binding the SDK adapter to a
//! source-specific parser and registering it with the dispatch + node factory
//! registries via `inventory::submit!`.

use crate::node_sdk::parser::ParserError;
use sinex_primitives::privacy::{self, ProcessingContext};

pub mod ai_session;
pub mod bookmark;
pub mod browser;
pub mod desktop;
pub mod document;
pub mod email;
pub mod finance;
pub mod fs;
pub mod git;
pub mod health;
pub mod knowledgebase;
pub mod library;
pub mod media;
pub mod messaging;
pub mod music;
pub mod polylogue;
pub mod social;
pub mod system;
pub mod terminal;
pub mod weechat;

fn redact_payload_strings(
    payload: serde_json::Value,
    context: ProcessingContext,
) -> Result<serde_json::Value, ParserError> {
    privacy::process_json(&payload, context)
        .map_err(|error| ParserError::Privacy(error.to_string()))
}

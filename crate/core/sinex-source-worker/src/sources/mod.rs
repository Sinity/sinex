//! Source modules — combine mechanisms + parsers per source.
//!
//! Each submodule defines one source unit by binding the SDK adapter to a
//! source-specific parser and registering it with the dispatch + node factory
//! registries via `inventory::submit!`.

pub mod ai_session;
pub mod browser;
pub mod desktop;
pub mod document;
pub mod fs;
pub mod library;
pub mod system;
pub mod terminal;
pub mod weechat;

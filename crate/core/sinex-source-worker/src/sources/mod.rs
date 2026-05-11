//! Source modules — combine mechanisms + parsers per source.
//!
//! Each submodule defines one source unit by binding the SDK adapter to a
//! source-specific parser and registering it with the dispatch + node factory
//! registries via `inventory::submit!`.

pub mod weechat;

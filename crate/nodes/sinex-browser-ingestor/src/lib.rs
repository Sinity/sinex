#![doc = include_str!("../../../lib/sinex-node-sdk/docs/overview.md")]

//! Browser history ingestor that captures historical dump files and browser
//! history `SQLite` databases through the normal node/runtime plane.

mod history_formats;
mod sqlite_sources;
mod unified_node;
mod visit;

pub use sqlite_sources::{BrowserSqliteFormat, BrowserSqliteSourceConfig};
pub use unified_node::{BrowserIngestorConfig, BrowserNode};

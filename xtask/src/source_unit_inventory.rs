//! Link shipped node crates so their inventory source-unit descriptors are
//! visible to xtask.

extern crate sinex_analytics_automaton as _;
extern crate sinex_browser_ingestor as _;
extern crate sinex_daily_summarizer as _;
extern crate sinex_desktop_ingestor as _;
extern crate sinex_document_ingestor as _;
extern crate sinex_fs_ingestor as _;
extern crate sinex_health_automaton as _;
extern crate sinex_hourly_summarizer as _;
extern crate sinex_session_detector as _;
extern crate sinex_system_ingestor as _;
extern crate sinex_terminal_command_canonicalizer as _;
extern crate sinex_terminal_ingestor as _;

pub fn link_source_unit_inventories() {}

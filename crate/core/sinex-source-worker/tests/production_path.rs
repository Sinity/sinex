//! Production-path test harness entry point.
//!
//! This file is the integration test binary root. It declares `mod production_path`
//! so Rust picks up the `tests/production_path/` directory tree.
//!
//! Wave B subagents add `case!(...)` invocations inside the fenced regions in:
//! - `production_path/obligations/initial_ingestion.rs`
//! - `production_path/obligations/privacy.rs`
//!
//! The canary test (`weechat_message_canary`) in
//! `production_path/obligations/initial_ingestion.rs` proves the harness
//! end-to-end and provides a copy-paste template for Wave B.

mod production_path;

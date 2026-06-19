//! Required input-key declarations for terminal parsers.

#[path = "required_input_keys_support.rs"]
mod required_input_keys_support;

use required_input_keys_support::{
    assert_required_input_keys, assert_required_key_blocks_readiness,
};
use sinex_primitives::parser::SourceId;
use sinexd::runtime::parser::SourceRecordFingerprint;
use sinexd::sources::source_contracts::terminal::{
    atuin_history::AtuinHistoryRecord, fish_history::FishHistoryRecord,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn terminal_sqlite_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_required_input_keys(AtuinHistoryRecord::default(), &["command", "timestamp"]);
    assert_required_input_keys(FishHistoryRecord::default(), &["command"]);
    Ok(())
}

#[sinex_test]
async fn atuin_required_schema_removal_blocks_readiness() -> TestResult<()> {
    let before = rusqlite::Connection::open_in_memory()?;
    before.execute_batch(
        "CREATE TABLE history (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );",
    )?;
    let after = rusqlite::Connection::open_in_memory()?;
    after.execute_batch(
        "CREATE TABLE history (
            id TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL
        );",
    )?;

    let before = SourceRecordFingerprint::from_sqlite_connection(&before)?;
    let after = SourceRecordFingerprint::from_sqlite_connection(&after)?;
    let drift = SourceRecordFingerprint::diff(
        SourceId::from_static("terminal.atuin-history"),
        &before,
        &after,
    )
    .expect("removing command should produce schema drift");
    assert_required_key_blocks_readiness(drift, AtuinHistoryRecord::default(), "command");
    Ok(())
}

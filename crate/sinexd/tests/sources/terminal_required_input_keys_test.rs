//! Required input-key declarations for imperative terminal parsers.

use sinexd::node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinex_primitives::{
    parser::SourceId,
    rpc::sources::{CaveatSeverity, caveat_codes},
};
use sinexd::sources::source_contracts::terminal::{
    atuin_history::AtuinHistoryParser, fish_history::FishHistoryParser,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn terminal_sqlite_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_eq!(
        AtuinHistoryParser.required_input_keys(),
        vec!["history.command", "history.timestamp"]
    );
    assert_eq!(
        FishHistoryParser.required_input_keys(),
        vec!["fish_history.command"]
    );
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
    let mut drift = SourceRecordFingerprint::diff(
        SourceId::from_static("terminal.atuin-history"),
        &before,
        &after,
    )
    .expect("removing command should produce schema drift");
    drift.required_input_keys = AtuinHistoryParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("history.command")
    }));
    Ok(())
}

//! Required input-key declarations for the ActivityWatch SQLite parser.

use sinex_node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinex_primitives::{
    parser::SourceUnitId,
    rpc::sources::{CaveatSeverity, caveat_codes},
};
use sinex_source_worker::sources::desktop::activitywatch::ActivityWatchParser;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn activitywatch_parser_declares_required_sqlite_keys() -> TestResult<()> {
    assert_eq!(
        ActivityWatchParser.required_input_keys(),
        vec![
            "buckets.id",
            "buckets.name",
            "events.bucketrow",
            "events.data",
            "events.endtime",
            "events.id",
            "events.starttime",
        ]
    );
    Ok(())
}

#[sinex_test]
async fn activitywatch_required_join_column_removal_blocks_readiness() -> TestResult<()> {
    let before = rusqlite::Connection::open_in_memory()?;
    before.execute_batch(
        "CREATE TABLE buckets (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            bucketrow INTEGER NOT NULL,
            starttime INTEGER NOT NULL,
            endtime INTEGER NOT NULL,
            data TEXT NOT NULL
        );",
    )?;

    let after = rusqlite::Connection::open_in_memory()?;
    after.execute_batch(
        "CREATE TABLE buckets (
            id INTEGER PRIMARY KEY
        );
        CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            bucketrow INTEGER NOT NULL,
            starttime INTEGER NOT NULL,
            endtime INTEGER NOT NULL,
            data TEXT NOT NULL
        );",
    )?;

    let before = SourceRecordFingerprint::from_sqlite_connection(&before)?;
    let after = SourceRecordFingerprint::from_sqlite_connection(&after)?;
    let mut drift = SourceRecordFingerprint::diff(
        SourceUnitId::from_static("desktop.activitywatch"),
        &before,
        &after,
    )
    .expect("removing buckets.name should produce SQLite schema drift");
    drift.required_input_keys = ActivityWatchParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("buckets.name")
    }));
    Ok(())
}

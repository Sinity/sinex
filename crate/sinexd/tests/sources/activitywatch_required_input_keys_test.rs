//! Required input-key declarations for the `ActivityWatch` `SQLite` parser.

#[path = "required_input_keys_support.rs"]
mod required_input_keys_support;

use required_input_keys_support::{
    assert_required_input_keys, assert_required_key_blocks_readiness,
};
use sinex_primitives::parser::SourceId;
use sinexd::runtime::parser::SourceRecordFingerprint;
use sinexd::sources::source_contracts::desktop::activitywatch::ActivityWatchParser;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn activitywatch_parser_declares_required_sqlite_keys() -> TestResult<()> {
    assert_required_input_keys(
        ActivityWatchParser,
        &[
            "buckets.id",
            "buckets.name",
            "events.bucketrow",
            "events.data",
            "events.endtime",
            "events.id",
            "events.starttime",
        ],
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
    let drift = SourceRecordFingerprint::diff(
        SourceId::from_static("desktop.activitywatch"),
        &before,
        &after,
    )
    .expect("removing buckets.name should produce SQLite schema drift");
    assert_required_key_blocks_readiness(drift, ActivityWatchParser, "buckets.name");
    Ok(())
}

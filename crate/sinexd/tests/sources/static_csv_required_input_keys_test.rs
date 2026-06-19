//! Required input-key declarations for imperative static CSV parsers.

#[path = "required_input_keys_support.rs"]
mod required_input_keys_support;

use required_input_keys_support::{
    assert_required_input_keys, assert_required_key_blocks_readiness,
};
use sinex_primitives::parser::SourceId;
use sinexd::runtime::parser::SourceRecordFingerprint;
use sinexd::sources::source_contracts::{
    bookmark::RaindropBookmarkParser,
    health::SleepMergedSummaryParser,
    social::{RedditCommentParser, RedditPostParser},
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn static_csv_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_required_input_keys(
        RaindropBookmarkParser,
        &["id", "url", "created", "favorite"],
    );
    assert_required_input_keys(
        SleepMergedSummaryParser,
        &["sh_datauuid", "start_local", "end_local"],
    );
    assert_required_input_keys(RedditCommentParser, &["id", "date", "subreddit"]);
    assert_required_input_keys(RedditPostParser, &["id", "date", "subreddit"]);
    Ok(())
}

#[sinex_test]
async fn raindrop_required_header_removal_blocks_readiness() -> TestResult<()> {
    let before = SourceRecordFingerprint::from_csv_bytes(
        b"id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite\n",
    )?;
    let after = SourceRecordFingerprint::from_csv_bytes(
        b"id,title,note,excerpt,folder,tags,created,cover,highlights,favorite\n",
    )?;
    let drift =
        SourceRecordFingerprint::diff(SourceId::from_static("raindrop-bookmarks"), &before, &after)
            .expect("removing url should produce CSV shape drift");
    assert_required_key_blocks_readiness(drift, RaindropBookmarkParser, "url");
    Ok(())
}

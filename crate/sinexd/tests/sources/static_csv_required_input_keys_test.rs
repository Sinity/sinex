//! Required input-key declarations for imperative static CSV parsers.

use sinex_node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinex_primitives::{
    parser::SourceUnitId,
    rpc::sources::{CaveatSeverity, caveat_codes},
};
use sinexd::sources::sources::{
    bookmark::RaindropBookmarkParser,
    health::SleepMergedSummaryParser,
    social::{RedditCommentParser, RedditPostParser},
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn static_csv_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_eq!(
        RaindropBookmarkParser.required_input_keys(),
        vec!["id", "url", "created", "favorite"]
    );
    assert_eq!(
        SleepMergedSummaryParser.required_input_keys(),
        vec!["sh_datauuid", "start_local", "end_local"]
    );
    assert_eq!(
        RedditCommentParser.required_input_keys(),
        vec!["id", "date", "subreddit"]
    );
    assert_eq!(
        RedditPostParser.required_input_keys(),
        vec!["id", "date", "subreddit"]
    );
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
    let mut drift = SourceRecordFingerprint::diff(
        SourceUnitId::from_static("raindrop-bookmarks"),
        &before,
        &after,
    )
    .expect("removing url should produce CSV shape drift");
    drift.required_input_keys = RaindropBookmarkParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("url")
    }));
    Ok(())
}

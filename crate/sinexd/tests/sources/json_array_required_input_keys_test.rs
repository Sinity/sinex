//! Required input-key declarations for imperative JSON array export parsers.

#[path = "required_input_keys_support.rs"]
mod required_input_keys_support;

use required_input_keys_support::{
    assert_required_input_keys, assert_required_key_blocks_readiness,
};
use sinex_primitives::parser::SourceId;
use sinexd::runtime::parser::SourceRecordFingerprint;
use sinexd::sources::source_contracts::music::SpotifyHistoryParser;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn spotify_parser_declares_required_array_element_keys() -> TestResult<()> {
    assert_required_input_keys(SpotifyHistoryParser, &["/[]/ts"]);
    Ok(())
}

#[sinex_test]
async fn spotify_required_array_field_removal_blocks_readiness() -> TestResult<()> {
    let before = SourceRecordFingerprint::from_json(&serde_json::json!([
        {
            "ts": "2026-01-01T00:00:00Z",
            "ms_played": 1000,
            "spotify_track_uri": "spotify:track:1"
        }
    ]));
    let after = SourceRecordFingerprint::from_json(&serde_json::json!([
        {
            "ms_played": 1000,
            "spotify_track_uri": "spotify:track:1"
        }
    ]));
    let drift = SourceRecordFingerprint::diff(
        SourceId::from_static("spotify-extended-history"),
        &before,
        &after,
    )
    .expect("removing ts should produce JSON array shape drift");
    assert_required_key_blocks_readiness(drift, SpotifyHistoryParser, "/[]/ts");
    Ok(())
}

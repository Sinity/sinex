//! Required input-key declarations for imperative JSON array export parsers.

use sinex_node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinex_primitives::{
    parser::SourceUnitId,
    rpc::sources::{CaveatSeverity, caveat_codes},
};
use sinex_source_worker::sources::music::SpotifyHistoryParser;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn spotify_parser_declares_required_array_element_keys() -> TestResult<()> {
    assert_eq!(SpotifyHistoryParser.required_input_keys(), vec!["/[]/ts"]);
    Ok(())
}

#[sinex_test]
async fn spotify_required_array_field_removal_blocks_readiness(
    _ctx: TestContext,
) -> TestResult<()> {
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
    let mut drift = SourceRecordFingerprint::diff(
        SourceUnitId::from_static("spotify-extended-history"),
        &before,
        &after,
    )
    .expect("removing ts should produce JSON array shape drift");
    drift.required_input_keys = SpotifyHistoryParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("/[]/ts")
    }));
    Ok(())
}

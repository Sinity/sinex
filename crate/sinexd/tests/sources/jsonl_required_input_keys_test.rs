//! Required input-key declarations for imperative JSONL export parsers.

use sinex_node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinex_primitives::{
    parser::SourceUnitId,
    rpc::sources::{CaveatSeverity, caveat_codes},
};
use sinexd::sources::sources::social::{WykopEntryCommentParser, WykopEntryParser};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn wykop_jsonl_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_eq!(
        WykopEntryParser.required_input_keys(),
        vec!["/[]/entry_id", "/[]/entry_created_at"]
    );
    assert_eq!(
        WykopEntryCommentParser.required_input_keys(),
        vec!["/[]/comment_id", "/[]/comment_created_at"]
    );
    Ok(())
}

#[sinex_test]
async fn wykop_entry_required_timestamp_removal_blocks_readiness() -> TestResult<()> {
    let before = SourceRecordFingerprint::from_jsonl_bytes(
        br#"{"entry_id":76315507,"entry_created_at":"2024-05-18 06:53:25","entry_content":"x"}
"#,
    )?;
    let after = SourceRecordFingerprint::from_jsonl_bytes(
        br#"{"entry_id":76315507,"entry_content":"x"}
"#,
    )?;
    let mut drift =
        SourceRecordFingerprint::diff(SourceUnitId::from_static("wykop-entries"), &before, &after)
            .expect("removing entry_created_at should produce JSONL shape drift");
    drift.required_input_keys = WykopEntryParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("/[]/entry_created_at")
    }));
    Ok(())
}

#[sinex_test]
async fn wykop_comment_required_id_removal_blocks_readiness() -> TestResult<()> {
    let before = SourceRecordFingerprint::from_jsonl_bytes(
        br#"{"comment_id":279391731,"comment_created_at":"2025-02-16 08:21:58","entry_id":80205363}
"#,
    )?;
    let after = SourceRecordFingerprint::from_jsonl_bytes(
        br#"{"comment_created_at":"2025-02-16 08:21:58","entry_id":80205363}
"#,
    )?;
    let mut drift = SourceRecordFingerprint::diff(
        SourceUnitId::from_static("wykop-entry-comments"),
        &before,
        &after,
    )
    .expect("removing comment_id should produce JSONL shape drift");
    drift.required_input_keys = WykopEntryCommentParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("/[]/comment_id")
    }));
    Ok(())
}

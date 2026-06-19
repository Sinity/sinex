//! Required input-key declarations for imperative JSONL export parsers.

#[path = "required_input_keys_support.rs"]
mod required_input_keys_support;

use required_input_keys_support::{
    assert_required_input_keys, assert_required_key_blocks_readiness,
};
use sinex_primitives::parser::SourceId;
use sinexd::runtime::parser::SourceRecordFingerprint;
use sinexd::sources::source_contracts::social::{WykopEntryCommentParser, WykopEntryParser};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn wykop_jsonl_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_required_input_keys(WykopEntryParser, &["/[]/entry_id", "/[]/entry_created_at"]);
    assert_required_input_keys(
        WykopEntryCommentParser,
        &["/[]/comment_id", "/[]/comment_created_at"],
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
    let drift =
        SourceRecordFingerprint::diff(SourceId::from_static("wykop-entries"), &before, &after)
            .expect("removing entry_created_at should produce JSONL shape drift");
    assert_required_key_blocks_readiness(drift, WykopEntryParser, "/[]/entry_created_at");
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
    let drift = SourceRecordFingerprint::diff(
        SourceId::from_static("wykop-entry-comments"),
        &before,
        &after,
    )
    .expect("removing comment_id should produce JSONL shape drift");
    assert_required_key_blocks_readiness(drift, WykopEntryCommentParser, "/[]/comment_id");
    Ok(())
}

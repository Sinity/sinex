//! Required input-key declarations for conversation export parsers.

use sinexd::node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinex_primitives::{
    parser::SourceUnitId,
    rpc::sources::{CaveatSeverity, caveat_codes},
};
use sinexd::sources::sources::{
    ai_session::{ChatGptSessionParser, ClaudeSessionParser},
    messaging::MessengerThreadParser,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn conversation_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_eq!(
        ClaudeSessionParser.required_input_keys(),
        vec!["/[]/uuid", "/[]/chat_messages"]
    );
    assert_eq!(
        ChatGptSessionParser.required_input_keys(),
        vec!["/[]/id", "/[]/current_node", "/[]/mapping"]
    );
    assert_eq!(
        MessengerThreadParser.required_input_keys(),
        vec!["/messages"]
    );
    Ok(())
}

#[sinex_test]
async fn claude_required_conversation_field_removal_blocks_readiness() -> TestResult<()> {
    let before = SourceRecordFingerprint::from_json(&serde_json::json!([
        {
            "uuid": "conversation-1",
            "name": "Conversation",
            "chat_messages": []
        }
    ]));
    let after = SourceRecordFingerprint::from_json(&serde_json::json!([
        {
            "name": "Conversation",
            "chat_messages": []
        }
    ]));
    let mut drift = SourceRecordFingerprint::diff(
        SourceUnitId::from_static("ai-session-claude"),
        &before,
        &after,
    )
    .expect("removing uuid should produce JSON array shape drift");
    drift.required_input_keys = ClaudeSessionParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("/[]/uuid")
    }));
    Ok(())
}

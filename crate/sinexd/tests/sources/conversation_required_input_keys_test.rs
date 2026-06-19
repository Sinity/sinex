//! Required input-key declarations for conversation export parsers.

#[path = "required_input_keys_support.rs"]
mod required_input_keys_support;

use required_input_keys_support::{
    assert_required_input_keys, assert_required_key_blocks_readiness,
};
use sinex_primitives::parser::SourceId;
use sinexd::runtime::parser::SourceRecordFingerprint;
use sinexd::sources::source_contracts::{
    ai_session::{ChatGptSessionParser, ClaudeSessionParser},
    messaging::MessengerThreadParser,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn conversation_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_required_input_keys(ClaudeSessionParser, &["/[]/uuid", "/[]/chat_messages"]);
    assert_required_input_keys(
        ChatGptSessionParser,
        &["/[]/id", "/[]/current_node", "/[]/mapping"],
    );
    assert_required_input_keys(MessengerThreadParser, &["/messages"]);
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
    let drift =
        SourceRecordFingerprint::diff(SourceId::from_static("ai-session-claude"), &before, &after)
            .expect("removing uuid should produce JSON array shape drift");
    assert_required_key_blocks_readiness(drift, ClaudeSessionParser, "/[]/uuid");
    Ok(())
}

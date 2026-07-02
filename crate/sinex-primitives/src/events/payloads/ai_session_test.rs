use super::*;
use crate::events::EventPayload as _;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn claude_declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(ClaudeAiMessagePayload::SOURCE.as_static_str(), "claude");
    assert_eq!(
        ClaudeAiMessagePayload::EVENT_TYPE.as_static_str(),
        "ai.message"
    );
    Ok(())
}

#[sinex_test]
async fn chatgpt_declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(ChatGptAiMessagePayload::SOURCE.as_static_str(), "chatgpt");
    assert_eq!(
        ChatGptAiMessagePayload::EVENT_TYPE.as_static_str(),
        "ai.message"
    );
    Ok(())
}

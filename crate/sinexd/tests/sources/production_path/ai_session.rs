//! Production-path obligation tests for AI session exports.
//!
//! Source contracts covered:
//! - `ai-session-claude`  (static JSON export + `ClaudeSessionParser`)
//! - `ai-session-chatgpt` (static JSON export + `ChatGptSessionParser`)

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    const CLAUDE_FIXTURE: &[u8] = br#"[
      {
        "uuid": "conv-aaa",
        "name": "Harness Claude Session",
        "chat_messages": [
          {
            "uuid": "msg-001",
            "sender": "human",
            "created_at": "2024-06-01T10:00:00.000000Z",
            "content": [{"type": "text", "text": "Hello there"}]
          },
          {
            "uuid": "msg-002",
            "sender": "assistant",
            "created_at": "2024-06-01T10:00:05.000000Z",
            "content": [{"type": "text", "text": "Hi!"}]
          }
        ]
      }
    ]"#;

    const CHATGPT_FIXTURE: &[u8] = br#"[
      {
        "id": "chatgpt-conv-1",
        "title": "Harness ChatGPT Session",
        "current_node": "node-asst",
        "default_model_slug": "gpt-4",
        "mapping": {
          "node-root": {
            "parent": null,
            "children": ["node-user"],
            "message": null
          },
          "node-user": {
            "parent": "node-root",
            "children": ["node-asst"],
            "message": {
              "id": "node-user",
              "author": {"role": "user"},
              "create_time": 1717228800.0,
              "content": {"content_type": "text", "parts": ["Hello GPT"]},
              "metadata": {}
            }
          },
          "node-asst": {
            "parent": "node-user",
            "children": [],
            "message": {
              "id": "node-asst",
              "author": {"role": "assistant"},
              "create_time": 1717228860.0,
              "content": {"content_type": "text", "parts": ["Hello user!"]},
              "metadata": {"model_slug": "gpt-4o"}
            }
          }
        }
      }
    ]"#;

    const CLAUDE_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "ai-session-claude",
        "ai-session-claude",
        crate::AdapterKind::StaticFile,
        CLAUDE_FIXTURE,
        &["ai.message"],
    );

    const CHATGPT_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
        "ai-session-chatgpt",
        "ai-session-chatgpt",
        crate::AdapterKind::StaticFile,
        CHATGPT_FIXTURE,
        &["ai.message"],
    );

    #[sinex_test]
    async fn ai_session_claude_obligations() -> TestResult<()> {
        crate::run_production_path_case(CLAUDE_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }

    #[sinex_test]
    async fn ai_session_chatgpt_obligations() -> TestResult<()> {
        crate::run_production_path_case(CHATGPT_CASE)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("{e}"))?;
        Ok(())
    }
}

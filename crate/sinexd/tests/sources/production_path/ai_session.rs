//! Production-path obligation tests for AI session exports.
//!
//! Source contracts covered:
//! - `ai-session-claude`  (static JSON export + `ClaudeSessionParser`)
//! - `ai-session-chatgpt` (static JSON export + `ChatGptSessionParser`)

#[cfg(test)]
#[path = "ai_session_test.rs"]
mod tests;

//! Git Activity Detector - Example `TransducerNode` Implementation
//!
//! This example demonstrates how to use `TransducerNode` to create
//! a node that detects git commands from terminal events.
//!
//! Run with: cargo run --example `git_activity_detector`

use serde::{Deserialize, Serialize};
use sinex_node_sdk::Timestamp;
use sinex_node_sdk::derived_node::{DerivedOutput, DerivedTriggerContext};
use sinex_node_sdk::{NodeLogicError, TransducerNode};
use sinex_primitives::privacy::ProcessingContext;
use std::collections::HashMap;

// ============================================================================
// Input Event Type
// ============================================================================

/// Terminal command event (from terminal.command.executed)
#[derive(Debug, Clone, Deserialize)]
pub struct TerminalCommandEvent {
    /// The command that was executed
    pub command: String,
    /// Working directory where command was run
    #[serde(default)]
    pub cwd: String,
    /// Exit code of the command
    #[serde(default)]
    pub exit_code: i32,
    /// When the command was executed
    #[serde(default = "Timestamp::now")]
    pub timestamp: Timestamp,
}

// ============================================================================
// Output Event Type
// ============================================================================

/// Git activity detected (emitted as git.activity.detected)
#[derive(Debug, Clone, Serialize)]
pub struct GitActivityEvent {
    /// Git subcommand (commit, push, pull, etc.)
    pub subcommand: String,
    /// Repository path
    pub repo_path: String,
    /// Full command that was run
    pub full_command: String,
    /// Command exit code (0 = success)
    pub exit_code: i32,
    /// Whether the command succeeded
    pub success: bool,
    /// Timestamp of the activity
    pub timestamp: Timestamp,
}

// ============================================================================
// Node State
// ============================================================================

/// State persisted across restarts
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GitActivityState {
    /// Count of commands by repo path
    pub commands_by_repo: HashMap<String, u64>,
    /// Count of commands by subcommand
    pub commands_by_type: HashMap<String, u64>,
    /// Total git commands seen
    pub total_commands: u64,
    /// Last activity timestamp
    pub last_activity: Option<Timestamp>,
}

// ============================================================================
// TransducerNode Implementation
// ============================================================================

/// Git Activity Detector - detects git commands from terminal events
pub struct GitActivityDetector;

impl GitActivityDetector {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Extract git subcommand from full command string
    fn extract_subcommand(command: &str) -> Option<String> {
        let parts: Vec<&str> = command.split_whitespace().collect();

        // Find "git" and get the next word
        for (i, part) in parts.iter().enumerate() {
            if *part == "git" {
                return parts.get(i + 1).map(std::string::ToString::to_string);
            }
        }

        None
    }
}

impl Default for GitActivityDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl TransducerNode for GitActivityDetector {
    type State = GitActivityState;
    type Input = TerminalCommandEvent;
    type Output = GitActivityEvent;

    fn name(&self) -> &'static str {
        "git-activity-detector"
    }

    fn input_event_type(&self) -> &'static str {
        "terminal.command.executed"
    }

    fn output_event_type(&self) -> &'static str {
        "git.activity.detected"
    }

    fn output_privacy_context(&self) -> ProcessingContext {
        ProcessingContext::Command
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
        context: &DerivedTriggerContext,
    ) -> Result<Option<DerivedOutput<Self::Output>>, NodeLogicError> {
        // Filter: only process git commands
        if !input.command.trim_start().starts_with("git ") {
            return Ok(None);
        }

        // Extract subcommand
        let subcommand =
            Self::extract_subcommand(&input.command).unwrap_or_else(|| "unknown".to_string());

        // Update state
        state.total_commands += 1;
        state.last_activity = Some(input.timestamp);

        *state.commands_by_repo.entry(input.cwd.clone()).or_insert(0) += 1;
        *state
            .commands_by_type
            .entry(subcommand.clone())
            .or_insert(0) += 1;

        // 1:1 transform: ts_orig from input, single parent
        Ok(Some(DerivedOutput::transduced(
            GitActivityEvent {
                subcommand,
                repo_path: input.cwd,
                full_command: input.command,
                exit_code: input.exit_code,
                success: input.exit_code == 0,
                timestamp: input.timestamp,
            },
            context.ts_orig.unwrap_or_else(Timestamp::now),
            context.trigger_uuid(),
        )))
    }
}

// ============================================================================
// Main - Demonstration
// ============================================================================

fn main() {
    println!("Git Activity Detector - TransducerNode Example");
    println!("================================================");
    println!();
    println!("This demonstrates TransducerNode with ~100 lines of code:");
    println!("  - Input:  terminal.command.executed");
    println!("  - Output: git.activity.detected");
    println!("  - State:  Command counts by repo and type");
    println!();
    println!("For local experimentation, run:");
    println!("  cargo run -p sinex-node-sdk --example git_activity_detector");
    println!("For workspace-managed binaries, use xtask run ... after wrapping with TransducerNodeAdapter.");
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::domain::{ProcessingMode, TriggerKind};
    use sinex_primitives::events::Event;
    use sinex_primitives::{Id, JsonValue};
    use xtask::sandbox::prelude::*;

    fn test_context() -> DerivedTriggerContext {
        let event_id: Id<Event<JsonValue>> = Id::new();
        DerivedTriggerContext {
            trigger_event_id: event_id,
            source: "test".into(),
            event_type: "terminal.command.executed".into(),
            ts_orig: None,
            ts_coided: event_id.timestamp(),
            processing_mode: ProcessingMode::Live,
            trigger_kind: TriggerKind::NewEvent,
            created_by_operation_id: None,
        }
    }

    #[sinex_test]
    async fn test_filters_non_git_commands() -> TestResult<()> {
        let mut node = GitActivityDetector::new();
        let mut state = GitActivityState::default();

        let input = TerminalCommandEvent {
            command: "ls -la".to_string(),
            cwd: "/home/user".to_string(),
            exit_code: 0,
            timestamp: Timestamp::now(),
        };

        let context = test_context();
        let result = node.process(&mut state, input, &context).await.unwrap();
        assert!(result.is_none());
        assert_eq!(state.total_commands, 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_detects_git_commit() -> TestResult<()> {
        let mut node = GitActivityDetector::new();
        let mut state = GitActivityState::default();

        let input = TerminalCommandEvent {
            command: "git commit -m 'test'".to_string(),
            cwd: "/home/user/project".to_string(),
            exit_code: 0,
            timestamp: Timestamp::now(),
        };

        let context = test_context();
        let result = node.process(&mut state, input, &context).await.unwrap();
        assert!(result.is_some());

        let output = result.unwrap();
        assert_eq!(output.payload.subcommand, "commit");
        assert_eq!(output.payload.repo_path, "/home/user/project");
        assert!(output.payload.success);

        assert_eq!(state.total_commands, 1);
        assert_eq!(state.commands_by_type.get("commit"), Some(&1));
        Ok(())
    }

    #[sinex_test]
    async fn test_tracks_state_across_calls() -> TestResult<()> {
        let mut node = GitActivityDetector::new();
        let mut state = GitActivityState::default();

        // First command
        let input1 = TerminalCommandEvent {
            command: "git status".to_string(),
            cwd: "/repo1".to_string(),
            exit_code: 0,
            timestamp: Timestamp::now(),
        };
        let context = test_context();
        node.process(&mut state, input1, &context).await.unwrap();

        // Second command (same repo)
        let input2 = TerminalCommandEvent {
            command: "git push".to_string(),
            cwd: "/repo1".to_string(),
            exit_code: 0,
            timestamp: Timestamp::now(),
        };
        node.process(&mut state, input2, &context).await.unwrap();

        // Third command (different repo)
        let input3 = TerminalCommandEvent {
            command: "git pull".to_string(),
            cwd: "/repo2".to_string(),
            exit_code: 0,
            timestamp: Timestamp::now(),
        };
        node.process(&mut state, input3, &context).await.unwrap();

        assert_eq!(state.total_commands, 3);
        assert_eq!(state.commands_by_repo.get("/repo1"), Some(&2));
        assert_eq!(state.commands_by_repo.get("/repo2"), Some(&1));
        Ok(())
    }

    #[sinex_test]
    async fn test_extracts_subcommand() -> TestResult<()> {
        assert_eq!(
            GitActivityDetector::extract_subcommand("git commit -m 'msg'"),
            Some("commit".to_string())
        );
        assert_eq!(
            GitActivityDetector::extract_subcommand("git push origin main"),
            Some("push".to_string())
        );
        assert_eq!(
            GitActivityDetector::extract_subcommand("sudo git pull"),
            Some("pull".to_string())
        );
        assert_eq!(GitActivityDetector::extract_subcommand("ls -la"), None);
        Ok(())
    }
}

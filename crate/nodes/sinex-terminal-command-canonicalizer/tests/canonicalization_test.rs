//! Tests for the terminal command canonicalizer's `process()` method.
//!
//! Validates source filtering, JSON field extraction, exit code parsing,
//! timestamp fallback, and empty-command handling.

use sinex_node_sdk::TransducerNode;
use sinex_node_sdk::derived_node::DerivedTriggerContext;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::temporal::{Timestamp, now};
use sinex_primitives::{Id, JsonValue};
use sinex_terminal_command_canonicalizer::TerminalCommandCanonicalizer;
use xtask::sandbox::prelude::*;

fn make_context(source: &str, event_type: &str) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: source.into(),
        event_type: event_type.into(),
        ts_orig: Some(Timestamp::now()),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

// ── Source Filtering ────────────────────────────────────────────────────

#[sinex_test]
async fn test_accepts_shell_kitty_source() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "ls -la"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_some(), "shell.kitty should be accepted");
    assert_eq!(result.unwrap().payload.command, "ls -la");
    Ok(())
}

#[sinex_test]
async fn test_accepts_shell_atuin_source() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.atuin", "command.executed");

    let input = serde_json::json!({"command": "git status"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_some(), "shell.atuin should be accepted");
    assert_eq!(result.unwrap().payload.command, "git status");
    Ok(())
}

#[sinex_test]
async fn test_accepts_shell_history_bash() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.bash", "command.executed");

    let input = serde_json::json!({"command": "echo hello"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_some(), "shell.history.bash should be accepted");
    Ok(())
}

#[sinex_test]
async fn test_accepts_shell_history_zsh() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.zsh", "command.executed");

    let input = serde_json::json!({"command": "cd /tmp"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_some(), "shell.history.zsh should be accepted");
    Ok(())
}

#[sinex_test]
async fn test_accepts_shell_history_fish() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.fish", "command.executed");

    let input = serde_json::json!({"command": "set -x PATH /usr/bin"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_some(), "shell.history.fish should be accepted");
    Ok(())
}

#[sinex_test]
async fn test_rejects_unknown_source() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("unknown.source", "command.executed");

    let input = serde_json::json!({"command": "ls"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_none(), "unknown source should be rejected");
    Ok(())
}

#[sinex_test]
async fn test_rejects_shell_prefix_but_wrong_variant() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.nushell", "command.executed");

    let input = serde_json::json!({"command": "ls"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(
        result.is_none(),
        "shell.nushell is not in the accepted list"
    );
    Ok(())
}

// ── Empty/Missing Command ───────────────────────────────────────────────

#[sinex_test]
async fn test_empty_command_returns_none() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": ""});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_none(), "empty command should be skipped");
    Ok(())
}

#[sinex_test]
async fn test_whitespace_only_command_returns_none() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "   \t  "});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(
        result.is_none(),
        "whitespace-only command should be skipped"
    );
    Ok(())
}

#[sinex_test]
async fn test_missing_command_field_returns_none() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"working_directory": "/tmp"});
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_none(), "missing command field should be skipped");
    Ok(())
}

// ── JSON Field Extraction ───────────────────────────────────────────────

#[sinex_test]
async fn test_extracts_all_fields() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({
        "command": "cargo build",
        "working_directory": "/home/user/project",
        "exit_code": 0,
        "duration_ms": 1500,
        "user": "testuser",
        "session_id": "sess-001",
        "environment_hash": "abc123"
    });
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(result.payload.command, "cargo build");
    assert_eq!(result.payload.working_directory, "/home/user/project");
    assert_eq!(
        result.payload.exit_code,
        sinex_primitives::units::ExitCode::from_raw(0)
    );
    assert_eq!(result.payload.duration_ms, 1500);
    assert_eq!(result.payload.user, "testuser");
    assert_eq!(result.payload.session_id, "sess-001");
    assert_eq!(result.payload.environment_hash, "abc123");
    Ok(())
}

#[sinex_test]
async fn test_defaults_for_missing_optional_fields() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "pwd"});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(result.payload.command, "pwd");
    assert_eq!(result.payload.working_directory, "");
    assert_eq!(
        result.payload.exit_code,
        sinex_primitives::units::ExitCode::from_raw(0)
    );
    assert_eq!(result.payload.duration_ms, 0);
    assert_eq!(result.payload.user, "");
    assert_eq!(result.payload.session_id, "");
    assert_eq!(result.payload.environment_hash, "");
    Ok(())
}

// ── Exit Code Parsing ───────────────────────────────────────────────────

#[sinex_test]
async fn test_exit_code_nonzero() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "false", "exit_code": 1});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(
        result.payload.exit_code,
        sinex_primitives::units::ExitCode::from_raw(1)
    );
    Ok(())
}

#[sinex_test]
async fn test_exit_code_signal_killed() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "sleep 100", "exit_code": 137});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(
        result.payload.exit_code,
        sinex_primitives::units::ExitCode::from_raw(137)
    );
    Ok(())
}

// ── Timestamp Handling ──────────────────────────────────────────────────

#[sinex_test]
async fn test_end_time_rfc3339_parsing() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({
        "command": "date",
        "end_time": "2025-01-15T10:30:00Z"
    });
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    let parsed = sinex_primitives::temporal::parse_rfc3339("2025-01-15T10:30:00Z").unwrap();
    assert_eq!(result.payload.end_time, parsed);
    Ok(())
}

#[sinex_test]
async fn test_end_time_fallback_on_invalid_rfc3339() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let before = now();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({
        "command": "date",
        "end_time": "not-a-timestamp"
    });
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert!(
        result.payload.end_time >= before,
        "fallback should be >= test start time"
    );
    Ok(())
}

// ── Source Event Tracking ───────────────────────────────────────────────

#[sinex_test]
async fn test_source_events_contains_context_event_id() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");
    let expected_id = ctx.trigger_uuid().to_string();

    let input = serde_json::json!({"command": "whoami"});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(result.payload.source_events.len(), 1);
    assert_eq!(result.payload.source_events[0], expected_id);
    Ok(())
}

#[sinex_test]
async fn test_enrichment_history_starts_empty() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "test"});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert!(result.payload.enrichment_history.is_empty());
    Ok(())
}

// ── Derived Output Metadata ─────────────────────────────────────────────

#[sinex_test]
async fn test_transducer_temporal_policy_is_inherit_parent() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "echo hello"});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(
        result.temporal_policy,
        sinex_primitives::domain::SyntheticTemporalPolicy::InheritParent,
    );
    Ok(())
}

#[sinex_test]
async fn test_transducer_ts_orig_inherits_from_context() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");
    let expected_ts = ctx.ts_orig.unwrap();

    let input = serde_json::json!({"command": "ls"});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(result.ts_orig, expected_ts);
    Ok(())
}

#[sinex_test]
async fn test_transducer_single_source_event_id() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"command": "pwd"});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(result.source_event_ids.len(), 1);
    assert_eq!(result.source_event_ids[0], ctx.trigger_uuid());
    Ok(())
}

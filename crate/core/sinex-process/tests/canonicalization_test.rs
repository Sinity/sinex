//! Tests for the terminal command canonicalizer's `process()` method.
//!
//! Validates source filtering, JSON field extraction, exit code parsing,
//! timestamp fallback, and empty-command handling.

use sinex_node_sdk::derived_node::DerivedTriggerContext;
use sinex_node_sdk::{NodeLogicError, TransducerNode};
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::CanonicalCommandPayload;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
use sinex_process::automata::canonicalizer::TerminalCommandCanonicalizer;
use xtask::sandbox::prelude::*;

fn make_context_with_optional_ts(
    source: &str,
    event_type: &str,
    ts_orig: Option<Timestamp>,
) -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: source.into(),
        event_type: event_type.into(),
        ts_orig,
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn make_context(source: &str, event_type: &str) -> DerivedTriggerContext {
    make_context_with_optional_ts(source, event_type, Some(Timestamp::now()))
}

fn kitty_input(command: &str) -> JsonValue {
    serde_json::json!({
        "command": command,
        "kitty_window_id": "1",
        "kitty_tab_id": "1"
    })
}

// ── Source Filtering ────────────────────────────────────────────────────

#[sinex_test]
async fn test_accepts_shell_kitty_source() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = kitty_input("ls -la");
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_some(), "shell.kitty should be accepted");
    assert_eq!(result.unwrap().payload.command, "ls -la");
    Ok(())
}

#[sinex_test]
async fn test_emits_payload_declared_source() -> TestResult<()> {
    let canon = TerminalCommandCanonicalizer::new();
    assert_eq!(
        canon.output_event_source(),
        CanonicalCommandPayload::SOURCE.as_static_str()
    );
    Ok(())
}

#[sinex_test]
async fn test_accepts_shell_atuin_source() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.atuin", "command.executed");

    let input = serde_json::json!({
        "command_string": "git status",
        "cwd": "/home/user/project",
        "exit_code": 0,
        "duration_ns": 1_500_000_000u64,
        "atuin_history_id": "hist-001",
        "atuin_session_id": "sess-001",
        "timestamp": 1_735_000_000,
        "ts_start_orig": "2025-01-15T10:29:58.500Z",
        "ts_end_orig": "2025-01-15T10:30:00Z",
        "hostname": "test-host"
    });
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_some(), "shell.atuin should be accepted");
    let payload = result.unwrap().payload;
    assert_eq!(payload.command, "git status");
    assert_eq!(
        payload.working_directory.as_deref(),
        Some("/home/user/project")
    );
    assert_eq!(payload.duration_ms, Some(1500));
    assert_eq!(payload.session_id.as_deref(), Some("sess-001"));
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
async fn test_missing_ts_orig_is_rejected() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context_with_optional_ts("shell.kitty", "command.executed", None);

    let error = canon
        .process(&mut state, kitty_input("ls -la"), &ctx)
        .await
        .expect_err("missing ts_orig must be rejected");

    assert!(
        matches!(&error, NodeLogicError::InputParsing(msg) if msg.contains("missing ts_orig")),
        "expected InputParsing with 'missing ts_orig', got: {error:?}"
    );
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

    let input = kitty_input("");
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(result.is_none(), "empty command should be skipped");
    Ok(())
}

#[sinex_test]
async fn test_whitespace_only_command_returns_none() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = kitty_input("   \t  ");
    let result = canon.process(&mut state, input, &ctx).await.unwrap();

    assert!(
        result.is_none(),
        "whitespace-only command should be skipped"
    );
    Ok(())
}

#[sinex_test]
async fn test_missing_command_field_errors() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({"working_directory": "/tmp", "kitty_window_id": "1", "kitty_tab_id": "1"});
    let error = canon
        .process(&mut state, input, &ctx)
        .await
        .expect_err("missing required command should fail honestly");

    assert!(
        error
            .to_string()
            .contains("failed to parse shell.kitty command.executed payload"),
        "unexpected error: {error}"
    );
    Ok(())
}

// ── JSON Field Extraction ───────────────────────────────────────────────

#[sinex_test]
async fn test_extracts_all_fields() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.bash", "command.executed");

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
    assert_eq!(
        result.payload.working_directory.as_deref(),
        Some("/home/user/project")
    );
    assert_eq!(
        result.payload.exit_code,
        Some(sinex_primitives::units::ExitCode::from_raw(0))
    );
    assert_eq!(result.payload.duration_ms, Some(1500));
    assert_eq!(result.payload.user.as_deref(), Some("testuser"));
    assert_eq!(result.payload.session_id.as_deref(), Some("sess-001"));
    assert_eq!(result.payload.environment_hash.as_deref(), Some("abc123"));
    Ok(())
}

#[sinex_test]
async fn test_missing_optional_fields_remain_unknown() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.bash", "command.executed");

    let input = serde_json::json!({"command": "pwd"});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(result.payload.command, "pwd");
    assert_eq!(result.payload.working_directory, None);
    assert_eq!(result.payload.exit_code, None);
    assert_eq!(result.payload.duration_ms, None);
    assert_eq!(result.payload.user, None);
    assert_eq!(result.payload.session_id, None);
    assert_eq!(result.payload.environment_hash, None);
    Ok(())
}

// ── Exit Code Parsing ───────────────────────────────────────────────────

#[sinex_test]
async fn test_exit_code_nonzero() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.bash", "command.executed");

    let input = serde_json::json!({"command": "false", "exit_code": 1});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(
        result.payload.exit_code,
        Some(sinex_primitives::units::ExitCode::from_raw(1))
    );
    Ok(())
}

#[sinex_test]
async fn test_exit_code_signal_killed() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.bash", "command.executed");

    let input = serde_json::json!({"command": "sleep 100", "exit_code": 137});
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(
        result.payload.exit_code,
        Some(sinex_primitives::units::ExitCode::from_raw(137))
    );
    Ok(())
}

// ── Timestamp Handling ──────────────────────────────────────────────────

#[sinex_test]
async fn test_atuin_timestamps_are_preserved() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.atuin", "command.executed");

    let input = serde_json::json!({
        "command_string": "date",
        "cwd": "/tmp",
        "exit_code": 0,
        "duration_ns": 1_500_000_000u64,
        "atuin_history_id": "hist-001",
        "atuin_session_id": "sess-001",
        "timestamp": 1_735_000_000,
        "ts_start_orig": "2025-01-15T10:29:58.500Z",
        "ts_end_orig": "2025-01-15T10:30:00Z",
        "hostname": "test-host"
    });
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    let start = sinex_primitives::temporal::parse_rfc3339("2025-01-15T10:29:58.500Z").unwrap();
    let end = sinex_primitives::temporal::parse_rfc3339("2025-01-15T10:30:00Z").unwrap();
    assert_eq!(result.payload.start_time, start);
    assert_eq!(result.payload.end_time, end);
    assert_eq!(result.payload.duration_ms, Some(1500));
    Ok(())
}

#[sinex_test]
async fn test_invalid_atuin_payload_shape_errors() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.atuin", "command.executed");

    let input = serde_json::json!({
        "command": "date",
        "working_directory": "/tmp",
        "exit_code": 0,
        "duration_ms": 1000
    });
    let error = canon
        .process(&mut state, input, &ctx)
        .await
        .expect_err("old generic shape should not be accepted for shell.atuin");

    assert!(
        error
            .to_string()
            .contains("failed to parse shell.atuin command.executed payload"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_optional_field_type_errors() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.history.bash", "command.executed");

    let input = serde_json::json!({
        "command": "echo hello",
        "working_directory": 42,
    });
    let error = canon
        .process(&mut state, input, &ctx)
        .await
        .expect_err("malformed optional fields should fail honestly");

    assert!(
        error
            .to_string()
            .contains("failed to parse shell.history.bash command.executed payload"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn test_non_string_command_field_errors() -> TestResult<()> {
    let mut canon = TerminalCommandCanonicalizer::new();
    let mut state = ();
    let ctx = make_context("shell.kitty", "command.executed");

    let input = serde_json::json!({
        "command": 42,
        "kitty_window_id": "1",
        "kitty_tab_id": "1"
    });
    let error = canon
        .process(&mut state, input, &ctx)
        .await
        .expect_err("non-string commands should fail honestly");

    assert!(
        error
            .to_string()
            .contains("failed to parse shell.kitty command.executed payload"),
        "unexpected error: {error}"
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

    let input = kitty_input("whoami");
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

    let input = kitty_input("test");
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

    let input = kitty_input("echo hello");
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

    let input = kitty_input("ls");
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

    let input = kitty_input("pwd");
    let result = canon
        .process(&mut state, input, &ctx)
        .await
        .unwrap()
        .expect("should produce output");

    assert_eq!(result.source_event_ids.len(), 1);
    assert_eq!(result.source_event_ids[0], ctx.trigger_uuid());
    Ok(())
}

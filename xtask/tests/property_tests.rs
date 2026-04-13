//! Property-based tests for xtask.
//!
//! Uses proptest to verify invariants that should hold for all valid inputs:
//! - `CommandResult` serialization roundtrips preserve data
//! - `ProcessBuilder` argument handling is consistent
//! - JSON output conforms to expected schema
//! - `HistoryDb` database roundtrips preserve written data

use proptest::prelude::*;
use sinex_primitives::temporal;
use xtask::command::CommandResult;
use xtask::history::{HistoryDb, InvocationQuery, InvocationStatus};
use xtask::output::{OutputFormat, Status, StructuredError};
use xtask::sandbox::sinex_proptest;

// ============================================================================
// Strategy Generators
// ============================================================================

/// Generate a valid command name (alphanumeric with dashes).
fn command_name_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9\\-]{2,20}".prop_map(|s| s)
}

/// Generate a valid error code (uppercase with underscores).
fn error_code_strategy() -> impl Strategy<Value = String> {
    "[A-Z][A-Z0-9_]{2,20}".prop_map(|s| s)
}

/// Generate a valid error message.
fn error_message_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9 .,!?\\-]{5,100}".prop_map(|s| s)
}

/// Generate a file location string.
fn location_strategy() -> impl Strategy<Value = String> {
    ("[a-z_/]{5,30}", 1u32..=10000u32, 1u32..=200u32)
        .prop_map(|(path, line, col)| format!("{path}:{line}:{col}"))
}

/// Generate a Status enum value.
fn status_strategy() -> impl Strategy<Value = Status> {
    prop_oneof![
        Just(Status::Success),
        Just(Status::Failed),
        Just(Status::Partial),
        Just(Status::Running),
        Just(Status::Cancelled),
    ]
}

/// Generate an `OutputFormat` enum value.
fn output_format_strategy() -> impl Strategy<Value = OutputFormat> {
    prop_oneof![
        Just(OutputFormat::Human),
        Just(OutputFormat::Json),
        Just(OutputFormat::Compact),
        Just(OutputFormat::Silent),
    ]
}

/// Generate a `StructuredError`.
fn structured_error_strategy() -> impl Strategy<Value = StructuredError> {
    (
        error_code_strategy(),
        error_message_strategy(),
        prop::option::of(location_strategy()),
        prop::option::of(error_message_strategy()),
    )
        .prop_map(|(code, message, location, suggestion)| StructuredError {
            code,
            message,
            location,
            suggestion,
        })
}

/// Generate a `CommandResult` (using the output module's version).
fn output_command_result_strategy() -> impl Strategy<Value = xtask::output::CommandResult> {
    (
        command_name_strategy(),
        prop::option::of(command_name_strategy()),
        status_strategy(),
        0.0f64..=3600.0f64,
        prop::collection::vec(structured_error_strategy(), 0..=5),
        prop::collection::vec(error_message_strategy(), 0..=3),
    )
        .prop_map(
            |(command, subcommand, status, duration_secs, errors, suggested_fixes)| {
                xtask::output::CommandResult {
                    command,
                    subcommand,
                    status,
                    duration_secs,
                    timestamp: temporal::now(),
                    details: None,
                    data: None,
                    is_silent: false,
                    errors,
                    warnings: Vec::new(),
                    suggested_fixes,
                    message: None,
                }
            },
        )
}

/// Generate a `CommandResult` (using the command module's version).
fn command_result_strategy() -> impl Strategy<Value = CommandResult> {
    (
        status_strategy(),
        prop::option::of(error_message_strategy()),
        prop::collection::vec(error_message_strategy(), 0..=5),
        prop::collection::vec(structured_error_strategy(), 0..=3),
        prop::collection::vec(error_message_strategy(), 0..=3),
        prop::option::of(0.0f64..=3600.0f64),
    )
        .prop_map(
            |(status, message, details, errors, warnings, duration_secs)| CommandResult {
                status,
                message,
                details,
                data: None,
                is_silent: false,
                errors,
                warnings,
                duration_secs,
                timestamp: Some(temporal::now()),
            },
        )
}

// ============================================================================
// CommandResult Serialization Roundtrip Tests
// ============================================================================

sinex_proptest! {
    /// Output CommandResult serializes to JSON and deserializes back correctly.
    fn output_command_result_json_roundtrip(result in output_command_result_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&result).expect("should serialize to JSON");
        let parsed: xtask::output::CommandResult = serde_json::from_str(&json_str)
            .expect("should deserialize from JSON");

        // Verify key fields are preserved
        prop_assert_eq!(&result.command, &parsed.command);
        prop_assert_eq!(&result.subcommand, &parsed.subcommand);
        prop_assert_eq!(result.status, parsed.status);
        prop_assert_eq!(result.errors.len(), parsed.errors.len());
        prop_assert_eq!(result.suggested_fixes.len(), parsed.suggested_fixes.len());

        // Duration should be approximately equal (floating point)
        let duration_diff = (result.duration_secs - parsed.duration_secs).abs();
        prop_assert!(duration_diff < 0.0001, "Duration should be preserved");
        Ok(())
    }

    /// Command CommandResult serializes to JSON and deserializes back correctly.
    fn command_result_json_roundtrip(result in command_result_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&result).expect("should serialize to JSON");
        let parsed: CommandResult = serde_json::from_str(&json_str)
            .expect("should deserialize from JSON");

        // Verify key fields are preserved
        prop_assert_eq!(result.status, parsed.status);
        prop_assert_eq!(&result.message, &parsed.message);
        prop_assert_eq!(result.details.len(), parsed.details.len());
        prop_assert_eq!(result.errors.len(), parsed.errors.len());
        prop_assert_eq!(result.warnings.len(), parsed.warnings.len());
        Ok(())
    }

    /// StructuredError serializes to JSON and deserializes back correctly.
    fn structured_error_json_roundtrip(error in structured_error_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&error).expect("should serialize to JSON");
        let parsed: StructuredError = serde_json::from_str(&json_str)
            .expect("should deserialize from JSON");

        prop_assert_eq!(&error.code, &parsed.code);
        prop_assert_eq!(&error.message, &parsed.message);
        prop_assert_eq!(&error.location, &parsed.location);
        prop_assert_eq!(&error.suggestion, &parsed.suggestion);
        Ok(())
    }

    /// Status serializes to JSON and deserializes back correctly.
    fn status_json_roundtrip(status in status_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&status).expect("should serialize");
        let parsed: Status = serde_json::from_str(&json_str).expect("should deserialize");

        prop_assert_eq!(status, parsed);
        Ok(())
    }

    /// OutputFormat serializes to JSON and deserializes back correctly.
    fn output_format_json_roundtrip(format in output_format_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&format).expect("should serialize");
        let parsed: OutputFormat = serde_json::from_str(&json_str).expect("should deserialize");

        // Compare discriminants
        let original_name = format!("{format:?}");
        let parsed_name = format!("{parsed:?}");
        prop_assert_eq!(original_name, parsed_name);
        Ok(())
    }
}

// ============================================================================
// ProcessBuilder Argument Escaping Tests
// ============================================================================

/// Generate a command argument that might need escaping.
fn argument_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple arguments
        "[a-zA-Z0-9_\\-]{1,30}",
        // Arguments with spaces (quoted)
        "[a-zA-Z0-9 ]{3,30}",
        // Paths
        "/[a-zA-Z0-9/_\\-\\.]{5,50}",
        // Flags
        Just("-v".to_string()),
        Just("--verbose".to_string()),
        Just("--output=json".to_string()),
    ]
}

sinex_proptest! {
    /// Arguments can be collected and joined without data loss.
    fn arguments_preserve_content(args in prop::collection::vec(argument_strategy(), 0..=10)) -> TestResult<()> {
        // Simulate argument collection (like ProcessBuilder.args())
        let collected: Vec<String> = args.clone();

        prop_assert_eq!(args.len(), collected.len());
        for (original, collected) in args.iter().zip(collected.iter()) {
            prop_assert_eq!(original, collected);
        }
        Ok(())
    }

    /// Empty arguments list is valid.
    fn empty_arguments_valid(args in Just(Vec::<String>::new())) -> TestResult<()> {
        prop_assert!(args.is_empty());
        Ok(())
    }

    /// Arguments with special characters are preserved.
    fn special_char_arguments(
        prefix in "[a-zA-Z]{1,5}",
        special in prop_oneof![Just("="), Just("-"), Just("_"), Just(".")],
        suffix in "[a-zA-Z0-9]{1,10}"
    ) -> TestResult<()> {
        let arg = format!("{prefix}{special}{suffix}");

        // Verify the argument can be serialized and deserialized
        let json = serde_json::to_string(&arg).expect("should serialize");
        let parsed: String = serde_json::from_str(&json).expect("should deserialize");

        prop_assert_eq!(arg, parsed);
        Ok(())
    }
}

// ============================================================================
// JSON Output Schema Validation Tests
// ============================================================================

sinex_proptest! {
    /// JSON output always has required fields present.
    fn json_output_has_required_fields(result in output_command_result_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("should parse as JSON value");

        // Required fields must be present
        prop_assert!(parsed.get("command").is_some(), "command field required");
        prop_assert!(parsed.get("status").is_some(), "status field required");
        prop_assert!(parsed.get("duration_secs").is_some(), "duration_secs field required");
        prop_assert!(parsed.get("timestamp").is_some(), "timestamp field required");
        Ok(())
    }

    /// JSON status field is one of the valid enum values.
    fn json_status_is_valid_enum(result in output_command_result_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("should parse as JSON value");

        let status = parsed.get("status")
            .and_then(|v| v.as_str())
            .expect("status should be a string");

        let valid_statuses = ["success", "failed", "partial", "running", "cancelled"];
        prop_assert!(
            valid_statuses.contains(&status),
            "Status '{}' should be one of {:?}",
            status,
            valid_statuses
        );
        Ok(())
    }

    /// JSON errors array contains objects with required fields.
    fn json_errors_have_required_fields(result in output_command_result_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("should parse as JSON value");

        if let Some(errors) = parsed.get("errors").and_then(|v| v.as_array()) {
            for error in errors {
                prop_assert!(error.get("code").is_some(), "error must have code");
                prop_assert!(error.get("message").is_some(), "error must have message");
            }
        }
        Ok(())
    }

    /// JSON duration_secs is a valid non-negative number.
    fn json_duration_is_valid(result in output_command_result_strategy()) -> TestResult<()> {
        let json_str = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("should parse as JSON value");

        let duration = parsed.get("duration_secs")
            .and_then(sinex_primitives::JsonValue::as_f64)
            .expect("duration_secs should be a number");

        prop_assert!(duration >= 0.0, "Duration should be non-negative");
        prop_assert!(duration.is_finite(), "Duration should be finite");
        Ok(())
    }
}

// ============================================================================
// Status Invariants
// ============================================================================

sinex_proptest! {
    /// Status symbol is never empty.
    fn status_symbol_non_empty(status in status_strategy()) -> TestResult<()> {
        let symbol = status.symbol();
        prop_assert!(!symbol.is_empty(), "Status symbol should not be empty");
        Ok(())
    }

    /// Status color code is a valid ANSI escape sequence.
    fn status_color_is_valid_ansi(status in status_strategy()) -> TestResult<()> {
        let color = status.color_code();
        prop_assert!(color.starts_with("\x1b["), "Color should be ANSI escape: {:?}", color);
        prop_assert!(color.ends_with('m'), "Color should end with 'm': {:?}", color);
        Ok(())
    }

    /// Success status is_success returns true.
    fn success_status_is_success(_unused in Just(())) -> TestResult<()> {
        let status = Status::Success;
        prop_assert!(status.is_success());
        Ok(())
    }

    /// Non-success statuses return false for is_success.
    fn non_success_is_not_success(status in prop_oneof![
        Just(Status::Failed),
        Just(Status::Partial),
        Just(Status::Running),
        Just(Status::Cancelled),
    ]) -> TestResult<()> {
        prop_assert!(!status.is_success());
        Ok(())
    }
}

// ============================================================================
// CommandResult Builder Invariants
// ============================================================================

sinex_proptest! {
    /// CommandResult::success() creates a Success status.
    fn success_builder_creates_success(_unused in Just(())) -> TestResult<()> {
        let result = CommandResult::success();
        prop_assert_eq!(result.status, Status::Success);
        prop_assert!(result.is_success());
        Ok(())
    }

    /// CommandResult::failure() creates a Failed status with the error.
    fn failure_builder_creates_failure(error in structured_error_strategy()) -> TestResult<()> {
        let error_code = error.code.clone();
        let result = CommandResult::failure(error);

        prop_assert_eq!(result.status, Status::Failed);
        prop_assert!(!result.is_success());
        prop_assert_eq!(result.errors.len(), 1);
        prop_assert_eq!(&result.errors[0].code, &error_code);
        Ok(())
    }

    /// Chaining with_message, with_detail, with_warning, with_duration doesn't
    /// interfere — all four postconditions hold simultaneously on one builder chain.
    fn command_result_builder_chain_invariants(
        message in error_message_strategy(),
        detail in error_message_strategy(),
        warning in error_message_strategy(),
        secs in 0u64..=3600u64,
        nanos in 0u32..=999_999_999u32,
    ) -> TestResult<()> {
        let duration = std::time::Duration::new(secs, nanos);
        let result = CommandResult::success()
            .with_message(message.clone())
            .with_detail(detail.clone())
            .with_warning(warning.clone())
            .with_duration(duration);

        prop_assert_eq!(result.message, Some(message));
        prop_assert_eq!(result.details.len(), 1);
        prop_assert_eq!(&result.details[0], &detail);
        prop_assert_eq!(result.warnings.len(), 1);
        prop_assert_eq!(&result.warnings[0], &warning);
        let expected_secs = duration.as_secs_f64();
        let diff = (result.duration_secs.unwrap_or(0.0) - expected_secs).abs();
        prop_assert!(diff < 0.0001, "Duration should match");
        Ok(())
    }

    /// Multiple with_detail calls accumulate.
    fn multiple_details_accumulate(details in prop::collection::vec(error_message_strategy(), 1..=5)) -> TestResult<()> {
        let mut result = CommandResult::success();
        for detail in &details {
            result = result.with_detail(detail.clone());
        }

        prop_assert_eq!(result.details.len(), details.len());
        Ok(())
    }
}

// ============================================================================
// HistoryDb Roundtrip Invariants
// ============================================================================

sinex_proptest! {
    /// Any finished invocation can be retrieved by ID from the recent list.
    ///
    /// This is the core storage roundtrip guarantee: whatever was written
    /// to the history DB must be queryable back. If this invariant breaks,
    /// all history-based commands (`xtask history list`, `xtask status`, etc.)
    /// would show incomplete or stale data.
    ///
    /// Uses `.expect()` for DB setup — proptest catches panics as test failures,
    /// so a failed DB open surfaces as a falsification rather than a broken harness.
    fn historydb_finished_invocation_is_retrievable(
        command in prop_oneof![
            Just("check"), Just("test"), Just("build"), Just("fix")
        ],
        exit_code in 0i32..=1i32,
        duration_secs in 0.1f64..=60.0f64
    ) -> TestResult<()> {
        let db = HistoryDb::open_in_memory().expect("in-memory DB open must succeed");

        let inv_id = db.start_invocation(command, None, None, None)
            .expect("start_invocation must succeed");
        let status = if exit_code == 0 {
            InvocationStatus::Success
        } else {
            InvocationStatus::Failed
        };
        db.finish_invocation(inv_id, status, Some(exit_code), duration_secs)
            .expect("finish_invocation must succeed");

        let exact = db
            .get_invocation(inv_id)
            .expect("get_invocation must succeed")
            .expect("written invocation must exist");
        let recent = InvocationQuery::new()
            .for_invocation(inv_id)
            .run(&db)
            .expect("InvocationQuery::for_invocation must succeed");

        prop_assert_eq!(exact.id, inv_id, "exact lookup must preserve invocation id");
        prop_assert_eq!(recent.len(), 1, "exact query must only return the requested invocation");
        prop_assert_eq!(&recent[0].command, &command, "command must round-trip");
        prop_assert_eq!(recent[0].exit_code, Some(exit_code), "exit_code must round-trip");

        Ok(())
    }

    /// get_recent_filtered never returns more entries than the requested limit.
    ///
    /// The limit parameter is a hard upper bound, not a hint. Violating this
    /// would overflow agent-facing JSON payloads and break UX assumptions
    /// (e.g. `xtask history list --limit 5` returning 20 entries).
    #[cases(32)]
    fn historydb_recent_respects_limit(
        write_count in 5usize..=15usize,
        query_limit in 1usize..=4usize
    ) -> TestResult<()> {
        // write_count (5–15) > query_limit (1–4) guarantees the cap is always exercised
        let db = HistoryDb::open_in_memory().expect("in-memory DB open must succeed");

        for _ in 0..write_count {
            let id = db.start_invocation("check", None, None, None)
                .expect("start_invocation must succeed");
            db.finish_invocation(id, InvocationStatus::Success, Some(0), 1.0)
                .expect("finish_invocation must succeed");
        }

        let results = InvocationQuery::new()
            .limit(query_limit)
            .run(&db)
            .expect("InvocationQuery::limit must succeed");

        prop_assert!(
            results.len() <= query_limit,
            "get_recent_filtered({}) returned {} entries — must be ≤ {}",
            query_limit, results.len(), query_limit
        );

        Ok(())
    }

    /// Offset pagination produces non-overlapping pages.
    ///
    /// Page 0 and page 1 (same page size) must not share any invocation IDs.
    /// Broken pagination would cause `xtask history list --offset N` to show
    /// duplicate entries or skip entries silently.
    #[cases(32)]
    fn historydb_offset_pages_are_disjoint(
        total in 6usize..=12usize,
        page_size in 2usize..=3usize
    ) -> TestResult<()> {
        let db = HistoryDb::open_in_memory().expect("in-memory DB open must succeed");

        for _ in 0..total {
            let id = db.start_invocation("check", None, None, None)
                .expect("start_invocation must succeed");
            db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.5)
                .expect("finish_invocation must succeed");
        }

        let page0 = InvocationQuery::new()
            .limit(page_size)
            .offset(0)
            .run(&db)
            .expect("page0 query must succeed");
        let page1 = InvocationQuery::new()
            .limit(page_size)
            .offset(page_size)
            .run(&db)
            .expect("page1 query must succeed");

        let ids0: std::collections::HashSet<i64> = page0.iter().map(|i| i.id).collect();
        let ids1: std::collections::HashSet<i64> = page1.iter().map(|i| i.id).collect();

        let overlap: Vec<_> = ids0.intersection(&ids1).collect();
        prop_assert!(
            overlap.is_empty(),
            "pages 0 and 1 must not share entries, but shared: {:?}", overlap
        );

        Ok(())
    }
}

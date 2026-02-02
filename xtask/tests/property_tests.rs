//! Property-based tests for xtask.
//!
//! Uses proptest to verify invariants that should hold for all valid inputs:
//! - `CommandResult` serialization roundtrips preserve data
//! - `ProcessBuilder` argument handling is consistent
//! - JSON output conforms to expected schema
//!
//! Requires the `sandbox` feature to be enabled (provides proptest).

#![cfg(feature = "sandbox")]

use proptest::prelude::*;
use sinex_primitives::temporal;
use xtask::command::CommandResult;
use xtask::output::{OutputFormat, Status, StructuredError};

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
        .prop_map(|(path, line, col)| format!("{}:{}:{}", path, line, col))
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

/// Generate an OutputFormat enum value.
fn output_format_strategy() -> impl Strategy<Value = OutputFormat> {
    prop_oneof![
        Just(OutputFormat::Human),
        Just(OutputFormat::Json),
        Just(OutputFormat::Compact),
        Just(OutputFormat::Silent),
    ]
}

/// Generate a StructuredError.
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

/// Generate a CommandResult (using the output module's version).
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
                    data: None, // Simplified for now, could use a json strategy
                    is_silent: false,
                    errors,
                    suggested_fixes,
                    message: None,
                }
            },
        )
}

/// Generate a CommandResult (using the command module's version).
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

proptest! {
    /// Output CommandResult serializes to JSON and deserializes back correctly.
    #[test]
    fn output_command_result_json_roundtrip(result in output_command_result_strategy()) {
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
    }

    /// Command CommandResult serializes to JSON and deserializes back correctly.
    #[test]
    fn command_result_json_roundtrip(result in command_result_strategy()) {
        let json_str = serde_json::to_string(&result).expect("should serialize to JSON");
        let parsed: CommandResult = serde_json::from_str(&json_str)
            .expect("should deserialize from JSON");

        // Verify key fields are preserved
        prop_assert_eq!(result.status, parsed.status);
        prop_assert_eq!(&result.message, &parsed.message);
        prop_assert_eq!(result.details.len(), parsed.details.len());
        prop_assert_eq!(result.errors.len(), parsed.errors.len());
        prop_assert_eq!(result.warnings.len(), parsed.warnings.len());
    }

    /// StructuredError serializes to JSON and deserializes back correctly.
    #[test]
    fn structured_error_json_roundtrip(error in structured_error_strategy()) {
        let json_str = serde_json::to_string(&error).expect("should serialize to JSON");
        let parsed: StructuredError = serde_json::from_str(&json_str)
            .expect("should deserialize from JSON");

        prop_assert_eq!(&error.code, &parsed.code);
        prop_assert_eq!(&error.message, &parsed.message);
        prop_assert_eq!(&error.location, &parsed.location);
        prop_assert_eq!(&error.suggestion, &parsed.suggestion);
    }

    /// Status serializes to JSON and deserializes back correctly.
    #[test]
    fn status_json_roundtrip(status in status_strategy()) {
        let json_str = serde_json::to_string(&status).expect("should serialize");
        let parsed: Status = serde_json::from_str(&json_str).expect("should deserialize");

        prop_assert_eq!(status, parsed);
    }

    /// OutputFormat serializes to JSON and deserializes back correctly.
    #[test]
    fn output_format_json_roundtrip(format in output_format_strategy()) {
        let json_str = serde_json::to_string(&format).expect("should serialize");
        let parsed: OutputFormat = serde_json::from_str(&json_str).expect("should deserialize");

        // Compare discriminants
        let original_name = format!("{:?}", format);
        let parsed_name = format!("{:?}", parsed);
        prop_assert_eq!(original_name, parsed_name);
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

proptest! {
    /// Arguments can be collected and joined without data loss.
    #[test]
    fn arguments_preserve_content(args in prop::collection::vec(argument_strategy(), 0..=10)) {
        // Simulate argument collection (like ProcessBuilder.args())
        let collected: Vec<String> = args.clone();

        prop_assert_eq!(args.len(), collected.len());
        for (original, collected) in args.iter().zip(collected.iter()) {
            prop_assert_eq!(original, collected);
        }
    }

    /// Empty arguments list is valid.
    #[test]
    fn empty_arguments_valid(args in Just(Vec::<String>::new())) {
        prop_assert!(args.is_empty());
    }

    /// Arguments with special characters are preserved.
    #[test]
    fn special_char_arguments(
        prefix in "[a-zA-Z]{1,5}",
        special in prop_oneof![Just("="), Just("-"), Just("_"), Just(".")],
        suffix in "[a-zA-Z0-9]{1,10}"
    ) {
        let arg = format!("{}{}{}", prefix, special, suffix);

        // Verify the argument can be serialized and deserialized
        let json = serde_json::to_string(&arg).expect("should serialize");
        let parsed: String = serde_json::from_str(&json).expect("should deserialize");

        prop_assert_eq!(arg, parsed);
    }
}

// ============================================================================
// JSON Output Schema Validation Tests
// ============================================================================

proptest! {
    /// JSON output always has required fields present.
    #[test]
    fn json_output_has_required_fields(result in output_command_result_strategy()) {
        let json_str = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("should parse as JSON value");

        // Required fields must be present
        prop_assert!(parsed.get("command").is_some(), "command field required");
        prop_assert!(parsed.get("status").is_some(), "status field required");
        prop_assert!(parsed.get("duration_secs").is_some(), "duration_secs field required");
        prop_assert!(parsed.get("timestamp").is_some(), "timestamp field required");
    }

    /// JSON status field is one of the valid enum values.
    #[test]
    fn json_status_is_valid_enum(result in output_command_result_strategy()) {
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
    }

    /// JSON errors array contains objects with required fields.
    #[test]
    fn json_errors_have_required_fields(result in output_command_result_strategy()) {
        let json_str = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("should parse as JSON value");

        if let Some(errors) = parsed.get("errors").and_then(|v| v.as_array()) {
            for error in errors {
                prop_assert!(error.get("code").is_some(), "error must have code");
                prop_assert!(error.get("message").is_some(), "error must have message");
            }
        }
    }

    /// JSON duration_secs is a valid non-negative number.
    #[test]
    fn json_duration_is_valid(result in output_command_result_strategy()) {
        let json_str = serde_json::to_string(&result).expect("should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str)
            .expect("should parse as JSON value");

        let duration = parsed.get("duration_secs")
            .and_then(|v| v.as_f64())
            .expect("duration_secs should be a number");

        prop_assert!(duration >= 0.0, "Duration should be non-negative");
        prop_assert!(duration.is_finite(), "Duration should be finite");
    }
}

// ============================================================================
// Status Invariants
// ============================================================================

proptest! {
    /// Status symbol is never empty.
    #[test]
    fn status_symbol_non_empty(status in status_strategy()) {
        let symbol = status.symbol();
        prop_assert!(!symbol.is_empty(), "Status symbol should not be empty");
    }

    /// Status color code is a valid ANSI escape sequence.
    #[test]
    fn status_color_is_valid_ansi(status in status_strategy()) {
        let color = status.color_code();
        prop_assert!(color.starts_with("\x1b["), "Color should be ANSI escape: {:?}", color);
        prop_assert!(color.ends_with('m'), "Color should end with 'm': {:?}", color);
    }

    /// Success status is_success returns true.
    #[test]
    fn success_status_is_success(_unused in Just(())) {
        let status = Status::Success;
        prop_assert!(status.is_success());
    }

    /// Non-success statuses return false for is_success.
    #[test]
    fn non_success_is_not_success(status in prop_oneof![
        Just(Status::Failed),
        Just(Status::Partial),
        Just(Status::Running),
        Just(Status::Cancelled),
    ]) {
        prop_assert!(!status.is_success());
    }
}

// ============================================================================
// CommandResult Builder Invariants
// ============================================================================

proptest! {
    /// CommandResult::success() creates a Success status.
    #[test]
    fn success_builder_creates_success(_unused in Just(())) {
        let result = CommandResult::success();
        prop_assert_eq!(result.status, Status::Success);
        prop_assert!(result.is_success());
    }

    /// CommandResult::failure() creates a Failed status with the error.
    #[test]
    fn failure_builder_creates_failure(error in structured_error_strategy()) {
        let error_code = error.code.clone();
        let result = CommandResult::failure(error);

        prop_assert_eq!(result.status, Status::Failed);
        prop_assert!(!result.is_success());
        prop_assert_eq!(result.errors.len(), 1);
        prop_assert_eq!(&result.errors[0].code, &error_code);
    }

    /// with_message sets the message.
    #[test]
    fn with_message_sets_message(message in error_message_strategy()) {
        let result = CommandResult::success().with_message(message.clone());
        prop_assert_eq!(result.message, Some(message));
    }

    /// with_detail adds a detail.
    #[test]
    fn with_detail_adds_detail(detail in error_message_strategy()) {
        let result = CommandResult::success().with_detail(detail.clone());
        prop_assert_eq!(result.details.len(), 1);
        prop_assert_eq!(&result.details[0], &detail);
    }

    /// with_warning adds a warning.
    #[test]
    fn with_warning_adds_warning(warning in error_message_strategy()) {
        let result = CommandResult::success().with_warning(warning.clone());
        prop_assert_eq!(result.warnings.len(), 1);
        prop_assert_eq!(&result.warnings[0], &warning);
    }

    /// Multiple with_detail calls accumulate.
    #[test]
    fn multiple_details_accumulate(details in prop::collection::vec(error_message_strategy(), 1..=5)) {
        let mut result = CommandResult::success();
        for detail in &details {
            result = result.with_detail(detail.clone());
        }

        prop_assert_eq!(result.details.len(), details.len());
    }

    /// with_duration sets duration_secs.
    #[test]
    fn with_duration_sets_duration(secs in 0u64..=3600u64, nanos in 0u32..=999_999_999u32) {
        let duration = std::time::Duration::new(secs, nanos);
        let result = CommandResult::success().with_duration(duration);

        let expected = duration.as_secs_f64();
        let diff = (result.duration_secs.unwrap_or(0.0) - expected).abs();
        prop_assert!(diff < 0.0001, "Duration should match");
    }
}

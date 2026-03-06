#![allow(clippy::expect_used)] // Test utilities use expect() on constant values and infallible serialization
//! Testing utilities for domain primitives.
//!
//! Provides event fixtures and property testing strategies for domain types.
//!
//! # Usage Patterns
//!
//! For **pure logic tests** (no DB):
//! ```rust,ignore
//! // Simplest: when source/type don't matter
//! let event = event_stub(json!({"value": 42}));
//!
//! // Typed payloads: elegant one-liner
//! use sinex_primitives::testing::TestablePayload;
//! let event = FileCreatedPayload { ... }.into_test_event();
//!
//! // Full control: specify source/type
//! let event = event_fixture(
//!     EventSource::from_static("fs-watcher"),
//!     EventType::from_static("file.created"),
//!     json!({...}),
//! );
//! ```
//!
//! For **DB tests**: Use `ctx.pipeline().publish()` or `ctx.pipeline().publish_with_timestamp()`.

use crate::events::Publishable;

/// Create a minimal event stub for testing when source/type don't matter.
///
/// Uses `"test"` as source and `"test.stub"` as event type.
///
/// **WARNING**: Do NOT insert into database. Use for pure logic tests only.
///
/// # Example
/// ```rust,ignore
/// use sinex_primitives::testing::event_stub;
///
/// // When you just need *some* event
/// let event = event_stub(json!({"value": 42}));
/// assert!(event.payload["value"] == 42);
/// ```
#[must_use]
pub fn event_stub(payload: crate::JsonValue) -> crate::Event<crate::JsonValue> {
    event_fixture(
        crate::EventSource::from_static("test"),
        crate::EventType::from_static("test.stub"),
        payload,
    )
}

/// Extension trait for creating test events from typed payloads.
///
/// # Example
/// ```rust,ignore
/// use sinex_primitives::testing::TestablePayload;
/// use my_crate::FileCreatedPayload;
///
/// let payload = FileCreatedPayload { path: "/test".into(), size: 1024 };
/// let event = payload.into_test_event();  // Source/type inferred from payload
/// ```
pub trait TestablePayload: Publishable + Sized {
    /// Convert this payload into a test event.
    ///
    /// **WARNING**: Do NOT insert into database. Use for pure logic tests only.
    fn into_test_event(self) -> crate::Event<crate::JsonValue> {
        let payload_json = self
            .to_json_value()
            .expect("TestablePayload serialization should not fail");
        event_fixture(self.source(), self.event_type(), payload_json)
    }
}

// Blanket impl for all Publishable types
impl<T: Publishable + Sized> TestablePayload for T {}

/// Create an event fixture for testing (in-memory only).
///
/// **WARNING**: Do NOT insert events created with this function into the database.
/// The random material ID will fail FK constraints. For DB tests, use
/// `Sandbox::publish()` instead.
///
/// # Example
/// ```rust,ignore
/// use sinex_primitives::testing::event_fixture;
/// use sinex_primitives::{EventSource, EventType};
/// use serde_json::json;
///
/// let event = event_fixture(
///     EventSource::from_static("fs-watcher"),
///     EventType::from_static("file.created"),
///     json!({ "path": "/test/file.txt", "size": 1024 }),
/// );
/// ```
pub fn event_fixture(
    source: crate::EventSource,
    event_type: crate::EventType,
    payload: crate::JsonValue,
) -> crate::Event<crate::JsonValue> {
    use crate::events::SourceMaterial;
    use crate::{Event, HostName, Id, OffsetKind, Provenance, Timestamp, Uuid};

    let material_id = Uuid::now_v7();

    Event {
        id: None,
        source,
        event_type,
        payload,
        ts_orig: Some(Timestamp::now()),
        host: HostName::new(gethostname::gethostname().to_string_lossy().to_string()),
        node_version: Some("test".to_string()),
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::from_uuid(material_id),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
    }
}

#[cfg(feature = "proptest")]
pub mod strategies {
    //! Property testing strategies for domain types.

    use crate::domain::{CommandText, HostName, RecordedPath, SanitizedPath, ShellName};
    use crate::events::payloads::{
        FileCreatedPayload, HyprlandWindowFocusedPayload, KittyCommandExecutedPayload,
        ProcessHeartbeatPayload,
    };
    use crate::testing::TestablePayload;
    use crate::units::{ExitCode, Nanoseconds, SequenceNumber};
    use crate::{EventSource, EventType, Timestamp, Uuid};
    use proptest::prelude::*;

    /// Generate random event sources (regex guarantees validity).
    pub fn event_source() -> impl Strategy<Value = EventSource> {
        "[a-z][a-z0-9-]{0,30}"
            .prop_map(|s| EventSource::new(s).expect("regex-generated source is always valid"))
    }

    /// Generate random event types (regex guarantees validity).
    pub fn event_type() -> impl Strategy<Value = EventType> {
        "[a-z][a-z0-9.]{0,30}"
            .prop_filter(
                "must not start/end with dot or have consecutive dots",
                |s| !s.starts_with('.') && !s.ends_with('.') && !s.contains(".."),
            )
            .prop_map(|s| EventType::new(s).expect("filtered regex source is always valid"))
    }

    /// Generate random UUIDv7 IDs.
    pub fn uuid_strategy() -> impl Strategy<Value = Uuid> {
        any::<u128>().prop_map(|bits| Uuid::from_bytes(bits.to_be_bytes()))
    }

    // ─────────────────────────────────────────────────────────────
    // Supporting Type Strategies
    // ─────────────────────────────────────────────────────────────

    /// Generate valid sanitized paths for testing.
    pub fn sanitized_path() -> impl Strategy<Value = SanitizedPath> {
        r"/[a-z]{1,8}(/[a-z0-9._-]{1,12}){0,4}".prop_map(|s| {
            SanitizedPath::from_str_validated(&s)
                .unwrap_or_else(|_| SanitizedPath::from_static("/tmp/test"))
        })
    }

    /// Generate random timestamps within a recent window (last 24 hours).
    pub fn timestamp() -> impl Strategy<Value = Timestamp> {
        (0i64..86400).prop_map(|secs_ago| {
            Timestamp::from_unix_timestamp(Timestamp::now().unix_timestamp() - secs_ago)
                .unwrap_or_else(|| Timestamp::now())
        })
    }

    /// Generate random HostName values.
    pub fn hostname() -> impl Strategy<Value = HostName> {
        "[a-z][a-z0-9-]{2,15}".prop_map(HostName::new)
    }

    /// Generate random CommandText values.
    pub fn command_text() -> impl Strategy<Value = CommandText> {
        r"[a-z]{1,8}( [a-z0-9/_.-]{1,20}){0,5}".prop_map(|s| CommandText::new(s))
    }

    /// Generate random ShellName values.
    pub fn shell_name() -> impl Strategy<Value = ShellName> {
        r"(bash|zsh|fish|sh)".prop_map(ShellName::new)
    }

    /// Generate random ExitCode values (including success and failure codes).
    pub fn exit_code() -> impl Strategy<Value = ExitCode> {
        prop_oneof![
            Just(ExitCode::SUCCESS),
            (1i32..=255i32).prop_map(ExitCode::from_raw),
        ]
    }

    /// Generate random SequenceNumber values.
    pub fn sequence_number() -> impl Strategy<Value = SequenceNumber> {
        any::<u64>().prop_map(SequenceNumber::from_raw)
    }

    /// Generate random Nanoseconds values (0-1 hour in nanoseconds).
    pub fn nanoseconds() -> impl Strategy<Value = Nanoseconds> {
        (0i64..3_600_000_000_000i64).prop_map(Nanoseconds::from_nanos)
    }

    // ─────────────────────────────────────────────────────────────
    // High-Traffic Payload Strategies
    // ─────────────────────────────────────────────────────────────

    /// Generate random FileCreatedPayload values.
    pub fn file_created_payload() -> impl Strategy<Value = FileCreatedPayload> {
        (
            sanitized_path(),
            0u64..10_000_000u64,
            timestamp(),
            proptest::option::of(0u32..0o777),
        )
            .prop_map(|(path, size, created_at, permissions)| FileCreatedPayload {
                #[allow(clippy::expect_used)] // Generated path is always valid ASCII
                path: RecordedPath::from_observed(path.as_str())
                    .expect("generated test path should not contain null bytes"),
                size,
                created_at,
                permissions,
            })
    }

    /// Generate random KittyCommandExecutedPayload values.
    pub fn kitty_command_executed_payload() -> impl Strategy<Value = KittyCommandExecutedPayload> {
        (
            command_text(),
            proptest::option::of(sanitized_path()),
            proptest::option::of(exit_code()),
            proptest::option::of(0u64..600_000u64),
            proptest::option::of(shell_name()),
            "[0-9]{1,6}",
            "[0-9]{1,6}",
        )
            .prop_map(
                |(
                    command,
                    working_directory,
                    exit_status,
                    execution_time_ms,
                    shell_type,
                    window_id,
                    tab_id,
                )| {
                    KittyCommandExecutedPayload {
                        command,
                        working_directory: working_directory.map(|p| {
                            #[allow(clippy::expect_used)] // Generated path is valid ASCII
                            RecordedPath::from_observed(p.as_str())
                                .expect("generated test path should not contain null bytes")
                        }),
                        exit_status,
                        execution_time_ms,
                        shell_type,
                        kitty_window_id: window_id,
                        kitty_tab_id: tab_id,
                    }
                },
            )
    }

    /// Generate random HyprlandWindowFocusedPayload values.
    pub fn window_focused_payload() -> impl Strategy<Value = HyprlandWindowFocusedPayload> {
        (
            "[0-9a-f]{8,16}",
            "[a-zA-Z][a-zA-Z0-9._-]{2,30}",
            "[A-Z][a-zA-Z0-9 ._-]{2,50}",
            0i32..20i32,
            proptest::option::of("[0-9a-f]{8,16}"),
        )
            .prop_map(
                |(window_id, window_class, window_title, workspace_id, previous_window_id)| {
                    HyprlandWindowFocusedPayload {
                        window_id,
                        window_class,
                        window_title,
                        workspace_id,
                        previous_window_id,
                    }
                },
            )
    }

    /// Generate random ProcessHeartbeatPayload values.
    pub fn process_heartbeat_payload() -> impl Strategy<Value = ProcessHeartbeatPayload> {
        use crate::events::payloads::ProcessStatus;

        (
            "[a-z][a-z0-9-]{2,20}",
            sequence_number(),
            prop_oneof![
                Just(ProcessStatus::Healthy),
                Just(ProcessStatus::Degraded),
                Just(ProcessStatus::Failed),
            ],
        )
            .prop_map(|(source, sequence, status)| ProcessHeartbeatPayload {
                source,
                sequence,
                status,
                metrics: None,
            })
    }

    /// Generate a complete Event<JsonValue> with random FileCreatedPayload for property testing.
    ///
    /// WARNING: Do NOT insert into database — no valid provenance.
    pub fn file_created_event() -> impl Strategy<Value = crate::Event<crate::JsonValue>> {
        file_created_payload().prop_map(|payload| payload.into_test_event())
    }

    /// Generate a complete Event<JsonValue> with random KittyCommandExecutedPayload for property testing.
    ///
    /// WARNING: Do NOT insert into database — no valid provenance.
    pub fn kitty_command_executed_event() -> impl Strategy<Value = crate::Event<crate::JsonValue>> {
        kitty_command_executed_payload().prop_map(|payload| payload.into_test_event())
    }

    /// Generate a complete Event<JsonValue> with random HyprlandWindowFocusedPayload for property testing.
    ///
    /// WARNING: Do NOT insert into database — no valid provenance.
    pub fn window_focused_event() -> impl Strategy<Value = crate::Event<crate::JsonValue>> {
        window_focused_payload().prop_map(|payload| payload.into_test_event())
    }

    /// Generate a complete Event<JsonValue> with random ProcessHeartbeatPayload for property testing.
    ///
    /// WARNING: Do NOT insert into database — no valid provenance.
    pub fn process_heartbeat_event() -> impl Strategy<Value = crate::Event<crate::JsonValue>> {
        process_heartbeat_payload().prop_map(|payload| payload.into_test_event())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use xtask::sandbox::sinex_proptest;

        sinex_proptest! {
            fn test_sanitized_path_strategy(path in sanitized_path()) {
                prop_assert!(path.as_str().starts_with('/'), "path should start with /");
                prop_assert!(!path.as_str().is_empty(), "path should not be empty");
                Ok(())
            }

            fn test_timestamp_strategy(ts in timestamp()) {
                let now = Timestamp::now();
                prop_assert!(ts.unix_timestamp() <= now.unix_timestamp(), "timestamp should be in the past");
                Ok(())
            }

            fn test_file_created_payload_strategy(payload in file_created_payload()) {
                prop_assert!(!payload.path.as_str().is_empty(), "path should not be empty");
                Ok(())
            }

            fn test_file_created_event_strategy(event in file_created_event()) {
                prop_assert!(!event.source.as_str().is_empty(), "source should not be empty");
                prop_assert!(!event.event_type.as_str().is_empty(), "event_type should not be empty");
                Ok(())
            }
        }
    }
}

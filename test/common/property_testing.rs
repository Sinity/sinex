// Property Testing Integration - Harmonized with TestContext
//
// Provides property-based testing capabilities that integrate seamlessly
// with the unified test infrastructure and event builders.

use crate::common::prelude::*;
use proptest::prelude::*;
use proptest::strategy::{Strategy, BoxedStrategy};

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
        ].boxed()
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
        ].boxed()
    }
    
    /// Strategy for valid file paths
    pub fn file_path() -> BoxedStrategy<String> {
        prop_oneof![
            Just("/tmp/test.txt".to_string()),
            Just("/home/user/document.pdf".to_string()),
            Just("/var/log/system.log".to_string()),
            "/[a-z0-9/._-]{1,100}\\.[a-z]{1,5}".prop_map(|s| s.to_string()),
        ].boxed()
    }
    
    /// Strategy for valid shell commands
    pub fn shell_command() -> BoxedStrategy<String> {
        prop_oneof![
            Just("ls -la".to_string()),
            Just("git status".to_string()),
            Just("cargo build".to_string()),
            Just("cd /home".to_string()),
            "[a-z]{2,10}( [a-z0-9-]{1,20})*".prop_map(|s| s.to_string()),
        ].boxed()
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
            |inner| prop_oneof![
                prop::collection::vec(inner.clone(), 0..10)
                    .prop_map(Value::from),
                prop::collection::hash_map(".*", inner, 0..10)
                    .prop_map(|map| {
                        Value::from(
                            map.into_iter()
                                .collect::<serde_json::Map<_, _>>()
                        )
                    }),
            ]
        ).boxed()
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
            Self::file_path().prop_map(|path| json!({
                "path": path,
                "size": any::<u64>(),
                "modified_time": "2025-01-01T00:00:00Z"
            }))
        ).boxed()
    }
    
    /// Strategy for terminal events
    pub fn terminal_event() -> BoxedStrategy<(String, String, Value)> {
        (
            Just("shell.kitty".to_string()),
            Just("command.executed".to_string()),
            (Self::shell_command(), 0u32..2u32, 0u64..5000u64).prop_map(|(cmd, exit_code, duration)| {
                json!({
                    "command": cmd,
                    "exit_code": exit_code,
                    "duration_ms": duration
                })
            })
        ).boxed()
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
            ("[a-z-]{5,20}", "[0-9]\\.[0-9]\\.[0-9]").prop_map(|(name, version)| {
                json!({
                    "agent_name": name,
                    "status": "running",
                    "version": version,
                    "uptime_seconds": any::<u64>(),
                })
            })
        ).boxed()
    }
    
    /// Strategy for any valid event
    pub fn any_event() -> BoxedStrategy<(String, String, Value)> {
        prop_oneof![
            Self::filesystem_event(),
            Self::terminal_event(),
            Self::agent_event(),
        ].boxed()
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
        ].boxed()
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
    pub async fn test_property<S, T, F, Fut>(
        &mut self,
        strategy: S,
        test_cases: u32,
        property: F,
    ) -> TestResult
    where
        S: Strategy<Value = T>,
        F: Fn(&TestContext, T) -> Fut,
        Fut: std::future::Future<Output = TestResult>,
    {
        for case_num in 0..test_cases {
            let value = strategy.new_tree(&mut self.runner)?.current();
            
            property(self.ctx, value).await
                .with_context(|| format!("Property test case {} failed", case_num))?;
        }
        
        Ok(())
    }
    
    /// Test event creation property
    pub async fn test_event_creation_property(&mut self, test_cases: u32) -> TestResult {
        self.test_property(
            SinexStrategies::any_event(),
            test_cases,
            |ctx, (source, event_type, payload)| async move {
                // Property: All valid events should be creatable and insertable
                let event = ctx.event()
                    .source(&source)
                    .type_(&event_type)
                    .payload(payload)
                    .insert()
                    .await?;
                
                // Verify basic properties
                assert_eq!(event.source, source);
                assert_eq!(event.event_type, event_type);
                assert!(event.id.to_string().len() == 26); // ULID length
                
                Ok(())
            }
        ).await
    }
    
    /// Test event querying property
    pub async fn test_event_querying_property(&mut self, test_cases: u32) -> TestResult {
        self.test_property(
            SinexStrategies::any_event(),
            test_cases,
            |ctx, (source, event_type, payload)| async move {
                // Property: Inserted events should be retrievable
                let event = ctx.event()
                    .source(&source)
                    .type_(&event_type)
                    .payload(payload)
                    .insert()
                    .await?;
                
                // Should be findable by ID
                let by_id = ctx.events().by_id(event.id).fetch_one().await?;
                assert!(by_id.is_some());
                
                // Should be findable by source
                let by_source = ctx.events().by_source(&source).fetch().await?;
                assert!(by_source.iter().any(|e| e.id == event.id));
                
                // Should be findable by type
                let by_type = ctx.events().by_type(&event_type).fetch().await?;
                assert!(by_type.iter().any(|e| e.id == event.id));
                
                Ok(())
            }
        ).await
    }
    
    /// Test malicious input handling
    pub async fn test_malicious_input_rejection(&mut self, test_cases: u32) -> TestResult {
        self.test_property(
            SinexStrategies::malicious_payload(),
            test_cases,
            |ctx, malicious_payload| async move {
                // Property: Malicious payloads should be rejected or sanitized
                let result = ctx.event()
                    .source("security_test")
                    .type_("malicious.input")
                    .payload(malicious_payload)
                    .insert()
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
                
                Ok(())
            }
        ).await
    }
    
    /// Test concurrency properties
    pub async fn test_concurrent_insertion_property(&mut self, test_cases: u32) -> TestResult {
        self.test_property(
            prop::collection::vec(SinexStrategies::any_event(), 1..10),
            test_cases,
            |ctx, events| async move {
                // Property: Concurrent insertions should all succeed
                let results = concurrent_test!(ctx, events.len(), |i| {
                    let (source, event_type, payload) = &events[i];
                    async move {
                        ctx.event()
                            .source(source)
                            .type_(event_type)
                            .payload(payload.clone())
                            .insert()
                            .await
                    }
                });
                
                // All insertions should succeed
                assert_eq!(results.len(), events.len());
                
                // All events should have unique IDs
                let ids: std::collections::HashSet<_> = results.iter().map(|e| e.id).collect();
                assert_eq!(ids.len(), events.len(), "Some events have duplicate IDs");
                
                Ok(())
            }
        ).await
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
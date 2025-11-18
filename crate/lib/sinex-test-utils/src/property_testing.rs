// Property Testing Integration - Harmonized with TestContext
//
// Provides property-based testing capabilities that integrate seamlessly
// with the unified test infrastructure and event builders.

use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Strategy};
use serde_json::{json, Value};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{sinex_prop, TestContext, TestResult};

    #[sinex_prop(cases = 8)]
    async fn property_creates_filesystem_events(
        ctx: &TestContext,
        #[strategy(SinexStrategies::filesystem_event())] event: (String, String, Value),
    ) -> TestResult<()> {
        let (source, event_type, payload) = event;
        let event = ctx.create_test_event(&source, &event_type, payload).await?;
        assert_eq!(event.source.as_str(), "filesystem");
        assert!(event.id.is_some());
        Ok(())
    }

    #[sinex_prop(cases = 8, seed = 42)]
    async fn property_json_payload_sanitization(
        ctx: &TestContext,
        #[strategy(SinexStrategies::json_payload())] payload: Value,
    ) -> TestResult<()> {
        let inserted = ctx
            .create_test_event("json-test", "test.json", payload.clone())
            .await?;
        let mut expected = payload.clone();
        TestContext::sanitize_payload(&mut expected);
        assert_eq!(inserted.payload, expected);
        Ok(())
    }

    #[sinex_prop(cases = 16)]
    fn property_event_source_pattern(
        #[strategy(SinexStrategies::event_source())] source: String,
    ) -> TestResult<()> {
        assert!(
            source
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_'),
            "source not normalized: {source}",
        );
        Ok(())
    }
}

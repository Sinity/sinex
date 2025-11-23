// Property Testing Integration - Harmonized with TestContext
//
// Provides property-based testing capabilities that integrate seamlessly
// with the unified test infrastructure and event builders.

use once_cell::sync::Lazy;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence};
use serde_json::{json, Value};
use std::{collections::HashMap, env, fs, path::PathBuf, sync::Mutex};

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

static PERSISTENCE_CACHE: Lazy<Mutex<HashMap<String, &'static str>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

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
    use super::*;
    use crate::{sinex_prop, TestContext};
    use color_eyre::eyre::Report;
    const FLOAT_ABS_TOLERANCE: f64 = 1e-12;
    const FLOAT_REL_TOLERANCE: f64 = 1e-12;

    #[sinex_prop(cases = 8)]
    async fn property_creates_filesystem_events(
        ctx: &TestContext,
        #[strategy(SinexStrategies::filesystem_event())] event: (String, String, Value),
    ) -> TestResult<()> {
        let (source, event_type, payload) = event;
        let event = ctx.create_test_event(&source, &event_type, payload).await?;
        assert_eq!(event.source.as_str(), "filesystem");
        assert!(event.id.is_some());
        Ok::<(), Report>(())
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
        assert_json_equivalent(&inserted.payload, &expected);
        Ok::<(), Report>(())
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
}

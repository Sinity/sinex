// Property Testing Integration - Harmonized with TestContext
//
// Provides property-based testing capabilities that integrate seamlessly
// with the unified test infrastructure and event builders.

use once_cell::sync::Lazy;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence};
use std::{collections::HashMap, env, fs, path::PathBuf, sync::Mutex};

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
    use crate::{sinex_prop, TestContext};
    use color_eyre::eyre::Report;
    use proptest::prelude::*;
    use proptest::strategy::BoxedStrategy;
    use serde_json::{json, Value};
    const FLOAT_ABS_TOLERANCE: f64 = 1e-12;
    const FLOAT_REL_TOLERANCE: f64 = 1e-12;

    fn file_path_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just("/tmp/test.txt".to_string()),
            Just("/home/user/document.pdf".to_string()),
            Just("/var/log/system.log".to_string()),
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

    #[sinex_prop(cases = 8)]
    async fn property_creates_filesystem_events(
        ctx: &TestContext,
        #[strategy(filesystem_event_strategy())] event: (String, String, Value),
    ) -> TestResult<()> {
        let (source, event_type, payload) = event;
        let event = ctx
            .publish_json_event(&source, &event_type, payload)
            .await?;
        assert_eq!(event.source.as_str(), "filesystem");
        assert!(event.id.is_some());
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
            .publish_json_event("json-test", "test.json", payload.clone())
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

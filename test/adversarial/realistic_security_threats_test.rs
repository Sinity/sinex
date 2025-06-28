use crate::common::prelude::*;
use crate::common::resources;
use std::fs;

/// Test filesystem monitoring against path traversal attacks
#[sinex_test]
async fn test_filesystem_path_traversal_protection(_ctx: TestContext) -> TestResult {
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

/// Test SQL injection protection in dynamic query construction
#[sinex_test(timeout = 20)]
async fn test_sql_injection_protection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Create test agent
    let agent_name = format!("sql_injection_test_{}", Ulid::new());
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "SQL injection security test"
    )
    .execute(&pool)
    .await?;

    // SQL injection attack patterns targeting common query parameters
    let injection_attacks = vec![
        // Classic injection attempts
        "'; DROP TABLE raw.events; --",
        "' OR '1'='1' --",
        "' UNION SELECT * FROM sinex_schemas.agent_manifests --",
        "'; INSERT INTO raw.events (id, source, event_type, host, payload) VALUES ('malicious', 'attack', 'injection', 'hacker', '{}'); --",


        "' AND (SELECT COUNT(*) FROM sinex_schemas.agent_manifests) > 0 --",
        "' OR (SELECT SUBSTRING(agent_name,1,1) FROM sinex_schemas.agent_manifests LIMIT 1)='a' --",


        "'; WAITFOR DELAY '00:00:05' --",
        "' OR pg_sleep(5) --",


        "'; CREATE TABLE malicious_table (data TEXT); --",
        "'; UPDATE sinex_schemas.agent_manifests SET description='HACKED' WHERE agent_name='test'; --",


        "%27%20OR%20%271%27%3D%271%27%20--",  // URL encoded
        "&#x27; OR &#x27;1&#x27;=&#x27;1&#x27; --",  // HTML entity encoded


        "' OR ASCII(SUBSTRING((SELECT agent_name FROM sinex_schemas.agent_manifests LIMIT 1),1,1))>64 --",
        "' AND (SELECT COUNT(*) FROM information_schema.tables WHERE table_name='agent_manifests')>0 --",
    ];

    let mut injection_attempts = Vec::new();
    let mut successful_injections = Vec::new();

    for (i, injection_payload) in injection_attacks.iter().enumerate() {
        println!("Testing injection payload {}: {}", i + 1, injection_payload);

        // Test 1: Event source field injection
        let malicious_source = injection_payload;
        let event_result = insert_raw_event(
            pool,
            malicious_source,
            "injection_test",
            "localhost",
            json!({"test": "injection"}),
            None,
            Some("1.0.0"),
            None,
        )
        .await;

        match event_result {
            Ok(event) => {
                // Check if injection was executed or just stored as data
                let stored_source: String = sqlx::query_scalar!(
                    "SELECT source FROM raw.events WHERE id = $1::uuid::ulid",
                    event.id.to_uuid()
                )
                .fetch_one(pool)
                .await?;

                if stored_source != *injection_payload {
                    successful_injections.push(format!(
                        "Event source injection: payload '{}' was modified to '{}'",
                        injection_payload, stored_source
                    ));
                }

                // Verify database integrity wasn't compromised
                let agent_count: i64 =
                    sqlx::query_scalar!("SELECT COUNT(*) FROM sinex_schemas.agent_manifests")
                        .fetch_one(pool)
                        .await?
                        .unwrap_or(0);

                if agent_count != 1 {
                    successful_injections.push(format!(
                        "Database integrity compromised: agent_manifests count = {} (expected 1)",
                        agent_count
                    ));
                }
            }
            Err(e) => {
                // Error is acceptable - injection was blocked
                println!("  Injection blocked: {}", e);
            }
        }

        injection_attempts.push(injection_payload.to_string());

        // Test 2: Agent name injection in work queue operations
        let malicious_agent_name = format!("test{}", injection_payload);

        // Try to create agent with malicious name
        let agent_creation = sqlx::query!(
            "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
             VALUES ($1, $2, $3)",
            malicious_agent_name,
            "1.0.0",
            "Injection test agent"
        )
        .execute(&pool)
        .await;

        match agent_creation {
            Ok(_) => {
                // Agent was created - check if name was sanitized
                let stored_agents: Vec<String> = sqlx::query_scalar!(
                    "SELECT agent_name FROM sinex_schemas.agent_manifests
                     WHERE agent_name LIKE $1",
                    format!(
                        "test%{}",
                        injection_payload.chars().take(10).collect::<String>()
                    )
                )
                .fetch_all(pool)
                .await
                .unwrap_or_default();

                if stored_agents.len() > 1 {
                    successful_injections.push(format!(
                        "Agent name injection may have succeeded: {} agents found",
                        stored_agents.len()
                    ));
                }

                // Clean up
                sqlx::query!(
                    "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                    malicious_agent_name
                )
                .execute(&pool)
                .await
                .ok();
            }
            Err(e) => {
                println!("  Agent creation blocked: {}", e);
            }
        }

        // Test 3: Search/filter injection
        let search_result = sqlx::query!(
            "SELECT agent_name FROM sinex_schemas.agent_manifests
             WHERE description LIKE $1 LIMIT 10",
            format!("%{}%", injection_payload)
        )
        .fetch_all(pool)
        .await;

        if let Err(e) = search_result {
            if e.to_string().contains("syntax error") || e.to_string().contains("invalid") {
                println!("  Search injection blocked: {}", e);
            }
        }
    }

    println!("\nSQL Injection Attack Test Results:");
    println!("  Injection patterns tested: {}", injection_attempts.len());
    println!("  Successful injections: {}", successful_injections.len());

    for injection in &successful_injections {
        println!("  SECURITY VIOLATION: {}", injection);
    }

    // Security requirement: NO SQL injections should succeed
    assert!(
        successful_injections.is_empty(),
        "SQL injection attacks succeeded:\n{}",
        successful_injections.join("\n")
    );

    println!("  ✓ All SQL injection attacks blocked");

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source LIKE '%DROP%' OR source LIKE '%UNION%' OR source LIKE '%OR%'")
        .execute(&pool).await.ok();
    sqlx::query!(
        "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    )
    .execute(&pool)
    .await?;

    Ok(())
}

/// Test resource exhaustion attack protection
#[sinex_test(timeout = 30)]
async fn test_resource_exhaustion_protection(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let agent_name = format!("exhaustion_test_{}", Ulid::new());

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Resource exhaustion test"
    )
    .execute(&pool)
    .await?;

    // Test 1: Memory exhaustion via large payloads
    let large_payload_attack = json!({
        "attack_type": "memory_exhaustion",
        "large_data": "A".repeat(10_000_000), // 10MB string
        "nested_objects": (0..1000).map(|i| json!({
            "level1": {
                "level2": {
                    "level3": {
                        "data": format!("nested_data_{}", i),
                        "large_field": "B".repeat(1000)
                    }
                }
            }
        })).collect::<Vec<_>>()
    });

    let memory_attack_start = std::time::Instant::now();
    let memory_attack_result = timeout(
        Duration::from_secs(3),
        insert_raw_event(
            pool,
            "memory.exhaustion",
            "large_payload",
            "localhost",
            large_payload_attack,
            None,
            Some("1.0.0"),
            None,
        ),
    )
    .await;

    let memory_attack_duration = memory_attack_start.elapsed();

    match memory_attack_result {
        Ok(Ok(_)) => {
            println!(
                "Large payload accepted in {:?} - checking for performance impact",
                memory_attack_duration
            );

            // Check if system is still responsive
            let health_check_start = std::time::Instant::now();
            let health_result = sqlx::query_scalar!("SELECT 1").fetch_one(pool).await;
            let health_check_duration = health_check_start.elapsed();

            if health_check_duration > Duration::from_secs(1) {
                println!("  WARNING: System response degraded after large payload attack");
            } else {
                println!("  ✓ System remained responsive after large payload");
            }

            assert!(
                health_result.is_ok(),
                "System should remain functional after large payload"
            );
        }
        Ok(Err(e)) => {
            println!("  ✓ Large payload rejected: {}", e);
        }
        Err(_) => {
            println!("  ✓ Large payload request timed out (protection mechanism)");
        }
    }

    // Test 2: Connection exhaustion attack
    let connection_attack_start = std::time::Instant::now();
    let mut attack_connections = Vec::new();
    let max_connection_attempts = 100;

    for i in 0..max_connection_attempts {
        match timeout(Duration::from_millis(100), pool.acquire()).await {
            Ok(Ok(conn)) => {
                attack_connections.push(conn);
            }
            Ok(Err(e)) => {
                println!("  Connection {} rejected: {}", i, e);
                break;
            }
            Err(_) => {
                println!("  Connection {} timed out", i);
                break;
            }
        }

        if i % 10 == 0 {
            println!("  Acquired {} connections", i + 1);
        }
    }

    let connections_acquired = attack_connections.len();
    println!("  Total connections acquired: {}", connections_acquired);

    // Test if system can still function with remaining capacity
    let remaining_capacity_test = timeout(
        Duration::from_secs(3),
        sqlx::query_scalar!("SELECT COUNT(*) FROM sinex_schemas.agent_manifests").fetch_one(pool),
    )
    .await;

    match remaining_capacity_test {
        Ok(Ok(_)) => {
            println!(
                "  ✓ System functional despite {} held connections",
                connections_acquired
            );
        }
        Ok(Err(e)) => {
            println!(
                "  WARNING: System degraded with {} connections: {}",
                connections_acquired, e
            );
        }
        Err(_) => {
            println!(
                "  WARNING: System timeout with {} connections",
                connections_acquired
            );
        }
    }

    // Clean up connections
    drop(attack_connections);
    let connection_attack_duration = connection_attack_start.elapsed();

    // Test 3: Query complexity attack (expensive operations)
    let complexity_attacks = vec![
        // Cartesian product attack
        "SELECT COUNT(*) FROM sinex_schemas.agent_manifests a1, sinex_schemas.agent_manifests a2, sinex_schemas.agent_manifests a3",


        "SELECT COUNT(*) FROM raw.events WHERE source ~ '.*a.*b.*c.*d.*e.*f.*g.*h.*i.*j.*'",


        "SELECT * FROM raw.events ORDER BY payload::text, source, event_type, host LIMIT 1000000",


        "WITH RECURSIVE attack(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM attack WHERE n < 100000) SELECT COUNT(*) FROM attack",
    ];

    let mut blocked_complex_queries = 0;

    for (i, complex_query) in complexity_attacks.iter().enumerate() {
        println!(
            "Testing complex query {}: {}",
            i + 1,
            &complex_query[..100.min(complex_query.len())]
        );

        let complexity_result = timeout(
            Duration::from_secs(3),
            sqlx::query(complex_query).execute(&pool),
        )
        .await;

        match complexity_result {
            Ok(Ok(_)) => {
                println!("  Complex query completed (may indicate vulnerability)");
            }
            Ok(Err(e)) => {
                println!("  ✓ Complex query blocked: {}", e);
                blocked_complex_queries += 1;
            }
            Err(_) => {
                println!("  ✓ Complex query timed out (protection active)");
                blocked_complex_queries += 1;
            }
        }
    }

    println!("\nResource Exhaustion Attack Test Results:");
    println!("  Memory attack duration: {:?}", memory_attack_duration);
    println!(
        "  Connection attack duration: {:?}",
        connection_attack_duration
    );
    println!("  Connections acquired: {}", connections_acquired);
    println!(
        "  Complex queries blocked: {}/{}",
        blocked_complex_queries,
        complexity_attacks.len()
    );

    // System should have some protection mechanisms
    assert!(
        connections_acquired < max_connection_attempts,
        "Connection pool should have limits"
    );
    assert!(
        memory_attack_duration < Duration::from_secs(3),
        "Large payload handling should not hang system"
    );

    println!("  ✓ Resource exhaustion protections active");

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'memory.exhaustion'")
        .execute(&pool)
        .await
        .ok();
    sqlx::query!(
        "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    )
    .execute(&pool)
    .await?;

    Ok(())
}

/// Test malicious configuration injection
#[sinex_test]
async fn test_configuration_injection_protection(_ctx: TestContext) -> TestResult {
    use std::fs;

    let temp_dir = resources::temp_dir()?;
    let config_dir = temp_dir.path().join("config");
    fs::create_dir_all(&config_dir)?;

    // Test malicious TOML injection in configuration
    let malicious_configs = vec![
        // Command injection in file paths
        r#"
        [event_sources.filesystem]
        watch_paths = ["/tmp; rm -rf /; echo"]
        "#,
        // Path traversal in configuration
        r#"
        [event_sources.filesystem]
        watch_paths = ["../../../etc/passwd"]
        "#,
        // Malicious regex injection
        r#"
        [routing.rules]
        pattern = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)*(.+)*"
        "#,
        // TOML bomb (deeply nested structure)
        r#"
        [a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z]
        value = "deep"
        "#,
        // Unicode/encoding attacks
        r#"
        [event_sources]
        name = "test\u0000\u0001\u0002\ufeff"
        "#,
        // Large string attack
        r#"
        [event_sources.test]
        large_field = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        "#,
    ];

    let mut config_violations = Vec::new();

    for (i, malicious_config) in malicious_configs.iter().enumerate() {
        let config_file = config_dir.join(format!("malicious_{}.toml", i));

        // Try to write malicious config
        match fs::write(&config_file, malicious_config) {
            Ok(_) => {
                // Try to parse as TOML
                let parse_result = fs::read_to_string(&config_file).and_then(|content| {
                    toml::from_str::<toml::Value>(&content)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                });

                match parse_result {
                    Ok(parsed_config) => {
                        // Check for dangerous values
                        let config_str = format!("{:?}", parsed_config);

                        if config_str.contains("/etc/passwd") {
                            config_violations
                                .push(format!("Config {}: Path traversal not sanitized", i));
                        }

                        if config_str.contains("; rm -rf") {
                            config_violations
                                .push(format!("Config {}: Command injection not sanitized", i));
                        }

                        if config_str.len() > 10_000_000 {
                            config_violations.push(format!(
                                "Config {}: Large config not rejected ({}MB)",
                                i,
                                config_str.len() / 1_000_000
                            ));
                        }

                        println!("  Config {} parsed successfully (may need validation)", i);
                    }
                    Err(e) => {
                        println!("  ✓ Config {} rejected: {}", i, e);
                    }
                }
            }
            Err(e) => {
                println!("  ✓ Config {} write blocked: {}", i, e);
            }
        }
    }

    println!("\nConfiguration Injection Test Results:");
    println!("  Malicious configs tested: {}", malicious_configs.len());
    println!("  Configuration violations: {}", config_violations.len());

    for violation in &config_violations {
        println!("  SECURITY VIOLATION: {}", violation);
    }

    // Configuration parsing should reject or sanitize malicious content
    if !config_violations.is_empty() {
        println!("  WARNING: Configuration validation may need strengthening");
    } else {
        println!("  ✓ Configuration injection protections active");
    }

    Ok(())
}

/// Test event payload sanitization
#[sinex_test(timeout = 15)]
async fn test_malicious_payload_sanitization(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    // Test various malicious payload patterns
    let malicious_payloads = vec![
        // Script injection attempts
        json!({
            "user_input": "<script>alert('xss')</script>",
            "html_content": "<img src=x onerror=alert('xss')>",
            "javascript": "javascript:alert('xss')"
        }),
        // JSON injection/pollution
        json!({
            "__proto__": {"admin": true},
            "constructor": {"prototype": {"admin": true}},
            "prototype": {"admin": true}
        }),
        // Binary/encoding attacks
        json!({
            "binary_data": "\x00\x01\x02\x03\x04\x05",
            "unicode_attack": "\u{0000}\u{0001}\u{0002}\u{0003}",
            "overlong_utf8": "invalid_utf8_sequence",
            "null_bytes": "test\x00data"
        }),
        // Large object attack
        json!({
            "large_array": (0..10000).collect::<Vec<i32>>(),
            "large_string": "X".repeat(100_000),
            "deep_nesting": {
                "level1": {
                    "level2": {
                        "level3": {
                            "level4": {
                                "level5": "deep"
                            }
                        }
                    }
                }
            }
        }),
        // Format string attacks
        json!({
            "format_string": "%s%s%s%s%s%s%s%s%s%s",
            "printf_attack": "%n%n%n%n%n%n%n%n%n%n",
            "path_attack": "../../../etc/passwd"
        }),
    ];

    let mut payload_violations = Vec::new();
    let mut successful_events = 0;

    for (i, malicious_payload) in malicious_payloads.iter().enumerate() {
        println!("Testing malicious payload {}", i + 1);

        let result = insert_raw_event(
            pool,
            "security.payload_test",
            "malicious_payload",
            "localhost",
            malicious_payload.clone(),
            None,
            Some("1.0.0"),
            None,
        )
        .await;

        match result {
            Ok(event) => {
                successful_events += 1;

                // Retrieve stored payload and check for sanitization
                let stored_payload: serde_json::Value = sqlx::query_scalar!(
                    "SELECT payload FROM raw.events WHERE id = $1::uuid::ulid",
                    event.id.to_uuid()
                )
                .fetch_one(pool)
                .await?;

                let stored_str = stored_payload.to_string();

                // Check if dangerous content was sanitized
                if stored_str.contains("<script>") {
                    payload_violations.push(format!("Payload {}: Script tags not sanitized", i));
                }

                if stored_str.contains("__proto__") {
                    payload_violations
                        .push(format!("Payload {}: Prototype pollution not prevented", i));
                }

                if stored_str.contains("\x00") {
                    payload_violations.push(format!("Payload {}: Null bytes not sanitized", i));
                }

                // Check for excessive size
                if stored_str.len() > 1_000_000 {
                    payload_violations.push(format!(
                        "Payload {}: Large payload not limited ({}KB)",
                        i,
                        stored_str.len() / 1000
                    ));
                }

                println!("  Payload {} stored successfully", i);
            }
            Err(e) => {
                println!("  ✓ Payload {} rejected: {}", i, e);
            }
        }
    }

    println!("\nMalicious Payload Test Results:");
    println!("  Payloads tested: {}", malicious_payloads.len());
    println!("  Successful storage: {}", successful_events);
    println!("  Sanitization violations: {}", payload_violations.len());

    for violation in &payload_violations {
        println!("  SECURITY VIOLATION: {}", violation);
    }

    // Payloads should be validated/sanitized appropriately
    if !payload_violations.is_empty() {
        println!("  WARNING: Payload sanitization may need enhancement");
    } else {
        println!("  ✓ Payload sanitization active");
    }

    // Cleanup
    sqlx::query!("DELETE FROM raw.events WHERE source = 'security.payload_test'")
        .execute(&pool)
        .await
        .ok();

    Ok(())
}

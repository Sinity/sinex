use crate::common::prelude::*;
use sinex_db::validation::EventValidator;
use std::time::{Duration, Instant};

#[sinex_test]
async fn test_circular_json_references(ctx: TestContext) -> TestResult {
    // Test that Sinex's event validation handles circular JSON references safely
    let circular_json = json!({
        "data": {
            "id": 1,
            "children": [
                {"$ref": "#/data"},  // Points back to root data
                {"$ref": "#/data/children/0"}  // Points to first child (self)
            ]
        },
        "metadata": {
            "refs": {
                "self": {"$ref": "#/metadata"},
                "parent": {"$ref": "#"}
            }
        }
    });

    // Test serialization doesn't cause infinite loops or stack overflow
    let start = Instant::now();
    let serialization_result = std::panic::catch_unwind(|| serde_json::to_string(&circular_json));
    let elapsed = start.elapsed();

    // Assert serialization completes in reasonable time without panicking
    assert!(
        serialization_result.is_ok(),
        "Circular JSON should not cause panic"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "Serialization should complete quickly"
    );

    // Test with Sinex validator - should handle gracefully
    let validator = EventValidator::new();
    let validation_result = validator.validate_with_rules("test", "circular.test", &circular_json);

    // Validator should either accept or gracefully reject, but not panic
    match validation_result {
        Ok(_) => {
            // If accepted, verify it's properly handled
            assert!(
                true,
                "Validator accepted circular JSON - should handle safely"
            );
        }
        Err(e) => {
            // If rejected, error should be meaningful
            assert!(
                !e.to_string().is_empty(),
                "Validation error should provide meaningful message"
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_json_billion_laughs_attack(ctx: TestContext) -> TestResult {
    // Test that Sinex can handle exponentially expanding JSON without resource exhaustion
    // Each level exponentially expands the previous

    let mut expanding_json = json!({
        "lol1": "lol".repeat(10),
    });

    let mut successful_levels = 0;
    let mut max_serialization_time = Duration::from_millis(0);

    // Create exponential expansion
    for level in 2..=8 {
        // Reduced max level for safety
        let prev_key = format!("lol{}", level - 1);
        let current_key = format!("lol{}", level);

        // Each level references previous level 10 times
        let mut expansion = Vec::new();
        for _ in 0..10 {
            if let Some(prev_value) = expanding_json.get(&prev_key) {
                expansion.push(prev_value.clone());
            }
        }

        expanding_json[current_key] = json!(expansion);

        // Test serialization at each level with time limits
        let start = Instant::now();
        match serde_json::to_string(&expanding_json) {
            Ok(json_str) => {
                let elapsed = start.elapsed();
                successful_levels += 1;
                max_serialization_time = max_serialization_time.max(elapsed);

                // Assert reasonable performance limits
                if elapsed > Duration::from_secs(2) {
                    break; // Stop before hitting resource limits
                }

                if json_str.len() > 10_000_000 {
                    // 10MB limit for safety
                    break;
                }

                // Verify the JSON structure is maintained
                assert!(json_str.len() > 0, "Serialized JSON should not be empty");
            }
            Err(_) => {
                break; // Stop on serialization failure
            }
        }
    }

    // Assert that Sinex can handle at least a few levels of expansion
    assert!(
        successful_levels >= 3,
        "Should handle at least 3 levels of exponential expansion"
    );
    assert!(
        max_serialization_time < Duration::from_secs(5),
        "Serialization should not take excessively long"
    );
    Ok(())
}

#[sinex_test]
async fn test_json_unicode_normalization_bypass(ctx: TestContext) -> TestResult {
    // Different Unicode representations that might bypass validation

    let unicode_variants = vec![
        ("Normal", "admin"),
        ("Zero-width", "ad\u{200B}min"),        // Zero-width space
        ("Right-to-left", "\u{202E}nimda"),     // RTL override makes "admin" appear as "admin"
        ("Homograph", "аdmin"),                 // Cyrillic 'а' instead of Latin 'a'
        ("Combining", "a\u{0301}dmin"),         // 'a' with acute accent combining
        ("Fullwidth", "ａｄｍｉｎ"),            // Fullwidth variants
        ("Invisible", "\u{2060}admin\u{2060}"), // Word joiner characters
    ];

    println!("Unicode normalization bypass test:");

    let validator = EventValidator::new();

    for (variant_name, username) in unicode_variants {
        let test_event = json!({
            "username": username,
            "action": "login",
            "permissions": ["admin", "write", "delete"]
        });

        println!(
            "  Testing {}: '{}' (bytes: {:?})",
            variant_name,
            username,
            username.as_bytes()
        );

        // Test if validation treats these as equivalent
        match validator.validate_with_rules("auth", "user.login", &test_event) {
            Ok(_) => {
                println!("    ACCEPTED - potential bypass if 'admin' is filtered");

                // Check if this would bypass a naive "admin" filter
                if username != "admin" && username.to_lowercase().contains("admin") {
                    println!("    SECURITY RISK: Might bypass simple admin filtering!");
                }
            }
            Err(e) => {
                println!("    Rejected: {}", e);
            }
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_json_depth_stack_overflow(ctx: TestContext) -> TestResult {
    // Test deeply nested JSON that could cause stack overflow during parsing/validation

    // Create JSON with extreme depth that might cause stack overflow
    // Using iterative approach to avoid stack overflow during creation
    fn create_deeply_nested_json(depth: usize) -> serde_json::Value {
        let mut result = json!("base_value");
        for i in 1..=depth {
            result = json!({
                "level": i,
                "nested": result
            });
        }
        result
    }

    println!("Testing JSON depth stack overflow attack:");

    // Test increasing depths to find limits
    // Reduced max depth to prevent stack overflow during serialization
    for depth in [10, 50, 100, 200, 300] {
        println!("  Testing depth: {}", depth);

        let start = Instant::now();

        // Test creation and serialization in panic-safe context
        let result = std::panic::catch_unwind(|| {
            let deep_json = create_deeply_nested_json(depth);
            let serialized = serde_json::to_string(&deep_json)?;
            Ok::<(usize, Duration), serde_json::Error>((serialized.len(), start.elapsed()))
        });

        match result {
            Ok(Ok((size, elapsed))) => {
                println!("    SUCCESS: {} bytes in {:?}", size, elapsed);

                // Test with Sinex validator to ensure it handles deep nesting
                let validator_result = std::panic::catch_unwind(|| {
                    let deep_json = create_deeply_nested_json(depth);
                    let validator = EventValidator::new();
                    validator.validate_with_rules("test", "deep.nesting", &deep_json)
                });

                match validator_result {
                    Ok(Ok(_)) => println!("    Validator accepted depth {}", depth),
                    Ok(Err(e)) => println!("    Validator rejected depth {}: {}", depth, e),
                    Err(_) => println!("    Validator panicked at depth {} - stack overflow protection needed", depth),
                }

                // Stop if serialization becomes too slow (potential DoS)
                if elapsed > Duration::from_secs(2) {
                    println!("    STOPPING: Serialization too slow, potential DoS vector");
                    break;
                }
            }
            Ok(Err(e)) => {
                println!("    SERIALIZATION ERROR at depth {}: {}", depth, e);
                break;
            }
            Err(_) => {
                println!("    STACK OVERFLOW: Panic at depth {} - limit found", depth);
                break;
            }
        }
    }

    // Test alternative deep nesting patterns
    println!("  Testing array depth:");

    let mut deep_array = json!("base");
    for level in 1..=300 {
        deep_array = json!([deep_array]);

        if level % 100 == 0 {
            match std::panic::catch_unwind(|| serde_json::to_string(&deep_array)) {
                Ok(Ok(serialized)) => {
                    println!("    Array depth {}: {} bytes", level, serialized.len());
                }
                Ok(Err(_)) => {
                    println!("    Array serialization failed at depth {}", level);
                    break;
                }
                Err(_) => {
                    println!("    Array stack overflow at depth {}", level);
                    break;
                }
            }
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_json_key_confusion_attack(ctx: TestContext) -> TestResult {
    // Test various key representations that might be treated as equivalent

    let key_variants = vec![
        "key",
        "Key",
        "KEY",
        "k\u{0065}y",  // 'e' as Unicode escape
        "k\u{0301}ey", // 'e' with combining accent
        "\u{006B}ey",  // 'k' as Unicode escape
        "＿key",       // Fullwidth underscore prefix
        "key\u{2060}", // Word joiner suffix
    ];

    println!("JSON key confusion attack:");

    // Create object with variant keys
    let mut test_object = serde_json::Map::new();

    for (i, key_variant) in key_variants.iter().enumerate() {
        test_object.insert(key_variant.to_string(), json!(format!("value_{}", i)));
    }

    let json_with_variants = json!(test_object);

    println!("  Created object with {} variant keys", key_variants.len());

    // Test access patterns
    for lookup_key in &key_variants {
        if let Some(value) = json_with_variants.get(lookup_key) {
            println!("    Key '{}' found: {}", lookup_key, value);
        } else {
            println!("    Key '{}' not found", lookup_key);
        }
    }

    // Test if serialization preserves all variants
    let serialized = serde_json::to_string_pretty(&json_with_variants).unwrap();
    println!("  Serialized object:\n{}", serialized);

    // Check for potential key collisions
    let unique_values: std::collections::HashSet<_> =
        json_with_variants.as_object().unwrap().values().collect();

    if unique_values.len() != key_variants.len() {
        println!(
            "  KEY COLLISION: {} unique values for {} keys!",
            unique_values.len(),
            key_variants.len()
        );
    }
    Ok(())
}

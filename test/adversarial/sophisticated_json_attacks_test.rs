use serde_json::json;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use sinex_db::validation::EventValidator;

#[test]
fn test_circular_json_references() {
    // Create JSON with circular references using JSON Pointer syntax
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
    
    println!("Testing circular JSON references:");
    
    // Test serialization (might infinite loop or stack overflow)
    let start = Instant::now();
    match std::panic::catch_unwind(|| {
        serde_json::to_string(&circular_json)
    }) {
        Ok(result) => {
            let elapsed = start.elapsed();
            match result {
                Ok(json_str) => {
                    println!("  Serialization succeeded in {:?}: {} bytes", elapsed, json_str.len());
                    if elapsed > Duration::from_secs(1) {
                        println!("  WARNING: Serialization took too long - potential infinite loop!");
                    }
                }
                Err(e) => {
                    println!("  Serialization failed: {}", e);
                }
            }
        }
        Err(_) => {
            println!("  PANIC: Circular reference caused stack overflow!");
        }
    }
    
    // Test with validator
    let validator = EventValidator::new();
    match validator.validate_with_rules("test", "circular.test", &circular_json) {
        Ok(_) => println!("  Validator accepted circular JSON"),
        Err(e) => println!("  Validator rejected circular JSON: {}", e),
    }
}

#[test]
fn test_json_billion_laughs_attack() {
    // XML billion laughs attack adapted for JSON
    // Each level exponentially expands the previous
    
    let mut expanding_json = json!({
        "lol1": "lol".repeat(10),
    });
    
    // Create exponential expansion
    for level in 2..=10 {
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
        
        // Calculate theoretical size
        let theoretical_size = 10_u64.pow(level as u32);
        println!("Level {}: Theoretical size {} elements", level, theoretical_size);
        
        // Test serialization at each level
        let start = Instant::now();
        match serde_json::to_string(&expanding_json) {
            Ok(json_str) => {
                let elapsed = start.elapsed();
                println!("  Serialized {} bytes in {:?}", json_str.len(), elapsed);
                
                if elapsed > Duration::from_secs(2) {
                    println!("  PERFORMANCE ISSUE: Serialization too slow at level {}", level);
                    break;
                }
                
                if json_str.len() > 100_000_000 { // 100MB
                    println!("  MEMORY ISSUE: JSON too large at level {}", level);
                    break;
                }
            }
            Err(e) => {
                println!("  Serialization failed at level {}: {}", level, e);
                break;
            }
        }
    }
}

#[test]
fn test_hash_collision_dos_attack() {
    // Generate keys that cause hash collisions in common hash functions
    
    // Known collision-prone strings for djb2 hash
    let collision_strings = vec![
        "Aa", "BB",  // These often hash to same value
        "AaAa", "AaBB", "BBAa", "BBBB",
        "C", "D",
        "AaAaAa", "AaAaBB", "AaBBAa", "AaBBBB",
        "BBBBAa", "BBBBBB",
    ];
    
    // Create object with many collision-prone keys
    let mut collision_map = HashMap::new();
    
    let start_setup = Instant::now();
    
    // Generate thousands of keys designed to collide
    for i in 0..10000 {
        let base = &collision_strings[i % collision_strings.len()];
        let key = format!("{}{:04}", base, i);
        collision_map.insert(key, format!("value_{}", i));
    }
    
    let setup_time = start_setup.elapsed();
    println!("Hash collision DoS test:");
    println!("  Setup time: {:?}", setup_time);
    
    // Test JSON operations that depend on hash performance
    let collision_json = json!(collision_map);
    
    // Test serialization performance
    let start_serialize = Instant::now();
    let serialized = serde_json::to_string(&collision_json).unwrap();
    let serialize_time = start_serialize.elapsed();
    
    println!("  Serialization: {} bytes in {:?}", serialized.len(), serialize_time);
    
    // Test deserialization performance
    let start_deserialize = Instant::now();
    let _deserialized: serde_json::Value = serde_json::from_str(&serialized).unwrap();
    let deserialize_time = start_deserialize.elapsed();
    
    println!("  Deserialization: {:?}", deserialize_time);
    
    // Test object access performance (hash lookups)
    let start_access = Instant::now();
    let mut found_count = 0;
    
    for i in 0..1000 {
        let search_key = format!("Aa{:04}", i);
        if collision_json.get(&search_key).is_some() {
            found_count += 1;
        }
    }
    
    let access_time = start_access.elapsed();
    println!("  Key lookups: {} found in {:?}", found_count, access_time);
    
    // Performance thresholds
    if serialize_time > Duration::from_secs(5) {
        println!("  VULNERABILITY: Serialization too slow - DoS possible!");
    }
    
    if deserialize_time > Duration::from_secs(5) {
        println!("  VULNERABILITY: Deserialization too slow - DoS possible!");
    }
    
    if access_time > Duration::from_millis(500) {
        println!("  VULNERABILITY: Hash lookups too slow - collision attack successful!");
    }
}

#[test]
fn test_json_unicode_normalization_bypass() {
    // Different Unicode representations that might bypass validation
    
    let unicode_variants = vec![
        ("Normal", "admin"),
        ("Zero-width", "ad\u{200B}min"),  // Zero-width space
        ("Right-to-left", "\u{202E}nimda"),  // RTL override makes "admin" appear as "admin"
        ("Homograph", "аdmin"),  // Cyrillic 'а' instead of Latin 'a'
        ("Combining", "a\u{0301}dmin"),  // 'a' with acute accent combining
        ("Fullwidth", "ａｄｍｉｎ"),  // Fullwidth variants
        ("Invisible", "\u{2060}admin\u{2060}"),  // Word joiner characters
    ];
    
    println!("Unicode normalization bypass test:");
    
    let validator = EventValidator::new();
    
    for (variant_name, username) in unicode_variants {
        let test_event = json!({
            "username": username,
            "action": "login",
            "permissions": ["admin", "write", "delete"]
        });
        
        println!("  Testing {}: '{}' (bytes: {:?})", 
                 variant_name, username, username.as_bytes());
        
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
}

#[test]
fn test_json_nested_array_explosion() {
    // Create deeply nested arrays that expand exponentially
    
    fn create_nested_arrays(depth: usize, width: usize) -> serde_json::Value {
        if depth == 0 {
            json!("base")
        } else {
            let nested = create_nested_arrays(depth - 1, width);
            let mut array = Vec::new();
            for _ in 0..width {
                array.push(nested.clone());
            }
            json!(array)
        }
    }
    
    println!("Testing nested array explosion:");
    
    for depth in 1..=15 {
        let width: u32 = 3; // Each level has 3 copies
        let theoretical_size = width.pow(depth as u32);
        
        println!("  Depth {}: Theoretical {} elements", depth, theoretical_size);
        
        let start = Instant::now();
        
        match std::panic::catch_unwind(|| {
            let nested_array = create_nested_arrays(depth, width as usize);
            let serialized = serde_json::to_string(&nested_array).unwrap();
            (serialized.len(), start.elapsed())
        }) {
            Ok((size, elapsed)) => {
                println!("    Created {} bytes in {:?}", size, elapsed);
                
                if elapsed > Duration::from_secs(2) {
                    println!("    PERFORMANCE ISSUE: Too slow at depth {}", depth);
                    break;
                }
                
                if size > 50_000_000 { // 50MB
                    println!("    MEMORY ISSUE: Too large at depth {}", depth);
                    break;
                }
            }
            Err(_) => {
                println!("    CRASH: Stack overflow or out of memory at depth {}", depth);
                break;
            }
        }
    }
}

#[test]
fn test_json_key_confusion_attack() {
    // Test various key representations that might be treated as equivalent
    
    let key_variants = vec![
        "key",
        "Key",
        "KEY",
        "k\u{0065}y",  // 'e' as Unicode escape
        "k\u{0301}ey",  // 'e' with combining accent
        "\u{006B}ey",  // 'k' as Unicode escape
        "＿key",        // Fullwidth underscore prefix
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
        println!("  KEY COLLISION: {} unique values for {} keys!", 
                 unique_values.len(), key_variants.len());
    }
}
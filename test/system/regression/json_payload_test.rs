use sinex_db::models::RawEvent;
use serde_json::json;

#[test]
fn test_json_payload_size_limits() {
    // Test extremely large JSON payloads
    let mut huge_array = vec![];
    for i in 0..10000 {
        huge_array.push(json!({
            "index": i,
            "data": "x".repeat(100)
        });
    }
    
    let event = events::generic_adversarial_event("test", "huge.payload", json!({"test": true}), None);
    };
    
    // This might cause issues with serialization or database storage
    let serialized = serde_json::to_string(&event);
    assert!(serialized.is_ok(), "Should handle large payloads");
    
    if let Ok(json_str) = serialized {
        println!("Payload size: {} bytes", json_str.len());
        // PostgreSQL jsonb has a practical limit
        assert!(json_str.len() < 1_000_000_000, "Payload too large for PostgreSQL");
    }
}

#[test]
fn test_json_special_characters() {
    // Test JSON with special characters that might break things
    let evil_payloads = vec![
        json!({ "key": "\u{0000}" }), // Null byte
        json!({ "key": "\u{001F}" }), // Control character
        json!({ "emoji": "😈🔥💣" }), // Emojis
        json!({ "rtl": "مرحبا بالعالم" }), // Right-to-left text
        json!({ "invalid": "test_invalid_char" }), // Test special handling
    ];
    
    for (i, payload) in evil_payloads.iter().enumerate() {
        let event = events::generic_adversarial_event("test", "test.event", json!({"test": true}), None)", i),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: payload.clone(),
        };
        
        // These might fail serialization or cause database issues
        match serde_json::to_string(&event) {
            Ok(_) => println!("Payload {} serialized successfully", i),
            Err(e) => println!("Payload {} failed: {}", i, e),
        }
    }
}

#[test]
fn test_recursive_json_structure() {
    // Create a deeply nested structure
    let mut nested = json!({ "value": "base" });
    for i in 0..1000 {
        nested = json!({ 
            "level": i, 
            "nested": nested 
        });
    }
    
    let event = events::generic_adversarial_event("test", "deeply.nested", json!({"test": true}), None);
    
    // This might cause stack overflow or other issues
    let result = serde_json::to_string(&event);
    println!("Deep nesting serialization: {:?}", result.is_ok());
}
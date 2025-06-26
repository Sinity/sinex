use crate::common::prelude::*;
#[sinex_test]
async fn test_json_payload_size_limits(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test extremely large JSON payloads
    let mut huge_array = vec![];
    for i in 0..10000 {
        huge_array.push(json!({
            "index": i,
            "data": "x".repeat(100)
        }));
    }
    
    let event = crate::common::events::generic_adversarial_event("test", "huge.payload", json!({"huge_array": huge_array}), None);
    
    // This might cause issues with serialization or database storage
    let serialized = serde_json::to_string(&event);
    assert!(serialized.is_ok(), "Should handle large payloads");
    
    if let Ok(json_str) = serialized {
        println!("Payload size: {} bytes", json_str.len());
        // PostgreSQL jsonb has a practical limit
        assert!(json_str.len() < 1_000_000_000, "Payload too large for PostgreSQL");
    }
    Ok(())
}

#[sinex_test]
async fn test_json_special_characters(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test JSON with special characters that might break things
    let evil_payloads = vec![
        json!({ "key": "\u{0000}" }), // Null byte
        json!({ "key": "\u{001F}" }), // Control character
        json!({ "emoji": "😈🔥💣" }), // Emojis
        json!({ "rtl": "مرحبا بالعالم" }), // Right-to-left text
        json!({ "invalid": "test_invalid_char" }), // Test special handling
    ];
    
    for (i, payload) in evil_payloads.iter().enumerate() {
        let mut event = crate::common::events::generic_adversarial_event("test", "test.event", json!({"test": true}), None);
        event.payload = payload.clone();
        
        // These might fail serialization or cause database issues
        match serde_json::to_string(&event) {
            Ok(_) => println!("Payload {} serialized successfully", i),
            Err(e) => println!("Payload {} failed: {}", i, e),
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_recursive_json_structure(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create a deeply nested structure
    let mut nested = json!({ "value": "base" });
    for i in 0..1000 {
        nested = json!({ 
            "level": i, 
            "nested": nested 
        });
    }
    
    let event = crate::common::events::generic_adversarial_event("test", "deeply.nested", nested, None);
    
    // This might cause stack overflow or other issues
    let result = serde_json::to_string(&event);
    println!("Deep nesting serialization: {:?}", result.is_ok());
    Ok(())
}
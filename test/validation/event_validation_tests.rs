use sinex_db::validation::EventValidator;
use serde_json::json;

#[test]
fn test_filesystem_validation() {
    let validator = EventValidator::new();
    
    // Valid file.created
    let valid = json!({
        "path": "/test.txt",
        "size": 1024,
        "permissions": "644"
    });
    assert!(validator.validate_with_rules("filesystem", "file.created", &valid).is_ok());
    
    // Invalid - missing size
    let invalid = json!({
        "path": "/test.txt"
    });
    assert!(validator.validate_with_rules("filesystem", "file.created", &invalid).is_err());
    
    // Invalid - wrong type for size
    let invalid = json!({
        "path": "/test.txt",
        "size": "not a number"
    });
    assert!(validator.validate_with_rules("filesystem", "file.created", &invalid).is_err());
    
    // Invalid - bad permissions
    let invalid = json!({
        "path": "/test.txt",
        "size": 1024,
        "permissions": "999" // Invalid octal
    });
    assert!(validator.validate_with_rules("filesystem", "file.created", &invalid).is_err());
}

#[test]
fn test_unknown_event_type() {
    let validator = EventValidator::new();
    
    // Unknown events should pass if they're objects
    let unknown = json!({
        "custom_field": "value"
    });
    assert!(validator.validate_with_rules("unknown_source", "unknown_type", &unknown).is_ok());
    
    // But not if they're not objects
    let invalid = json!("just a string");
    assert!(validator.validate_with_rules("unknown_source", "unknown_type", &invalid).is_err());
}
use crate::common::prelude::*;
use sinex_db::validation::EventValidator;

#[sinex_test]
async fn test_invalid_octal_permissions(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let validator = EventValidator::new();
    
    // This should FAIL but probably won't due to the bug
    let invalid_octal = json!({
        "path": "/test.txt",
        "size": 1024,
        "permissions": "888"  // Invalid octal (8 is not a valid octal digit)
    });
    
    let result = validator.validate_with_rules("filesystem", "file.created", &invalid_octal);
    assert!(result.is_err(), "Should reject invalid octal permissions like '888'");
    Ok(())
}

#[sinex_test]
async fn test_permissions_edge_cases(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let validator = EventValidator::new();
    
    // Test various edge cases
    let test_cases = vec![
        ("999", false, "all digits > 7"),
        ("0000", true, "4 digits with leading zero"),
        ("777", true, "valid 3 digits"),
        ("1777", true, "valid 4 digits with sticky bit"),
        ("", false, "empty string"),
        ("77", false, "only 2 digits"),
        ("77777", false, "too many digits"),
        ("0x777", false, "hex prefix"),
        ("0o777", false, "octal prefix"),
    ];
    
    for (perms, should_be_valid, desc) in test_cases {
        let event = json!({
            "path": "/test.txt",
            "size": 1024,
            "permissions": perms
        });
        
        let result = validator.validate_with_rules("filesystem", "file.created", &event);
        if should_be_valid {
            assert!(result.is_ok(), "Should accept {}: {}", desc, perms);
        } else {
            assert!(result.is_err(), "Should reject {}: {}", desc, perms);
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_path_validation_missing(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let validator = EventValidator::new();
    
    // The validator doesn't check for path traversal or null bytes!
    let dangerous_paths = vec![
        "../../../etc/passwd",
        "/test\0.txt",  // null byte
        "//double//slashes//",
        "/test/../../../etc/passwd",
        "",  // empty path
    ];
    
    for path in dangerous_paths {
        let event = json!({
            "path": path,
            "size": 1024
        });
        
        let result = validator.validate_with_rules("filesystem", "file.created", &event);
        // This will likely PASS but shouldn't for security reasons
        println!("Path '{}' validation: {:?}", path, result.is_ok());
    }
    Ok(())
}
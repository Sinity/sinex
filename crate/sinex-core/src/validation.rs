use std::path::{PathBuf, Component};
use JsonValue;
use unicode_normalization::UnicodeNormalization;

const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10MB
const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_KEYS: usize = 1000;

/// Validate a file path for security issues
pub fn validate_path(path: &str) -> Result<PathBuf, crate::Error> {
    // Check for null bytes
    if path.contains('\0') {
        return Err(crate::Error::Validation("Path contains null bytes".into()));
    }
    
    // Check length
    if path.len() > 4096 {
        return Err(crate::Error::Validation("Path too long".into()));
    }
    
    let path_buf = PathBuf::from(path);
    
    // Check for directory traversal
    let mut depth = 0i32;
    for component in path_buf.components() {
        match component {
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(crate::Error::Validation("Path traversal detected".into()));
                }
            }
            Component::Normal(_) => depth += 1,
            Component::RootDir => depth = 0,
            _ => {}
        }
    }
    
    Ok(path_buf)
}

/// Validate JSON with size and depth limits
pub fn validate_json(json_str: &str) -> Result<Value, crate::Error> {
    // Size check
    if json_str.len() > MAX_JSON_SIZE {
        return Err(crate::Error::Validation(format!(
            "JSON too large: {} bytes", json_str.len()
        )));
    }
    
    // Parse
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| crate::Error::Validation(format!("Invalid JSON: {}", e)))?;
    
    // Validate structure
    validate_json_structure(&value, 0)?;
    
    Ok(value)
}

fn validate_json_structure(value: &Value, depth: usize) -> Result<(), crate::Error> {
    if depth > MAX_JSON_DEPTH {
        return Err(crate::Error::Validation(format!(
            "JSON too deep: {} levels", depth
        )));
    }
    
    match value {
        Value::Object(map) => {
            if map.len() > MAX_JSON_KEYS {
                return Err(crate::Error::Validation(format!(
                    "Too many keys: {}", map.len()
                )));
            }
            
            for (_, v) in map {
                validate_json_structure(v, depth + 1)?;
            }
        }
        Value::Array(arr) => {
            for v in arr {
                validate_json_structure(v, depth + 1)?;
            }
        }
        _ => {} // Primitives are fine
    }
    
    Ok(())
}

/// Normalize and validate Unicode strings
pub fn normalize_unicode(input: &str) -> Result<String, crate::Error> {
    // Normalize to NFC
    let normalized: String = input.nfc().collect();
    
    // Check for dangerous characters
    for ch in normalized.chars() {
        match ch {
            // Zero-width characters
            '\u{200B}'..='\u{200D}' | '\u{FEFF}' | '\u{2060}' => {
                return Err(crate::Error::Validation(
                    "Zero-width characters not allowed".into()
                ));
            }
            // Direction overrides
            '\u{202A}'..='\u{202E}' | '\u{200E}' | '\u{200F}' => {
                return Err(crate::Error::Validation(
                    "Direction control characters not allowed".into()
                ));
            }
            _ => {}
        }
    }
    
    Ok(normalized)
}

/// Check if a string contains shell metacharacters
pub fn contains_shell_metacharacters(s: &str) -> bool {
    const DANGEROUS_CHARS: &[char] = &[
        ';', '|', '&', '$', '`', '(', ')', '{', '}', 
        '<', '>', '\\', '\n', '\r', '\0', '*', '?', 
        '[', ']', '!', '~', '"', '\'',
    ];
    
    s.contains("$(") || s.contains("${") || 
    s.chars().any(|c| DANGEROUS_CHARS.contains(&c))
}

/// Detect potential billion laughs pattern in JSON
pub fn check_json_expansion(value: &Value) -> Result<(), crate::Error> {
    fn estimate_expanded_size(value: &Value, depth: usize, seen_refs: &mut std::collections::HashSet<String>) -> Result<usize, crate::Error> {
        if depth > 10 {
            return Err(crate::Error::Validation(
                "Potential billion laughs attack detected".into()
            ));
        }
        
        match value {
            Value::Object(map) => {
                let mut size = 0;
                for (k, v) in map {
                    size += k.len();
                    size += estimate_expanded_size(v, depth + 1, seen_refs)?;
                }
                Ok(size)
            }
            Value::Array(arr) => {
                let mut size = 0;
                for v in arr {
                    size += estimate_expanded_size(v, depth + 1, seen_refs)?;
                }
                // Check for exponential expansion
                if depth > 3 && arr.len() > 100 {
                    return Err(crate::Error::Validation(
                        "Suspicious array expansion detected".into()
                    ));
                }
                Ok(size)
            }
            Value::String(s) => Ok(s.len()),
            _ => Ok(8), // Number, bool, null
        }
    }
    
    let mut seen = std::collections::HashSet::new();
    let estimated_size = estimate_expanded_size(value, 0, &mut seen)?;
    
    // If expanded size is more than 100x the original, reject
    if estimated_size > value.to_string().len() * 100 {
        return Err(crate::Error::Validation(
            "JSON expansion ratio too high".into()
        ));
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_path_validation() {
        // Valid paths
        assert!(validate_path("normal/path.txt").is_ok());
        assert!(validate_path("/absolute/path.txt").is_ok());
        
        // Invalid paths
        assert!(validate_path("/etc/passwd\0.txt").is_err());
        assert!(validate_path("../../../etc/passwd").is_err());
        assert!(validate_path(&"a".repeat(5000)).is_err());
    }
    
    #[test]
    fn test_json_validation() {
        // Valid JSON
        let valid = r#"{"key": "value", "number": 42}"#;
        assert!(validate_json(valid).is_ok());
        
        // Too large
        let large = format!(r#"{{"data": "{}"}}"#, "x".repeat(11_000_000));
        assert!(validate_json(&large).is_err());
        
        // Too deep
        let mut deep = String::from("{");
        for _ in 0..40 {
            deep.push_str(r#""a":{"#);
        }
        deep.push_str("1");
        for _ in 0..40 {
            deep.push('}');
        }
        deep.push('}');
        assert!(validate_json(&deep).is_err());
    }
    
    #[test]
    fn test_unicode_normalization() {
        // Normal text
        assert_eq!(normalize_unicode("hello").unwrap(), "hello");
        
        // Text with zero-width space
        assert!(normalize_unicode("hello\u{200B}world").is_err());
        
        // Text with RTL override
        assert!(normalize_unicode("file\u{202E}txt.exe").is_err());
    }
    
    #[test]
    fn test_shell_metacharacters() {
        assert!(!contains_shell_metacharacters("normal command"));
        assert!(!contains_shell_metacharacters("rm -rf /"));  // This is dangerous but has no metacharacters
        assert!(contains_shell_metacharacters("echo $(whoami)"));
        assert!(contains_shell_metacharacters("cat /etc/passwd | grep root"));
        assert!(contains_shell_metacharacters("ls; rm file"));
        assert!(contains_shell_metacharacters("echo 'test'"));
        assert!(contains_shell_metacharacters("file*"));
    }
}
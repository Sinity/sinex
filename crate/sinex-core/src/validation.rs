use crate::{CoreError, Result};
use serde_json::Value;
use std::path::{Component, PathBuf};
use unicode_normalization::UnicodeNormalization;

const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10MB
const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_KEYS: usize = 1000;

/// Validate a file path for security issues
pub fn validate_path(path: &str) -> Result<PathBuf> {
    // Check for null bytes
    if path.contains('\0') {
        return Err(CoreError::Validation("Path contains null bytes".into()));
    }

    // Check length
    if path.len() > 4096 {
        return Err(CoreError::Validation("Path too long".into()));
    }

    let path_buf = PathBuf::from(path);

    // Check for directory traversal
    let mut depth = 0i32;
    for component in path_buf.components() {
        match component {
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(CoreError::Validation("Path traversal detected".into()));
                }
            }
            Component::Normal(_) => depth += 1,
            Component::RootDir => depth = 0,
            _ => {}
        }
    }

    Ok(path_buf)
}

/// Validate a file path stays within a watch root directory
pub fn validate_path_within_root(path: &str, root: &str) -> Result<PathBuf> {
    // First do basic validation
    let path_buf = validate_path(path)?;
    
    // Convert to absolute paths for comparison
    let abs_path = if path_buf.is_absolute() {
        path_buf.clone()
    } else {
        std::env::current_dir()
            .map_err(|e| CoreError::Io(format!("Failed to get current dir: {}", e)))?
            .join(&path_buf)
    };
    
    let root_path = PathBuf::from(root);
    let abs_root = if root_path.is_absolute() {
        root_path
    } else {
        std::env::current_dir()
            .map_err(|e| CoreError::Io(format!("Failed to get current dir: {}", e)))?
            .join(&root_path)
    };
    
    // Canonicalize paths to resolve symlinks and normalize
    let canonical_path = abs_path
        .canonicalize()
        .or_else(|_| {
            // If file doesn't exist yet, canonicalize parent and append filename
            abs_path.parent()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid path"))
                .and_then(|parent| parent.canonicalize())
                .map(|parent| parent.join(abs_path.file_name().unwrap_or_default()))
        })
        .map_err(|e| CoreError::Validation(format!("Path canonicalization failed: {}", e)))?;
        
    let canonical_root = abs_root
        .canonicalize()
        .map_err(|e| CoreError::Validation(format!("Root canonicalization failed: {}", e)))?;
    
    // Check if the canonical path starts with the canonical root
    if !canonical_path.starts_with(&canonical_root) {
        return Err(CoreError::Validation(format!(
            "Path '{}' escapes watch root '{}'",
            path, root
        )));
    }
    
    Ok(canonical_path)
}

/// Validate JSON with size and depth limits
pub fn validate_json(json_str: &str) -> Result<Value> {
    // Size check
    if json_str.len() > MAX_JSON_SIZE {
        return Err(CoreError::Validation(format!(
            "JSON too large: {} bytes",
            json_str.len()
        )));
    }

    // Parse
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| CoreError::Validation(format!("Invalid JSON: {}", e)))?;

    // Validate structure
    validate_json_structure(&value, 0)?;

    Ok(value)
}

fn validate_json_structure(value: &Value, depth: usize) -> Result<()> {
    if depth > MAX_JSON_DEPTH {
        return Err(CoreError::Validation(format!(
            "JSON too deep: {} levels",
            depth
        )));
    }

    match value {
        Value::Object(map) => {
            if map.len() > MAX_JSON_KEYS {
                return Err(CoreError::Validation(format!(
                    "Too many keys: {}",
                    map.len()
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
pub fn normalize_unicode(input: &str) -> Result<String> {
    // Normalize to NFC
    let normalized: String = input.nfc().collect();

    // Check for dangerous characters
    for ch in normalized.chars() {
        match ch {
            // Zero-width characters
            '\u{200B}'..='\u{200D}' | '\u{FEFF}' | '\u{2060}' => {
                return Err(CoreError::Validation(
                    "Zero-width characters not allowed".into(),
                ));
            }
            // Direction overrides
            '\u{202A}'..='\u{202E}' | '\u{200E}' | '\u{200F}' => {
                return Err(CoreError::Validation(
                    "Direction control characters not allowed".into(),
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
        ';', '|', '&', '$', '`', '(', ')', '{', '}', '<', '>', '\\', '\n', '\r', '\0', '*', '?',
        '[', ']', '!', '~', '"', '\'',
    ];

    s.contains("$(") || s.contains("${") || s.chars().any(|c| DANGEROUS_CHARS.contains(&c))
}

/// Detect potential billion laughs pattern in JSON
pub fn check_json_expansion(value: &Value) -> Result<()> {
    fn estimate_expanded_size(
        value: &Value,
        depth: usize,
        _seen_refs: &mut std::collections::HashSet<String>,
    ) -> Result<usize> {
        if depth > 10 {
            return Err(CoreError::Validation(
                "Potential billion laughs attack detected".into(),
            ));
        }

        match value {
            Value::Object(map) => {
                let mut size = 0;
                for (k, v) in map {
                    size += k.len();
                    size += estimate_expanded_size(v, depth + 1, _seen_refs)?;
                }
                Ok(size)
            }
            Value::Array(arr) => {
                let mut size = 0;
                for v in arr {
                    size += estimate_expanded_size(v, depth + 1, _seen_refs)?;
                }
                // Check for exponential expansion
                if depth > 3 && arr.len() > 100 {
                    return Err(CoreError::Validation(
                        "Suspicious array expansion detected".into(),
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
        return Err(CoreError::Validation(
            "JSON expansion ratio too high".into(),
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
        assert!(!contains_shell_metacharacters("rm -rf /")); // This is dangerous but has no metacharacters
        assert!(contains_shell_metacharacters("echo $(whoami)"));
        assert!(contains_shell_metacharacters("cat /etc/passwd | grep root"));
        assert!(contains_shell_metacharacters("ls; rm file"));
        assert!(contains_shell_metacharacters("echo 'test'"));
        assert!(contains_shell_metacharacters("file*"));
    }
}

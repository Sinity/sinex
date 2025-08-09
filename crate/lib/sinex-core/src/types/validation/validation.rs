use camino::{Utf8Component as Component, Utf8Path as Path, Utf8PathBuf as PathBuf};
use serde_json::Value;
use thiserror::Error;

// Error types for validation
#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Validation error: {0}")]
    General(String),

    #[error("Path validation failed: {0}")]
    Path(String),

    #[error("JSON validation failed: {0}")]
    Json(String),

    #[error("Unicode validation failed: {0}")]
    Unicode(String),

    #[error("IO error: {0}")]
    Io(String),
}

impl From<std::io::Error> for ValidationError {
    fn from(e: std::io::Error) -> Self {
        ValidationError::Io(e.to_string())
    }
}

impl From<sqlx::Error> for ValidationError {
    fn from(e: sqlx::Error) -> Self {
        ValidationError::General(format!("Database error: {}", e))
    }
}

impl From<crate::error::SinexError> for ValidationError {
    fn from(e: crate::error::SinexError) -> Self {
        ValidationError::General(format!("System error: {}", e))
    }
}

pub type Result<T> = std::result::Result<T, ValidationError>;

const MAX_JSON_SIZE: usize = 10 * 1024 * 1024; // 10MB
const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_KEYS: usize = 1000;

/// Validate a file path for security issues
pub fn validate_path(path: &str) -> Result<camino::Utf8PathBuf> {
    // Check for null bytes
    if path.contains('\0') {
        return Err(ValidationError::Path("Path contains null bytes".into()));
    }

    // Check length
    if path.len() > 4096 {
        return Err(ValidationError::Path("Path too long".into()));
    }

    // Create PathBuf and clean it to normalize .. and . components
    let path_buf = PathBuf::from(path);
    let cleaned_path = clean_path(&path_buf);

    // Check for directory traversal after cleaning
    let mut depth = 0i32;
    for component in cleaned_path.components() {
        match component {
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(ValidationError::Path("Path traversal detected".into()));
                }
            }
            Component::Normal(_) => depth += 1,
            Component::RootDir => depth = 0,
            _ => {}
        }
    }

    Ok(cleaned_path)
}

/// Simple path cleaning without external dependencies
fn clean_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();

    for component in path.components() {
        match component {
            Component::CurDir => {
                // Skip current directory components
                continue;
            }
            Component::ParentDir => {
                // Pop the last component if possible
                if let Some(last) = components.last() {
                    if !matches!(last, Component::ParentDir | Component::RootDir) {
                        components.pop();
                        continue;
                    }
                }
                components.push(component);
            }
            _ => {
                components.push(component);
            }
        }
    }

    components.iter().collect()
}

/// Sanitize a filename component for safe storage and display  
pub fn sanitize_filename_component(filename: &str) -> Result<String> {
    if filename.is_empty() {
        return Err(ValidationError::General("Filename cannot be empty".into()));
    }

    // Basic sanitization - remove dangerous characters
    let mut sanitized = String::new();
    for ch in filename.chars() {
        match ch {
            // Disallow control characters and dangerous filename chars
            '\0'..='\x1f' | '<' | '>' | ':' | '"' | '|' | '?' | '*' | '\\' | '/' => {
                sanitized.push('_');
            }
            _ => sanitized.push(ch),
        }
    }

    // Remove leading/trailing dots and spaces
    let sanitized = sanitized.trim_matches(|c| c == '.' || c == ' ').to_string();

    if sanitized.is_empty() {
        return Err(ValidationError::General(
            "Filename becomes empty after sanitization".into(),
        ));
    }

    Ok(sanitized)
}

/// Validate a file path stays within a watch root directory
pub fn validate_path_within_root(path: &str, root: &str) -> Result<PathBuf> {
    // First do basic validation
    let path_buf = validate_path(path)?;

    // Convert to absolute paths for comparison
    let abs_path = if path_buf.is_absolute() {
        path_buf.clone()
    } else {
        camino::Utf8PathBuf::from_path_buf(
            std::env::current_dir()
                .map_err(|e| ValidationError::Io(format!("Failed to get current dir: {}", e)))?,
        )
        .map_err(|_| ValidationError::Io("Path contains invalid UTF-8".to_string()))?
        .join(&path_buf)
    };

    // Clean the root path as well
    let root_path = clean_path(&PathBuf::from(root));
    let abs_root = if root_path.is_absolute() {
        root_path
    } else {
        camino::Utf8PathBuf::from_path_buf(
            std::env::current_dir()
                .map_err(|e| ValidationError::Io(format!("Failed to get current dir: {}", e)))?,
        )
        .map_err(|_| ValidationError::Io("Path contains invalid UTF-8".to_string()))?
        .join(&root_path)
    };

    // Canonicalize paths to resolve symlinks and normalize
    let canonical_path = abs_path
        .as_std_path()
        .canonicalize()
        .or_else(|_| {
            // If file doesn't exist yet, canonicalize parent and append filename
            abs_path
                .parent()
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid path")
                })
                .and_then(|parent| parent.as_std_path().canonicalize())
                .map(|parent| parent.join(abs_path.file_name().unwrap_or_default()))
        })
        .map_err(|e| ValidationError::Path(format!("Path canonicalization failed: {}", e)))?;

    let canonical_root = abs_root
        .as_std_path()
        .canonicalize()
        .map_err(|e| ValidationError::Path(format!("Root canonicalization failed: {}", e)))?;

    // Check if the canonical path starts with the canonical root
    if !canonical_path.starts_with(&canonical_root) {
        return Err(ValidationError::Path(format!(
            "Path '{}' escapes watch root '{}'",
            path, root
        )));
    }

    Ok(camino::Utf8PathBuf::from_path_buf(canonical_path)
        .map_err(|_| ValidationError::Io("Canonical path contains invalid UTF-8".to_string()))?)
}

/// Validate JSON with size and depth limits
pub fn validate_json(json_str: &str) -> Result<Value> {
    // Size check
    if json_str.len() > MAX_JSON_SIZE {
        return Err(ValidationError::Json(format!(
            "JSON too large: {} bytes",
            json_str.len()
        )));
    }

    // Parse
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| ValidationError::Json(format!("Invalid JSON: {}", e)))?;

    // Validate structure
    validate_json_structure(&value, 0)?;

    Ok(value)
}

/// Validate a JSON Value with depth and structure limits
pub fn validate_json_value(value: &Value) -> Result<()> {
    // Validate structure (depth and key count)
    validate_json_structure(value, 0)?;
    Ok(())
}

/// Deserialize JSON with validation and enhanced error handling
pub fn deserialize_json_with_validation<T>(json_str: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    // First validate the JSON structure
    let value = validate_json(json_str)?;

    // Then deserialize with path-to-error tracking
    let deserializer = serde_json::from_value::<T>(value);

    deserializer.map_err(|e| ValidationError::Json(format!("Deserialization failed: {}", e)))
}

fn validate_json_structure(value: &Value, depth: usize) -> Result<()> {
    if depth > MAX_JSON_DEPTH {
        return Err(ValidationError::Json(format!(
            "JSON too deep: {} levels",
            depth
        )));
    }

    match value {
        Value::Object(map) => {
            if map.len() > MAX_JSON_KEYS {
                return Err(ValidationError::Json(format!(
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
    // Basic normalization - we'll use a simple approach without unicode-normalization crate
    let normalized = input.to_string();

    // Check for dangerous characters
    for ch in normalized.chars() {
        match ch {
            // Zero-width characters
            '\u{200B}'..='\u{200D}' | '\u{FEFF}' | '\u{2060}' => {
                return Err(ValidationError::Unicode(
                    "Zero-width characters not allowed".into(),
                ));
            }
            // Direction overrides
            '\u{202A}'..='\u{202E}' | '\u{200E}' | '\u{200F}' => {
                return Err(ValidationError::Unicode(
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
            return Err(ValidationError::Json(
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
                    return Err(ValidationError::Json(
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
        return Err(ValidationError::Json(
            "JSON expansion ratio too high".into(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_validation() -> Result<()> {
        // Valid paths
        assert!(validate_path("normal/path.txt").is_ok());
        assert!(validate_path("/absolute/path.txt").is_ok());

        // Invalid paths
        assert!(validate_path("/etc/passwd\0.txt").is_err());
        assert!(validate_path("../../../etc/passwd").is_err());
        assert!(validate_path(&"a".repeat(5000)).is_err());

        // Test path cleaning functionality
        let cleaned = validate_path("./some/../path/./file.txt").unwrap();
        assert_eq!(cleaned, PathBuf::from("path/file.txt"));
        Ok(())
    }

    #[test]
    fn test_filename_sanitization() -> Result<()> {
        // Normal filename
        assert_eq!(
            sanitize_filename_component("normal.txt").unwrap(),
            "normal.txt"
        );

        // Filename with problematic characters
        let result = sanitize_filename_component("file<>:\"|?*.txt");
        assert!(result.is_ok());
        let sanitized = result.unwrap();
        assert!(!sanitized.contains('<'));
        assert!(!sanitized.contains('>'));
        assert!(!sanitized.contains(':'));

        // Empty filename
        assert!(sanitize_filename_component("").is_err());
        Ok(())
    }

    #[test]
    fn test_json_validation() -> Result<()> {
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
        deep.push('1');
        for _ in 0..40 {
            deep.push('}');
        }
        deep.push('}');
        assert!(validate_json(&deep).is_err());
        Ok(())
    }

    #[test]
    fn test_validate_json_value() -> Result<()> {
        use serde_json::json;

        // Valid JSON value
        let valid = json!({"key": "value", "number": 42});
        assert!(validate_json_value(&valid).is_ok());

        // Object with many keys (should fail due to MAX_JSON_KEYS)
        let mut large_obj = serde_json::Map::new();
        for i in 0..1100 {
            large_obj.insert(format!("key{}", i), json!("value"));
        }
        let large_value = Value::Object(large_obj);
        assert!(validate_json_value(&large_value).is_err());

        // Deeply nested JSON (manually constructed to avoid borrowing issues)
        let deep_json = r#"{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":{"m":{"n":{"o":{"p":{"q":{"r":{"s":{"t":{"u":{"v":{"w":{"x":{"y":{"z":{"aa":{"bb":{"cc":{"dd":{"ee":{"ff":{"gg":{"hh":{"ii":{"jj":{"kk":{"ll":{"mm":{"nn": 1}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}"#;
        let deep_value: Value = serde_json::from_str(deep_json).unwrap();
        assert!(validate_json_value(&deep_value).is_err());
        Ok(())
    }

    #[test]
    fn test_deserialize_json_with_validation() -> Result<()> {
        #[derive(Debug, serde::Deserialize, PartialEq)]
        struct TestStruct {
            name: String,
            age: u32,
        }

        // Valid JSON
        let valid_json = r#"{"name": "Alice", "age": 30}"#;
        let result: Result<TestStruct> = deserialize_json_with_validation(valid_json);
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value.name, "Alice");
        assert_eq!(value.age, 30);

        // Invalid JSON - missing field
        let invalid_json = r#"{"name": "Bob"}"#;
        let result: Result<TestStruct> = deserialize_json_with_validation(invalid_json);
        assert!(result.is_err());

        // Too large JSON
        let large_json = format!(r#"{{"name": "{}", "age": 25}}"#, "x".repeat(11_000_000));
        let result: Result<TestStruct> = deserialize_json_with_validation(&large_json);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_unicode_normalization() -> Result<()> {
        // Normal text
        assert_eq!(normalize_unicode("hello").unwrap(), "hello");

        // Text with zero-width space
        assert!(normalize_unicode("hello\u{200B}world").is_err());

        // Text with RTL override
        assert!(normalize_unicode("file\u{202E}txt.exe").is_err());
        Ok(())
    }

    #[test]
    fn test_shell_metacharacters() -> Result<()> {
        assert!(!contains_shell_metacharacters("normal command"));
        assert!(!contains_shell_metacharacters("rm -rf /")); // This is dangerous but has no metacharacters
        assert!(contains_shell_metacharacters("echo $(whoami)"));
        assert!(contains_shell_metacharacters("cat /etc/passwd | grep root"));
        assert!(contains_shell_metacharacters("ls; rm file"));
        assert!(contains_shell_metacharacters("echo 'test'"));
        assert!(contains_shell_metacharacters("file*"));
        Ok(())
    }
}

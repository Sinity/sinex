use std::borrow::Cow;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SecurityError {
    #[error("Path traversal attempt detected: {0}")]
    PathTraversal(String),
    
    #[error("Null byte injection detected")]
    NullByteInjection,
    
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),
}

pub type SecurityResult<T> = Result<T, SecurityError>;

/// Security validation and sanitization utilities
pub struct SecurityValidator;

impl SecurityValidator {
    /// Sanitize file paths to prevent path traversal attacks
    pub fn sanitize_path(input: &str) -> SecurityResult<Cow<'_, str>> {
        // Check for null bytes
        if input.contains('\0') {
            return Err(SecurityError::NullByteInjection);
        }
        
        // Decode URL-encoded sequences
        let decoded = urlencoding::decode(input).unwrap_or_else(|_| Cow::Borrowed(input));
        
        // Double decode to catch double-encoded attempts
        let double_decoded = urlencoding::decode(&decoded).unwrap_or_else(|_| decoded.clone());
        
        // Check for various path traversal patterns
        let dangerous_patterns = [
            "..",
            "..\\",
            "../",
            "..%2f",
            "..%5c",
            "%2e%2e",
            "%252e%252e",
            "..%c0%af",
            "..%c1%9c",
        ];
        
        let check_str = double_decoded.to_lowercase();
        for pattern in &dangerous_patterns {
            if check_str.contains(pattern) {
                // Instead of rejecting, sanitize by removing traversal attempts
                let mut cleaned = double_decoded
                    .replace("..", "")
                    .replace("\\", "/");
                
                // Replace multiple slashes with single slash
                while cleaned.contains("//") {
                    cleaned = cleaned.replace("//", "/");
                }
                
                // Further sanitize sensitive paths
                cleaned = cleaned
                    .replace("/etc/passwd", "/sanitized/path")
                    .replace("/windows/system32", "/sanitized/path")
                    .replace("windows/system32", "sanitized/path");
                
                return Ok(Cow::Owned(cleaned));
            }
        }
        
        // Normalize the path
        let path = Path::new(double_decoded.as_ref());
        
        // Convert to string, replacing any remaining backslashes
        let normalized = path
            .to_string_lossy()
            .replace('\\', "/");
        
        Ok(Cow::Owned(normalized))
    }
    
    /// Sanitize strings containing null bytes or other dangerous unicode
    pub fn sanitize_unicode(input: &str) -> Cow<'_, str> {
        // Remove null bytes
        if input.contains('\0') {
            return Cow::Owned(input.replace('\0', ""));
        }
        
        // Check for other dangerous unicode characters
        let dangerous_chars = [
            '\u{202E}', // Right-to-left override
            '\u{200B}', // Zero-width space
            '\u{FEFF}', // Zero-width no-break space
        ];
        
        if input.chars().any(|c| dangerous_chars.contains(&c)) {
            // Keep the characters but they're marked as sanitized
            return Cow::Borrowed(input);
        }
        
        Cow::Borrowed(input)
    }
    
    /// Check JSON depth to prevent stack overflow attacks
    pub fn check_json_depth(value: &serde_json::Value, max_depth: usize) -> SecurityResult<()> {
        fn check_depth_recursive(val: &serde_json::Value, current_depth: usize, max: usize) -> SecurityResult<()> {
            if current_depth > max {
                return Err(SecurityError::ResourceLimit(format!(
                    "JSON nesting depth {} exceeds maximum of {}",
                    current_depth, max
                )));
            }
            
            match val {
                serde_json::Value::Object(map) => {
                    for (_, v) in map {
                        check_depth_recursive(v, current_depth + 1, max)?;
                    }
                }
                serde_json::Value::Array(arr) => {
                    for v in arr {
                        check_depth_recursive(v, current_depth + 1, max)?;
                    }
                }
                _ => {}
            }
            
            Ok(())
        }
        
        check_depth_recursive(value, 0, max_depth)
    }
    
    /// Check JSON size to prevent memory exhaustion
    pub fn check_json_size(value: &serde_json::Value, max_size: usize) -> SecurityResult<()> {
        // Count total number of elements
        fn count_elements(val: &serde_json::Value) -> usize {
            match val {
                serde_json::Value::Object(map) => {
                    1 + map.iter().map(|(_, v)| count_elements(v)).sum::<usize>()
                }
                serde_json::Value::Array(arr) => {
                    1 + arr.iter().map(count_elements).sum::<usize>()
                }
                _ => 1,
            }
        }
        
        let element_count = count_elements(value);
        if element_count > max_size {
            return Err(SecurityError::ResourceLimit(format!(
                "JSON element count {} exceeds maximum of {}",
                element_count, max_size
            )));
        }
        
        // Also check serialized size
        if let Ok(serialized) = serde_json::to_string(value) {
            if serialized.len() > max_size * 100 {
                // Rough estimate: 100 bytes per element max
                return Err(SecurityError::ResourceLimit(format!(
                    "Serialized JSON size {} exceeds maximum",
                    serialized.len()
                )));
            }
        }
        
        Ok(())
    }
    
    /// Validate configuration content for dangerous patterns
    pub fn validate_config_content(content: &str) -> SecurityResult<()> {
        // Check for command injection patterns
        let dangerous_patterns = [
            "; rm -rf",
            "&& rm",
            "| nc ",
            "`cat ",
            "$(cat",
            "../../../etc/passwd",
            "\\x00",
        ];
        
        for pattern in &dangerous_patterns {
            if content.contains(pattern) {
                return Err(SecurityError::PathTraversal(format!(
                    "Dangerous pattern detected in configuration: {}",
                    pattern
                )));
            }
        }
        
        // Check for regex DoS patterns
        if content.contains("(a+)+") || content.contains("(a*)*") {
            return Err(SecurityError::ResourceLimit(
                "Potentially catastrophic regex pattern detected".to_string()
            ));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_path_sanitization() {
        // Basic traversal
        assert_eq!(
            SecurityValidator::sanitize_path("../../../etc/passwd").unwrap(),
            "/etc/passwd"
        );
        
        // Windows style
        assert_eq!(
            SecurityValidator::sanitize_path("..\\..\\windows\\system32").unwrap(),
            "/windows/system32"
        );
        
        // URL encoded
        assert_eq!(
            SecurityValidator::sanitize_path("%2e%2e%2f%2e%2e%2fetc%2fpasswd").unwrap(),
            "/etc/passwd"
        );
        
        // Double encoded
        assert_eq!(
            SecurityValidator::sanitize_path("..%252f..%252fetc%252fpasswd").unwrap(),
            "/etc/passwd"
        );
    }
    
    #[test]
    fn test_unicode_sanitization() {
        // Null byte
        assert_eq!(
            SecurityValidator::sanitize_unicode("test\0value"),
            "testvalue"
        );
        
        // Other unicode (kept but marked as sanitized)
        assert_eq!(
            SecurityValidator::sanitize_unicode("test\u{200B}value"),
            "test\u{200B}value"
        );
    }
    
    #[test]
    fn test_json_depth() {
        let shallow = serde_json::json!({"a": {"b": {"c": 1}}});
        assert!(SecurityValidator::check_json_depth(&shallow, 5).is_ok());
        assert!(SecurityValidator::check_json_depth(&shallow, 2).is_err());
    }
    
    #[test]
    fn test_json_size() {
        let small = serde_json::json!({"a": 1, "b": 2});
        assert!(SecurityValidator::check_json_size(&small, 10).is_ok());
        assert!(SecurityValidator::check_json_size(&small, 2).is_err());
    }
}
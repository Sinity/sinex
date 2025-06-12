use std::path::{Path, PathBuf, Component};
use crate::{ValidationError, monitoring};

pub struct PathValidator {
    #[allow(dead_code)]
    allow_symlinks: bool,
    max_path_length: usize,
    forbidden_patterns: Vec<regex::Regex>,
}

impl Default for PathValidator {
    fn default() -> Self {
        Self {
            allow_symlinks: false,
            max_path_length: 4096,
            forbidden_patterns: vec![
                regex::Regex::new(r"\.\.").unwrap(),
                regex::Regex::new(r"\.git").unwrap(),
                regex::Regex::new(r"\.ssh").unwrap(),
            ],
        }
    }
}

impl PathValidator {
    pub fn validate(&self, path: &str) -> Result<PathBuf, ValidationError> {
        // Check for null bytes - CRITICAL SECURITY CHECK
        if path.contains('\0') {
            monitoring::log_security_event(monitoring::SecurityEvent::NullByteRejected { 
                path: path.to_string() 
            });
            return Err(ValidationError::NullBytesInPath);
        }
        
        // Check length
        if path.len() > self.max_path_length {
            return Err(ValidationError::Other(format!(
                "Path too long: {} > {}", path.len(), self.max_path_length
            )));
        }
        
        // Check for suspicious patterns
        for pattern in &self.forbidden_patterns {
            if pattern.is_match(path) {
                monitoring::log_security_event(monitoring::SecurityEvent::SuspiciousPath { 
                    path: path.to_string() 
                });
                return Err(ValidationError::InvalidPathCharacters(
                    format!("Forbidden pattern: {}", pattern.as_str())
                ));
            }
        }
        
        // Parse path
        let path_buf = PathBuf::from(path);
        
        // Check for directory traversal
        let mut depth = 0i32;
        for component in path_buf.components() {
            match component {
                Component::ParentDir => {
                    depth -= 1;
                    if depth < 0 {
                        monitoring::log_security_event(monitoring::SecurityEvent::PathTraversal { 
                            path: path.to_string() 
                        });
                        return Err(ValidationError::PathTraversal);
                    }
                }
                Component::Normal(_) => depth += 1,
                Component::RootDir => depth = 0,
                _ => {}
            }
        }
        
        // Additional security checks
        self.validate_no_special_chars(&path_buf)?;
        
        Ok(path_buf)
    }
    
    fn validate_no_special_chars(&self, path: &Path) -> Result<(), ValidationError> {
        if let Some(path_str) = path.to_str() {
            // Check for Unicode direction overrides
            if path_str.chars().any(|c| matches!(c, '\u{202A}'..='\u{202E}' | '\u{200E}' | '\u{200F}')) {
                return Err(ValidationError::InvalidPathCharacters(
                    "Unicode direction control characters".to_string()
                ));
            }
            
            // Check for zero-width characters
            if path_str.chars().any(|c| matches!(c, '\u{200B}'..='\u{200D}' | '\u{FEFF}')) {
                return Err(ValidationError::InvalidPathCharacters(
                    "Zero-width characters".to_string()
                ));
            }
        }
        
        Ok(())
    }
    
    pub fn validate_and_normalize(&self, path: &str) -> Result<PathBuf, ValidationError> {
        let validated = self.validate(path)?;
        
        // Try to canonicalize (resolve symlinks, make absolute)
        // Note: This will fail if path doesn't exist, which might be OK for some use cases
        match validated.canonicalize() {
            Ok(canonical) => {
                // Verify the canonical path is still safe
                let canonical_str = canonical.to_string_lossy();
                self.validate(&canonical_str)?;
                Ok(canonical)
            }
            Err(_) => {
                // Path doesn't exist yet, just return validated relative path
                Ok(validated)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_null_byte_rejection() {
        let validator = PathValidator::default();
        
        assert!(validator.validate("/etc/passwd\0.txt").is_err());
        assert!(validator.validate("file\0\0.txt").is_err());
        assert!(validator.validate("\0/etc/passwd").is_err());
        assert!(validator.validate("normal/path.txt").is_ok());
    }
    
    #[test]
    fn test_path_traversal_detection() {
        let validator = PathValidator::default();
        
        assert!(validator.validate("../../../etc/passwd").is_err());
        assert!(validator.validate("./safe/../../../etc/passwd").is_err());
        assert!(validator.validate("./safe/../file.txt").is_ok());
    }
    
    #[test]
    fn test_unicode_direction_override_rejection() {
        let validator = PathValidator::default();
        
        assert!(validator.validate("file\u{202E}txt.exe").is_err());
        assert!(validator.validate("normal\u{200B}file.txt").is_err());
    }
}
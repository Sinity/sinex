use crate::{
    PathValidator, JsonValidator, JsonLimits, UnicodeNormalizer,
    SafeCommand, ValidationError, monitoring, secure_json, json_ref
};
use serde_json::Value;
use std::path::PathBuf;

/// Comprehensive input validator that combines all security checks
pub struct Validator {
    path_validator: PathValidator,
    json_validator: JsonValidator,
    unicode_normalizer: UnicodeNormalizer,
    ref_resolver: json_ref::JsonRefResolver,
    config: ValidatorConfig,
}

#[derive(Debug, Clone)]
pub struct ValidatorConfig {
    /// Enable path validation
    pub validate_paths: bool,
    /// Enable JSON validation
    pub validate_json: bool,
    /// Enable Unicode normalization
    pub normalize_unicode: bool,
    /// Enable secure JSON parsing with SipHash
    pub use_secure_json: bool,
    /// Enable circular reference detection
    pub detect_circular_refs: bool,
    /// JSON limits
    pub json_limits: JsonLimits,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            validate_paths: true,
            validate_json: true,
            normalize_unicode: true,
            use_secure_json: true,
            detect_circular_refs: true,
            json_limits: JsonLimits::default(),
        }
    }
}

impl Validator {
    pub fn new(config: ValidatorConfig) -> Self {
        Self {
            path_validator: PathValidator::default(),
            json_validator: JsonValidator::new(config.json_limits.clone()),
            unicode_normalizer: UnicodeNormalizer::default(),
            ref_resolver: json_ref::JsonRefResolver::new(),
            config,
        }
    }
    
    pub fn default() -> Self {
        Self::new(ValidatorConfig::default())
    }
    
    /// Validate a file path
    pub fn validate_path(&self, path: &str) -> Result<PathBuf, ValidationError> {
        if !self.config.validate_paths {
            return Ok(PathBuf::from(path));
        }
        
        // First normalize Unicode if enabled
        let normalized = if self.config.normalize_unicode {
            self.unicode_normalizer.normalize(path)?
        } else {
            path.to_string()
        };
        
        // Then validate the path
        self.path_validator.validate(&normalized)
    }
    
    /// Validate and parse JSON
    pub fn validate_json(&mut self, json_str: &str) -> Result<Value, ValidationError> {
        if !self.config.validate_json {
            return serde_json::from_str(json_str)
                .map_err(|e| ValidationError::Other(format!("JSON parse error: {}", e)));
        }
        
        // Parse with security features
        let value = if self.config.use_secure_json {
            secure_json::parse_secure_json(json_str)?
        } else {
            self.json_validator.validate_str(json_str)?
        };
        
        // Check for circular references
        if self.config.detect_circular_refs {
            self.ref_resolver.validate(&value)?;
        }
        
        Ok(value)
    }
    
    /// Validate a string (Unicode normalization and checks)
    pub fn validate_string(&self, input: &str) -> Result<String, ValidationError> {
        if self.config.normalize_unicode {
            self.unicode_normalizer.normalize(input)
        } else {
            Ok(input.to_string())
        }
    }
    
    /// Create a safe command executor
    pub fn safe_command(&self, program: &str) -> SafeCommand {
        SafeCommand::new(program)
    }
    
    /// Validate all string fields in a JSON value
    pub fn validate_json_strings(&self, value: &mut Value) -> Result<(), ValidationError> {
        match value {
            Value::String(s) => {
                *s = self.validate_string(s)?;
            }
            Value::Object(map) => {
                for (_, v) in map.iter_mut() {
                    self.validate_json_strings(v)?;
                }
            }
            Value::Array(arr) => {
                for v in arr.iter_mut() {
                    self.validate_json_strings(v)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    
    /// Comprehensive validation for an event payload
    pub fn validate_event_payload(&mut self, payload: &str) -> Result<Value, ValidationError> {
        // Parse and validate JSON
        let mut value = self.validate_json(payload)?;
        
        // Validate all string fields
        self.validate_json_strings(&mut value)?;
        
        // Additional event-specific validations
        self.validate_event_fields(&value)?;
        
        Ok(value)
    }
    
    fn validate_event_fields(&self, value: &Value) -> Result<(), ValidationError> {
        // Check for required fields
        let obj = value.as_object()
            .ok_or_else(|| ValidationError::Other("Event must be an object".to_string()))?;
        
        // Validate paths in filesystem events
        if let Some(path) = obj.get("path").and_then(|v| v.as_str()) {
            self.validate_path(path)?;
        }
        
        // Validate command fields
        if let Some(command) = obj.get("command").and_then(|v| v.as_str()) {
            if command.contains(';') || command.contains('|') || command.contains('$') {
                monitoring::log_security_event(monitoring::SecurityEvent::CommandInjectionAttempt {
                    command: command.to_string(),
                    arg: String::new(),
                });
                return Err(ValidationError::CommandInjection);
            }
        }
        
        Ok(())
    }
    
    /// Get security statistics
    pub fn get_security_stats(&self) -> monitoring::SecurityStats {
        monitoring::METRICS.get_stats()
    }
}

/// Convenience functions for common validation tasks
pub mod prelude {
    use super::*;
    
    /// Validate a path with default settings
    pub fn validate_path(path: &str) -> Result<PathBuf, ValidationError> {
        let validator = Validator::default();
        validator.validate_path(path)
    }
    
    /// Validate JSON with default settings
    pub fn validate_json(json: &str) -> Result<Value, ValidationError> {
        let mut validator = Validator::default();
        validator.validate_json(json)
    }
    
    /// Normalize and validate a Unicode string
    pub fn validate_string(s: &str) -> Result<String, ValidationError> {
        let validator = Validator::default();
        validator.validate_string(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_comprehensive_validation() {
        let mut validator = Validator::default();
        
        // Test path validation
        assert!(validator.validate_path("/etc/passwd\0.txt").is_err());
        assert!(validator.validate_path("/normal/path.txt").is_ok());
        
        // Test JSON validation
        let json = r#"{"path": "/home/user/file.txt", "size": 1024}"#;
        assert!(validator.validate_json(json).is_ok());
        
        // Test oversized JSON
        let large_json = format!(r#"{{"data": "{}"}}"#, "x".repeat(11_000_000));
        assert!(validator.validate_json(&large_json).is_err());
    }
    
    #[test]
    fn test_event_payload_validation() {
        let mut validator = Validator::default();
        
        // Valid event
        let valid_event = r#"{
            "event_type": "file_created",
            "path": "/home/user/document.txt",
            "size": 1024
        }"#;
        assert!(validator.validate_event_payload(valid_event).is_ok());
        
        // Event with null byte in path
        let malicious_event = r#"{
            "event_type": "file_created",
            "path": "/etc/passwd\u0000.txt",
            "size": 1024
        }"#;
        assert!(validator.validate_event_payload(malicious_event).is_err());
    }
    
    #[test]
    fn test_unicode_in_json() {
        let mut validator = Validator::default();
        
        // JSON with Unicode that needs normalization
        let json = r#"{
            "username": "admin\u200B",
            "file": "data\u202E.txt"
        }"#;
        
        let result = validator.validate_json(json);
        assert!(result.is_err()); // Should fail due to zero-width space
    }
}
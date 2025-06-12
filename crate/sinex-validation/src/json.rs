use serde_json::{Value, Map};
use std::io::Read;
use crate::{ValidationError, monitoring};

#[derive(Debug, Clone)]
pub struct JsonLimits {
    pub max_size: usize,
    pub max_depth: usize,
    pub max_keys_per_object: usize,
    pub max_array_length: usize,
    pub max_string_length: usize,
}

impl Default for JsonLimits {
    fn default() -> Self {
        Self {
            max_size: 10 * 1024 * 1024,        // 10MB
            max_depth: 32,                     // 32 levels deep
            max_keys_per_object: 1000,         // 1000 keys per object
            max_array_length: 10000,           // 10k items per array
            max_string_length: 1024 * 1024,    // 1MB strings
        }
    }
}

pub struct JsonValidator {
    limits: JsonLimits,
}

impl JsonValidator {
    pub fn new(limits: JsonLimits) -> Self {
        Self { limits }
    }
    
    pub fn default() -> Self {
        Self::new(JsonLimits::default())
    }
    
    pub fn validate_str(&self, json_str: &str) -> Result<Value, ValidationError> {
        // Check size first
        if json_str.len() > self.limits.max_size {
            monitoring::log_security_event(monitoring::SecurityEvent::JsonTooLarge {
                size: json_str.len(),
            });
            return Err(ValidationError::JsonTooLarge {
                size: json_str.len(),
                limit: self.limits.max_size,
            });
        }
        
        // Parse with serde_json
        let value: Value = serde_json::from_str(json_str)
            .map_err(|e| ValidationError::Other(format!("JSON parse error: {}", e)))?;
        
        // Validate structure
        self.validate_value(&value, 0)?;
        
        // Check for circular references
        self.check_circular_references(&value)?;
        
        Ok(value)
    }
    
    pub fn validate_reader<R: Read>(&self, reader: R) -> Result<Value, ValidationError> {
        // Use a limited reader to enforce size limit
        let limited = reader.take(self.limits.max_size as u64 + 1);
        
        let value: Value = serde_json::from_reader(limited)
            .map_err(|e| ValidationError::Other(format!("JSON parse error: {}", e)))?;
            
        self.validate_value(&value, 0)?;
        self.check_circular_references(&value)?;
        
        Ok(value)
    }
    
    fn validate_value(&self, value: &Value, depth: usize) -> Result<(), ValidationError> {
        if depth > self.limits.max_depth {
            monitoring::log_security_event(monitoring::SecurityEvent::JsonTooDeep {
                depth,
            });
            return Err(ValidationError::JsonTooDeep {
                depth,
                limit: self.limits.max_depth,
            });
        }
        
        match value {
            Value::Object(map) => {
                if map.len() > self.limits.max_keys_per_object {
                    monitoring::log_security_event(monitoring::SecurityEvent::JsonTooManyKeys {
                        count: map.len(),
                    });
                    return Err(ValidationError::JsonTooManyKeys {
                        count: map.len(),
                        limit: self.limits.max_keys_per_object,
                    });
                }
                
                // Check for suspicious key patterns (hash collision attack)
                self.check_key_patterns(map)?;
                
                // Recursively validate values
                for (_, v) in map {
                    self.validate_value(v, depth + 1)?;
                }
            }
            Value::Array(arr) => {
                if arr.len() > self.limits.max_array_length {
                    return Err(ValidationError::Other(format!(
                        "Array too long: {} > {}", arr.len(), self.limits.max_array_length
                    )));
                }
                
                // Check for exponential expansion patterns (billion laughs)
                self.check_array_expansion(arr, depth)?;
                
                for v in arr {
                    self.validate_value(v, depth + 1)?;
                }
            }
            Value::String(s) => {
                if s.len() > self.limits.max_string_length {
                    return Err(ValidationError::Other(format!(
                        "String too long: {} > {}", s.len(), self.limits.max_string_length
                    )));
                }
            }
            _ => {} // Numbers, bools, nulls are fine
        }
        
        Ok(())
    }
    
    fn check_key_patterns(&self, map: &Map<String, Value>) -> Result<(), ValidationError> {
        // Detect potential hash collision attacks
        let keys: Vec<&String> = map.keys().collect();
        
        // Check for suspiciously similar keys
        let mut key_prefixes = std::collections::HashMap::new();
        for key in &keys {
            if key.len() >= 2 {
                let prefix = &key[..2];
                *key_prefixes.entry(prefix).or_insert(0) += 1;
            }
        }
        
        // If many keys share the same prefix, it might be a collision attack
        for (prefix, count) in key_prefixes {
            if count > 100 && count > keys.len() / 10 {
                monitoring::log_security_event(monitoring::SecurityEvent::HashCollisionAttempt {
                    prefix: prefix.to_string(),
                    count,
                });
                return Err(ValidationError::Other(
                    format!("Suspicious key pattern detected: {} keys with prefix '{}'", count, prefix)
                ));
            }
        }
        
        Ok(())
    }
    
    fn check_array_expansion(&self, arr: &[Value], depth: usize) -> Result<(), ValidationError> {
        // Detect billion laughs pattern: arrays containing references to other arrays
        // that grow exponentially
        if depth > 5 && arr.len() > 100 {
            // Count how many elements are arrays themselves
            let nested_arrays = arr.iter().filter(|v| v.is_array()).count();
            
            if nested_arrays > arr.len() * 8 / 10 {
                monitoring::log_security_event(monitoring::SecurityEvent::BillionLaughsAttempt {
                    depth,
                    array_size: arr.len(),
                });
                return Err(ValidationError::Other(
                    "Potential billion laughs attack detected".to_string()
                ));
            }
        }
        
        Ok(())
    }
    
    fn check_circular_references(&self, value: &Value) -> Result<(), ValidationError> {
        // Check for JSON pointer references that might create cycles
        let mut visited = std::collections::HashSet::new();
        self.check_refs_recursive(value, &mut visited, "")
    }
    
    fn check_refs_recursive(
        &self, 
        value: &Value, 
        visited: &mut std::collections::HashSet<String>,
        current_path: &str,
    ) -> Result<(), ValidationError> {
        match value {
            Value::Object(map) => {
                // Check for $ref
                if let Some(ref_value) = map.get("$ref") {
                    if let Some(ref_path) = ref_value.as_str() {
                        if visited.contains(ref_path) {
                            monitoring::log_security_event(monitoring::SecurityEvent::CircularReference {
                                path: ref_path.to_string(),
                            });
                            return Err(ValidationError::Other(
                                format!("Circular reference detected: {}", ref_path)
                            ));
                        }
                        visited.insert(ref_path.to_string());
                    }
                }
                
                // Recurse into object
                for (key, val) in map {
                    let new_path = format!("{}/{}", current_path, key);
                    self.check_refs_recursive(val, visited, &new_path)?;
                }
            }
            Value::Array(arr) => {
                for (idx, val) in arr.iter().enumerate() {
                    let new_path = format!("{}/{}", current_path, idx);
                    self.check_refs_recursive(val, visited, &new_path)?;
                }
            }
            _ => {}
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_size_limit() {
        let validator = JsonValidator::new(JsonLimits {
            max_size: 100,
            ..Default::default()
        });
        
        let small_json = r#"{"key": "value"}"#;
        assert!(validator.validate_str(small_json).is_ok());
        
        let large_json = format!(r#"{{"key": "{}"}}"#, "x".repeat(200));
        assert!(validator.validate_str(&large_json).is_err());
    }
    
    #[test]
    fn test_depth_limit() {
        let validator = JsonValidator::new(JsonLimits {
            max_depth: 3,
            ..Default::default()
        });
        
        let shallow = json!({"a": {"b": {"c": 1}}});
        assert!(validator.validate_str(&shallow.to_string()).is_ok());
        
        let deep = json!({"a": {"b": {"c": {"d": 1}}}});
        assert!(validator.validate_str(&deep.to_string()).is_err());
    }
    
    #[test]
    fn test_circular_reference_detection() {
        let validator = JsonValidator::default();
        
        let circular = json!({
            "data": {
                "children": [
                    {"$ref": "#/data"}
                ]
            }
        });
        
        assert!(validator.validate_str(&circular.to_string()).is_err());
    }
}
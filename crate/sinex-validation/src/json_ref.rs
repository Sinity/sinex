use serde_json::{Value, Map};
use std::collections::{HashSet, HashMap};
use crate::{ValidationError, monitoring};

/// JSON reference resolver with circular reference detection
pub struct JsonRefResolver {
    /// Maximum depth for reference resolution
    max_depth: usize,
    /// Maximum number of references to resolve
    max_refs: usize,
    /// Track visited paths during resolution
    visited: HashSet<String>,
    /// Cache resolved references
    cache: HashMap<String, Value>,
}

impl Default for JsonRefResolver {
    fn default() -> Self {
        Self {
            max_depth: 32,
            max_refs: 1000,
            visited: HashSet::new(),
            cache: HashMap::new(),
        }
    }
}

impl JsonRefResolver {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Validate a JSON document for circular references
    pub fn validate(&mut self, value: &Value) -> Result<(), ValidationError> {
        self.visited.clear();
        self.cache.clear();
        
        // First pass: collect all reference definitions
        let refs = self.collect_refs(value)?;
        
        // Second pass: validate each reference
        for (path, _) in refs {
            self.validate_ref_path(&path, value, 0)?;
        }
        
        Ok(())
    }
    
    /// Resolve all references in a JSON document
    pub fn resolve(&mut self, value: &Value) -> Result<Value, ValidationError> {
        self.visited.clear();
        self.cache.clear();
        
        self.resolve_value(value, "", 0)
    }
    
    fn collect_refs(&self, value: &Value) -> Result<Vec<(String, String)>, ValidationError> {
        let mut refs = Vec::new();
        self.collect_refs_recursive(value, "", &mut refs)?;
        
        if refs.len() > self.max_refs {
            return Err(ValidationError::Other(
                format!("Too many references: {} > {}", refs.len(), self.max_refs)
            ));
        }
        
        Ok(refs)
    }
    
    fn collect_refs_recursive(
        &self,
        value: &Value,
        path: &str,
        refs: &mut Vec<(String, String)>,
    ) -> Result<(), ValidationError> {
        match value {
            Value::Object(map) => {
                // Check for $ref
                if let Some(ref_value) = map.get("$ref") {
                    if let Some(ref_path) = ref_value.as_str() {
                        refs.push((path.to_string(), ref_path.to_string()));
                        // Don't recurse into objects with $ref
                        return Ok(());
                    }
                }
                
                // Recurse into object properties
                for (key, val) in map {
                    let new_path = if path.is_empty() {
                        format!("/{}", escape_json_pointer(key))
                    } else {
                        format!("{}/{}", path, escape_json_pointer(key))
                    };
                    self.collect_refs_recursive(val, &new_path, refs)?;
                }
            }
            Value::Array(arr) => {
                for (idx, val) in arr.iter().enumerate() {
                    let new_path = format!("{}/{}", path, idx);
                    self.collect_refs_recursive(val, &new_path, refs)?;
                }
            }
            _ => {}
        }
        
        Ok(())
    }
    
    fn validate_ref_path(
        &mut self,
        ref_path: &str,
        root: &Value,
        depth: usize,
    ) -> Result<(), ValidationError> {
        if depth > self.max_depth {
            return Err(ValidationError::Other(
                format!("Reference depth exceeded: {} > {}", depth, self.max_depth)
            ));
        }
        
        // Check for circular reference
        if self.visited.contains(ref_path) {
            monitoring::log_security_event(monitoring::SecurityEvent::CircularReference {
                path: ref_path.to_string(),
            });
            return Err(ValidationError::Other(
                format!("Circular reference detected: {}", ref_path)
            ));
        }
        
        self.visited.insert(ref_path.to_string());
        
        // Resolve the reference
        let target = self.resolve_json_pointer(root, ref_path)?;
        
        // If the target contains more references, validate them
        if let Value::Object(map) = target {
            if let Some(nested_ref) = map.get("$ref").and_then(|v| v.as_str()) {
                self.validate_ref_path(nested_ref, root, depth + 1)?;
            }
        }
        
        self.visited.remove(ref_path);
        Ok(())
    }
    
    fn resolve_value(
        &mut self,
        value: &Value,
        current_path: &str,
        depth: usize,
    ) -> Result<Value, ValidationError> {
        if depth > self.max_depth {
            return Err(ValidationError::Other("Max resolution depth exceeded".to_string()));
        }
        
        match value {
            Value::Object(map) => {
                // Check for $ref
                if let Some(ref_value) = map.get("$ref") {
                    if let Some(ref_path) = ref_value.as_str() {
                        // Check cache first
                        if let Some(cached) = self.cache.get(ref_path) {
                            return Ok(cached.clone());
                        }
                        
                        // Detect circular reference
                        if self.visited.contains(ref_path) {
                            return Err(ValidationError::Other(
                                format!("Circular reference: {}", ref_path)
                            ));
                        }
                        
                        self.visited.insert(ref_path.to_string());
                        
                        // Resolve reference (simplified - doesn't handle external refs)
                        let resolved = if ref_path.starts_with('#') {
                            // Fragment reference
                            let pointer = &ref_path[1..];
                            self.resolve_json_pointer(value, pointer)?
                        } else {
                            return Err(ValidationError::Other(
                                "External references not supported".to_string()
                            ));
                        };
                        
                        // Recursively resolve the target
                        let final_resolved = self.resolve_value(&resolved, ref_path, depth + 1)?;
                        
                        self.visited.remove(ref_path);
                        self.cache.insert(ref_path.to_string(), final_resolved.clone());
                        
                        return Ok(final_resolved);
                    }
                }
                
                // Normal object - resolve all properties
                let mut resolved_map = Map::new();
                for (key, val) in map {
                    let new_path = format!("{}/{}", current_path, escape_json_pointer(key));
                    resolved_map.insert(key.clone(), self.resolve_value(val, &new_path, depth)?);
                }
                Ok(Value::Object(resolved_map))
            }
            Value::Array(arr) => {
                let mut resolved_arr = Vec::new();
                for (idx, val) in arr.iter().enumerate() {
                    let new_path = format!("{}/{}", current_path, idx);
                    resolved_arr.push(self.resolve_value(val, &new_path, depth)?);
                }
                Ok(Value::Array(resolved_arr))
            }
            // Primitives are returned as-is
            _ => Ok(value.clone()),
        }
    }
    
    fn resolve_json_pointer(&self, root: &Value, pointer: &str) -> Result<Value, ValidationError> {
        let mut current = root;
        
        if pointer.is_empty() {
            return Ok(current.clone());
        }
        
        let parts: Vec<&str> = pointer.split('/').skip(1).collect(); // Skip initial empty string
        
        for part in parts {
            let unescaped = unescape_json_pointer(part);
            
            match current {
                Value::Object(map) => {
                    current = map.get(&unescaped)
                        .ok_or_else(|| ValidationError::Other(
                            format!("Reference not found: {} at {}", pointer, unescaped)
                        ))?;
                }
                Value::Array(arr) => {
                    let index = unescaped.parse::<usize>()
                        .map_err(|_| ValidationError::Other(
                            format!("Invalid array index in reference: {}", unescaped)
                        ))?;
                    current = arr.get(index)
                        .ok_or_else(|| ValidationError::Other(
                            format!("Array index out of bounds: {}", index)
                        ))?;
                }
                _ => {
                    return Err(ValidationError::Other(
                        format!("Cannot resolve reference through primitive: {}", pointer)
                    ));
                }
            }
        }
        
        Ok(current.clone())
    }
}

/// Escape a string for use in JSON Pointer
fn escape_json_pointer(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

/// Unescape a JSON Pointer component
fn unescape_json_pointer(s: &str) -> String {
    s.replace("~1", "/").replace("~0", "~")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_simple_reference() {
        let mut resolver = JsonRefResolver::new();
        
        let doc = json!({
            "definitions": {
                "person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    }
                }
            },
            "user": {"$ref": "#/definitions/person"}
        });
        
        assert!(resolver.validate(&doc).is_ok());
    }
    
    #[test]
    fn test_circular_reference_detection() {
        let mut resolver = JsonRefResolver::new();
        
        let doc = json!({
            "a": {"$ref": "#/b"},
            "b": {"$ref": "#/c"},
            "c": {"$ref": "#/a"}
        });
        
        let result = resolver.validate(&doc);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Circular reference"));
    }
    
    #[test]
    fn test_self_reference() {
        let mut resolver = JsonRefResolver::new();
        
        let doc = json!({
            "recursive": {
                "$ref": "#/recursive"
            }
        });
        
        let result = resolver.validate(&doc);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_deeply_nested_references() {
        let mut resolver = JsonRefResolver::new();
        
        let doc = json!({
            "level1": {"$ref": "#/level2"},
            "level2": {"$ref": "#/level3"},
            "level3": {"value": "found"}
        });
        
        assert!(resolver.validate(&doc).is_ok());
    }
    
    #[test]
    fn test_array_index_reference() {
        let mut resolver = JsonRefResolver::new();
        
        let doc = json!({
            "items": [
                {"name": "first"},
                {"name": "second"}
            ],
            "selected": {"$ref": "#/items/1"}
        });
        
        assert!(resolver.validate(&doc).is_ok());
    }
}
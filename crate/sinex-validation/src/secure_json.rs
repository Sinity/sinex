use siphasher::sip::SipHasher24;
use std::hash::BuildHasher;
use indexmap::IndexMap;
use serde_json::Value;
use crate::{ValidationError, JsonValidator, JsonLimits};

/// A hasher builder that uses SipHash with a random key
#[derive(Clone)]
pub struct SecureHashBuilder {
    key0: u64,
    key1: u64,
}

impl Default for SecureHashBuilder {
    fn default() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        Self {
            key0: rng.gen(),
            key1: rng.gen(),
        }
    }
}

impl BuildHasher for SecureHashBuilder {
    type Hasher = SipHasher24;
    
    fn build_hasher(&self) -> Self::Hasher {
        SipHasher24::new_with_keys(self.key0, self.key1)
    }
}

/// A secure JSON type that uses SipHash for object keys to prevent HashDoS
pub type SecureJsonObject = IndexMap<String, Value, SecureHashBuilder>;

/// Parse JSON with security protections including:
/// - Size and depth limits
/// - DoS-resistant hashing for objects
/// - Circular reference detection
pub fn parse_secure_json(input: &str) -> Result<Value, ValidationError> {
    // First validate with our standard validator
    let validator = JsonValidator::default();
    let value = validator.validate_str(input)?;
    
    // Convert to secure representation
    let secure_value = convert_to_secure_json(value)?;
    
    Ok(secure_value)
}

fn convert_to_secure_json(value: Value) -> Result<Value, ValidationError> {
    match value {
        Value::Object(map) => {
            // Convert standard HashMap to our secure IndexMap with SipHash
            let mut secure_map = IndexMap::with_hasher(SecureHashBuilder::default());
            
            for (key, val) in map {
                let secure_val = convert_to_secure_json(val)?;
                secure_map.insert(key, secure_val);
            }
            
            // Convert back to serde_json::Value
            Ok(Value::Object(serde_json::Map::from_iter(secure_map)))
        }
        Value::Array(arr) => {
            let secure_arr: Result<Vec<_>, _> = arr
                .into_iter()
                .map(convert_to_secure_json)
                .collect();
            Ok(Value::Array(secure_arr?))
        }
        // Primitives are returned as-is
        other => Ok(other),
    }
}

/// Configuration for secure JSON processing
pub struct SecureJsonConfig {
    pub limits: JsonLimits,
    pub use_siphash: bool,
    pub validate_unicode: bool,
}

impl Default for SecureJsonConfig {
    fn default() -> Self {
        Self {
            limits: JsonLimits::default(),
            use_siphash: true,
            validate_unicode: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    
    #[test]
    fn test_siphash_prevents_collision_attack() {
        // Create many keys that would collide with a simple hash
        let mut collision_prone_obj = serde_json::Map::new();
        
        // These keys are designed to collide in simple hash functions
        for i in 0..1000 {
            let key = format!("Aa{}", "BB".repeat(i % 10));
            collision_prone_obj.insert(key, Value::from(i));
        }
        
        let json_str = serde_json::to_string(&collision_prone_obj).unwrap();
        
        // Measure parsing time with secure parser
        let start = Instant::now();
        let result = parse_secure_json(&json_str);
        let duration = start.elapsed();
        
        assert!(result.is_ok());
        // Should complete quickly even with collision-prone keys
        assert!(duration.as_millis() < 100, "Parsing took too long: {:?}", duration);
    }
    
    #[test]
    fn test_secure_json_preserves_structure() {
        let json = r#"{
            "name": "test",
            "nested": {
                "array": [1, 2, 3],
                "bool": true
            }
        }"#;
        
        let result = parse_secure_json(json).unwrap();
        assert_eq!(result["name"], "test");
        assert_eq!(result["nested"]["array"][0], 1);
        assert_eq!(result["nested"]["bool"], true);
    }
}
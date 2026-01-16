use serde_json::Value;

/// Get a string value from a JSON object, returning "N/A" if not found or not a string
pub fn get_str<'a>(obj: &'a Value, key: &str) -> &'a str {
    obj.get(key).and_then(|v| v.as_str()).unwrap_or("N/A")
}

/// Get an owned string value from a JSON object
pub fn get_string(obj: &Value, key: &str) -> String {
    get_str(obj, key).to_string()
}

/// Get an optional string value from a JSON object
pub fn get_optional_str<'a>(obj: &'a Value, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(|v| v.as_str())
}

/// Get an i64 value from a JSON object, returning 0 if not found or not a number
pub fn get_i64(obj: &Value, key: &str) -> i64 {
    obj.get(key).and_then(|v| v.as_i64()).unwrap_or(0)
}

/// Get a u64 value from a JSON object, returning 0 if not found or not a number
pub fn get_u64(obj: &Value, key: &str) -> u64 {
    obj.get(key).and_then(|v| v.as_u64()).unwrap_or(0)
}

/// Get a boolean value from a JSON object, returning false if not found or not a boolean
pub fn get_bool(obj: &Value, key: &str) -> bool {
    obj.get(key).and_then(|v| v.as_bool()).unwrap_or(false)
}

/// Get a nested object from a JSON value
pub fn get_object<'a>(obj: &'a Value, key: &str) -> Option<&'a Value> {
    obj.get(key).filter(|v| v.is_object())
}

/// Get an array from a JSON value
pub fn get_array<'a>(obj: &'a Value, key: &str) -> Option<&'a Vec<Value>> {
    obj.get(key).and_then(|v| v.as_array())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_str() {
        let obj = json!({
            "name": "test",
            "number": 42,
            "null": null
        });

        assert_eq!(get_str(&obj, "name"), "test");
        assert_eq!(get_str(&obj, "missing"), "N/A");
        assert_eq!(get_str(&obj, "number"), "N/A"); // Not a string
        assert_eq!(get_str(&obj, "null"), "N/A"); // Null value
    }

    #[test]
    fn test_get_string() {
        let obj = json!({
            "name": "test"
        });

        assert_eq!(get_string(&obj, "name"), "test");
        assert_eq!(get_string(&obj, "missing"), "N/A");
    }

    #[test]
    fn test_get_optional_str() {
        let obj = json!({
            "name": "test",
            "number": 42
        });

        assert_eq!(get_optional_str(&obj, "name"), Some("test"));
        assert_eq!(get_optional_str(&obj, "missing"), None);
        assert_eq!(get_optional_str(&obj, "number"), None);
    }

    #[test]
    fn test_get_i64() {
        let obj = json!({
            "count": 42,
            "string": "not a number",
            "float": 3.14
        });

        assert_eq!(get_i64(&obj, "count"), 42);
        assert_eq!(get_i64(&obj, "missing"), 0);
        assert_eq!(get_i64(&obj, "string"), 0);
        assert_eq!(get_i64(&obj, "float"), 0); // f64 not convertible to i64
    }

    #[test]
    fn test_get_u64() {
        let obj = json!({
            "count": 42,
            "negative": -5
        });

        assert_eq!(get_u64(&obj, "count"), 42);
        assert_eq!(get_u64(&obj, "missing"), 0);
        assert_eq!(get_u64(&obj, "negative"), 0); // Can't convert negative to u64
    }

    #[test]
    fn test_get_bool() {
        let obj = json!({
            "enabled": true,
            "disabled": false,
            "string": "true"
        });

        assert_eq!(get_bool(&obj, "enabled"), true);
        assert_eq!(get_bool(&obj, "disabled"), false);
        assert_eq!(get_bool(&obj, "missing"), false);
        assert_eq!(get_bool(&obj, "string"), false); // Not a bool
    }

    #[test]
    fn test_get_object() {
        let obj = json!({
            "nested": {
                "key": "value"
            },
            "array": [],
            "string": "not an object"
        });

        assert!(get_object(&obj, "nested").is_some());
        assert!(get_object(&obj, "missing").is_none());
        assert!(get_object(&obj, "array").is_none());
        assert!(get_object(&obj, "string").is_none());
    }

    #[test]
    fn test_get_array() {
        let obj = json!({
            "items": [1, 2, 3],
            "object": {},
            "string": "not an array"
        });

        assert_eq!(get_array(&obj, "items").map(|a| a.len()), Some(3));
        assert!(get_array(&obj, "missing").is_none());
        assert!(get_array(&obj, "object").is_none());
        assert!(get_array(&obj, "string").is_none());
    }
}

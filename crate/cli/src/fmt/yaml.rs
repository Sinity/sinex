use serde::Serialize;

use crate::Result;

/// Format output as YAML
pub fn format_yaml<T: Serialize>(value: &T) -> Result<String> {
    serde_yaml::to_string(value).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn format_yaml_simple_object() {
        let val = json!({"name": "test", "count": 42});
        let result = format_yaml(&val).unwrap();
        assert!(result.contains("name:"));
        assert!(result.contains("test"));
        assert!(result.contains("count:"));
        assert!(result.contains("42"));
    }

    #[test]
    fn format_yaml_nested() {
        let val = json!({"parent": {"child": "value"}});
        let result = format_yaml(&val).unwrap();
        assert!(result.contains("parent:"));
        assert!(result.contains("child:"));
    }

    #[test]
    fn format_yaml_list() {
        let val = json!({"items": [1, 2, 3]});
        let result = format_yaml(&val).unwrap();
        assert!(result.contains("items:"));
    }

    #[test]
    fn format_yaml_null() {
        let val = json!(null);
        let result = format_yaml(&val).unwrap();
        assert!(result.contains("null"));
    }

    #[test]
    fn format_yaml_special_chars() {
        let val = json!({"text": "hello: world\nline2"});
        let result = format_yaml(&val).unwrap();
        // Should be able to parse back
        let parsed: serde_json::Value = serde_yaml::from_str(&result).unwrap();
        assert_eq!(parsed["text"], "hello: world\nline2");
    }

    #[test]
    fn format_yaml_empty_object() {
        let val = json!({});
        let result = format_yaml(&val).unwrap();
        assert!(result.contains("{}"));
    }
}

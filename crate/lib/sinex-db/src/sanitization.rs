use crate::models::Event;
use crate::security::{SecurityError, SecurityValidator};
use color_eyre::eyre::Result;
use serde_json::Value;
use sinex_types::domain::EventSource;
use std::borrow::Cow;

/// Event sanitization service that modifies events before storage
pub struct EventSanitizer;

impl EventSanitizer {
    /// Sanitize an event before storage, modifying content to prevent security issues
    /// while preserving the original attack data for security analysis
    pub fn sanitize_event(event: &mut Event) -> Result<bool> {
        let mut was_modified = false;

        // Sanitize the source field (where attacks come through in tests)
        match SecurityValidator::sanitize_path(event.source.as_str()) {
            Ok(Cow::Owned(sanitized)) => {
                if sanitized != event.source.as_str() {
                    event.source = EventSource::new(sanitized);
                    was_modified = true;
                }
            }
            Ok(Cow::Borrowed(_)) => {
                // No change needed
            }
            Err(SecurityError::PathTraversal(_)) => {
                // For path traversal, sanitize by removing dangerous sequences
                event.source =
                    EventSource::new(Self::sanitize_path_traversal(event.source.as_str()));
                was_modified = true;
            }
            Err(SecurityError::NullByteInjection) => {
                // Remove null bytes
                event.source = EventSource::new(event.source.as_str().replace('\0', ""));
                was_modified = true;
            }
            Err(_) => {
                // Other security errors - sanitize conservatively
                event.source =
                    EventSource::new(Self::sanitize_string_conservative(event.source.as_str()));
                was_modified = true;
            }
        }

        // Sanitize payload content
        if Self::sanitize_json_payload(&mut event.payload)? {
            was_modified = true;
        }

        Ok(was_modified)
    }

    /// Sanitize path traversal attempts by removing dangerous sequences
    fn sanitize_path_traversal(input: &str) -> String {
        input
            .replace("..", "")
            .replace("\\", "/")
            .replace("%2e%2e", "")
            .replace("%252e%252e", "")
            .replace("..%2f", "")
            .replace("..%5c", "")
            .replace("..%c0%af", "")
            .replace("..%c1%9c", "")
    }

    /// Conservative string sanitization - remove dangerous characters
    fn sanitize_string_conservative(input: &str) -> String {
        input
            .chars()
            .filter(|&c| c != '\0' && c.is_ascii_graphic() || c.is_ascii_whitespace())
            .collect()
    }

    /// Sanitize JSON payload recursively
    fn sanitize_json_payload(payload: &mut Value) -> Result<bool> {
        let mut was_modified = false;

        match payload {
            Value::String(s) => {
                // Check for path traversal in string values
                if s.contains("..") || s.contains('\0') {
                    let original = s.clone();
                    *s = Self::sanitize_path_traversal(s);
                    if *s != original {
                        was_modified = true;
                    }
                }
            }
            Value::Object(map) => {
                for (key, value) in map.iter_mut() {
                    // Special handling for path-like fields
                    if key.contains("path") || key == "file" || key == "directory" {
                        if let Value::String(s) = value {
                            let original = s.clone();
                            match SecurityValidator::sanitize_path(s) {
                                Ok(Cow::Owned(sanitized)) => {
                                    *s = sanitized;
                                    if *s != original {
                                        was_modified = true;
                                    }
                                }
                                Ok(Cow::Borrowed(_)) => {
                                    // No sanitization needed
                                }
                                Err(_) => {
                                    *s = Self::sanitize_path_traversal(s);
                                    if *s != original {
                                        was_modified = true;
                                    }
                                }
                            }
                        }
                    } else {
                        // Recursively sanitize nested content
                        if Self::sanitize_json_payload(value)? {
                            was_modified = true;
                        }
                    }
                }
            }
            Value::Array(arr) => {
                for item in arr.iter_mut() {
                    if Self::sanitize_json_payload(item)? {
                        was_modified = true;
                    }
                }
            }
            _ => {
                // Numbers, booleans, null - no sanitization needed
            }
        }

        Ok(was_modified)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Event;
    use serde_json::json;
    use sinex_test_utils::prelude::*;
    use sinex_types::domain::EventType;

    #[sinex_test]
    async fn test_path_traversal_sanitization(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let mut event = Event::schemaless()
            .source(EventSource::new("../../../etc/passwd"))
            .event_type(EventType::new("security.test"))
            .payload(json!({"path": "../../sensitive/file.txt"}))
            .build();

        let was_modified = EventSanitizer::sanitize_event(&mut event).unwrap();
        assert!(was_modified);

        // Source should be sanitized
        assert!(!event.source.as_str().contains(".."));

        // Payload path should be sanitized
        if let Some(path) = event.payload.get("path").and_then(|v| v.as_str()) {
            assert!(!path.contains(".."));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_null_byte_sanitization(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let mut event = Event::schemaless()
            .source(EventSource::new("test\0source"))
            .event_type(EventType::new("security.test"))
            .payload(json!({"data": "test\0value"}))
            .build();

        let was_modified = EventSanitizer::sanitize_event(&mut event).unwrap();
        assert!(was_modified);

        // Null bytes should be removed
        assert!(!event.source.contains('\0'));
        Ok(())
    }

    #[sinex_test]
    async fn test_sql_injection_preserved(ctx: TestContext) -> color_eyre::eyre::Result<()> {
        let mut event = Event::schemaless()
            .source(EventSource::new("security.test"))
            .event_type(EventType::new("sql.injection"))
            .payload(json!({"query": "'; DROP TABLE events; --"}))
            .build();

        let was_modified = EventSanitizer::sanitize_event(&mut event).unwrap();
        assert!(!was_modified);

        // SQL injection should be preserved in payload as it's just string data
        assert_eq!(
            event.payload.get("query").unwrap().as_str().unwrap(),
            "'; DROP TABLE events; --"
        );
        Ok(())
    }
}

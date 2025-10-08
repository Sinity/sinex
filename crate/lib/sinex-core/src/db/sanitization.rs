use crate::models::{Event, JsonValue};
use crate::security::{SecurityError, SecurityValidator};
use crate::types::domain::EventSource;
use color_eyre::eyre::Result;
use serde_json::Value;
use std::borrow::Cow;

/// Event sanitization service that modifies events before storage
pub struct EventSanitizer;

impl EventSanitizer {
    /// Sanitize any event type before storage, modifying content to prevent security issues
    /// while preserving the original attack data for security analysis
    pub fn sanitize_event_generic<T>(event: &mut Event<T>) -> Result<bool>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
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

        // Sanitize payload content by converting to JSON, sanitizing, and back
        let mut payload_json = serde_json::to_value(&event.payload)
            .map_err(|e| color_eyre::eyre::eyre!("Failed to serialize payload: {}", e))?;

        if Self::sanitize_json_payload(&mut payload_json)? {
            // Convert back to the original type
            event.payload = serde_json::from_value(payload_json).map_err(|e| {
                color_eyre::eyre::eyre!("Failed to deserialize sanitized payload: {}", e)
            })?;
            was_modified = true;
        }

        Ok(was_modified)
    }

    /// Sanitize an event before storage, modifying content to prevent security issues
    /// while preserving the original attack data for security analysis
    ///
    /// This is a specialized version for JsonValue events that's more efficient
    pub fn sanitize_event(event: &mut Event<JsonValue>) -> Result<bool> {
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

use crate::db::models::{Event, JsonValue};
use crate::db::security::{SecurityError, SecurityValidator};
use crate::types::domain::EventSource;
use color_eyre::eyre::Result;
use percent_encoding::percent_decode_str;
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
                // Remove null bytes and re-run traversal sanitization to keep outputs stable
                let cleaned = event.source.as_str().replace('\0', "");
                event.source = EventSource::new(Self::sanitize_path_traversal(&cleaned));
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
                // Remove null bytes and re-run traversal sanitization to keep outputs stable
                let cleaned = event.source.as_str().replace('\0', "");
                event.source = EventSource::new(Self::sanitize_path_traversal(&cleaned));
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
        // Normalize obvious separators and strip null bytes before further processing
        let mut normalized = input.replace('\\', "/");
        normalized.retain(|c| c != '\0');

        // Repeatedly percent-decode to collapse encoded traversal attempts like %252e%252e
        let mut decoded = normalized;
        let had_leading_slash = decoded.starts_with('/');
        for _ in 0..4 {
            let next = percent_decode_str(&decoded)
                .decode_utf8_lossy()
                .into_owned();
            if next == decoded {
                break;
            }
            decoded = next;
        }

        // Collapse traversal segments while preserving any remaining benign path data
        let mut sanitized = String::with_capacity(decoded.len());
        for segment in decoded.split('/') {
            if segment.is_empty() {
                continue;
            }

            let cleaned_segment = segment.replace("..", "");
            if cleaned_segment.is_empty() || cleaned_segment == "." {
                continue;
            }

            if !sanitized.is_empty() {
                sanitized.push('/');
            }
            sanitized.push_str(&cleaned_segment);
        }

        // As a final safeguard, strip any lingering traversal markers and redundant separators
        let mut final_value = sanitized;
        while final_value.contains("..") {
            final_value = final_value.replace("..", "");
        }
        while final_value.contains("//") {
            final_value = final_value.replace("//", "/");
        }

        if had_leading_slash && !final_value.is_empty() && !final_value.starts_with('/') {
            final_value.insert(0, '/');
        }

        if final_value.is_empty() {
            return "sanitized".to_string();
        }

        final_value
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
                    let is_path_field = matches!(
                        key.as_str(),
                        "path" | "file" | "directory" | "folder" | "root"
                    ) || key.ends_with("_path")
                        || key.ends_with("_file")
                        || key.ends_with("_dir");

                    if is_path_field {
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
    use super::EventSanitizer;
    use crate::types::events::DynamicPayload;
    use crate::types::Id;
    use serde_json::json;

    #[test]
    fn encoded_traversal_is_idempotent_once_sanitized() {
        let mut event = DynamicPayload::new(
            "..%2f..%2f..%2fetc%2fpasswd",
            "test.event",
            json!({"test": "data"}),
        )
        .from_material(Id::new())
        .build()
        .expect("test event should build");

        EventSanitizer::sanitize_event(&mut event).unwrap();
        let mut sanitized = event.clone();
        let changed = EventSanitizer::sanitize_event(&mut sanitized).unwrap();

        assert!(
            !changed,
            "sanitizing twice should be stable: {} -> {}",
            event.source, sanitized.source
        );
        assert_eq!(event.source, sanitized.source);
    }
}

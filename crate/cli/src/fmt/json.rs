use serde::Serialize;

use crate::Result;

/// Format output as JSON (one object per line)
pub fn format_json<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(Into::into)
}

/// Format multiple items as JSON lines
pub fn format_json_lines<T: Serialize>(items: &[T]) -> Result<String> {
    let mut output = String::new();
    for item in items {
        output.push_str(&serde_json::to_string(item)?);
        output.push('\n');
    }
    Ok(output)
}

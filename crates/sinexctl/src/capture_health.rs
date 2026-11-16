//! The local producer report remains readable while publishing is unavailable.
use serde_json::{Value, json};
use std::path::Path;

pub(crate) fn attach(status: &mut Value, state: Option<&Path>) {
    if let Some(root) = state {
        let path = root.join("health.json");
        status["local_capture"] = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(
                |error| json!({"unavailable": error.to_string(), "report": path.to_string_lossy()}),
            ),
            Err(error) => {
                json!({"unavailable": error.to_string(), "report": path.to_string_lossy()})
            }
        };
    }
}

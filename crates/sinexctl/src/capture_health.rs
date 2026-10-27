//! The local producer report remains readable while publishing is unavailable.
use serde_json::{Value, json};
use std::path::PathBuf;

pub(crate) fn attach(status: &mut Value, state: Option<&str>) {
    let root = state
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("SINEX_CAPTURE_STATE").map(PathBuf::from))
        .or_else(|| {
            std::env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state"))
                })
                .map(|root| root.join("sinex-session"))
        });
    if let Some(root) = root {
        let path = root.join("health.json");
        status["local_capture"] = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .unwrap_or_else(|error| json!({"unavailable": error.to_string(), "report": path.to_string_lossy()})),
            Err(error) => json!({"unavailable": error.to_string(), "report": path.to_string_lossy()}),
        };
    }
}

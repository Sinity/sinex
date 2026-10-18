//! Diagnostics that work while the daemon is down (architecture §7.2):
//! `doctor`, `status --offline`, `store verify --offline`. Read-only.
use serde_json::Value;

/// Check the database is reachable, at this build's schema version, and the
/// content store is readable.
pub async fn doctor(_database: &str, _store: &std::path::Path) -> Value {
    todo!("architecture §7.2: offline doctor, read-only")
}

/// Verify every registered blob against its BLAKE3 name.
pub async fn verify_store(_database: &str, _store: &std::path::Path) -> Value {
    todo!("architecture §7.2: store verify --offline")
}

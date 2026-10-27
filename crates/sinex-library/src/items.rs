//! Library identifiers.
/// A copy's key in an item's `copies`.
pub fn copy_key(host: &str, path: &str) -> String {
    format!("{host}:{path}")
}

/// A new identifier for an authored library record.
pub fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

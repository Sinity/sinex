//! Core primitive types for Sinex: `UUIDv7` and Timestamp.

pub mod timestamp;

// Re-export main types
pub use timestamp::Timestamp;
pub use uuid::Uuid;

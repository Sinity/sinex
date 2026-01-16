use serde::{Deserialize, Serialize};

/// Processing statistics for batch operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessingStats {
    pub processed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub duration: std::time::Duration,
    pub errors: Vec<String>,
}

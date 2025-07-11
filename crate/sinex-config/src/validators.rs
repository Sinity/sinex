//! Configuration validation utilities

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    pub valid: bool,
    pub errors: Vec<String>,
}

impl Default for ValidationReport {
    fn default() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
        }
    }
}

use serde::{Deserialize, Serialize};

/// Configuration for Kitty event source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KittyConfig {
    pub poll_interval_seconds: u64,
    pub socket_path: Option<String>,
    pub enabled: bool,
}

impl Default for KittyConfig {
    fn default() -> Self {
        Self {
            poll_interval_seconds: 5,
            socket_path: None,
            enabled: true,
        }
    }
}

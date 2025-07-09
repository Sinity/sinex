/// Kitty terminal event source modules
/// 
/// This module contains all components for monitoring Kitty terminal
/// events, organized into focused submodules for better maintainability.

pub mod config;
pub mod state;
pub mod protocol;
pub mod api;

// Re-export commonly used types
pub use config::KittyConfig;
pub use state::{KittyStateManager, KittyProcessInfo, KittyWindowState};
pub use protocol::KittyProtocol;
pub use api::{KittyApi, TabInfo};

// The main event source will be defined in the main module
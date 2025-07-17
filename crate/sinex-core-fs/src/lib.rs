//! Core filesystem utilities for the Sinex system
//!
//! This crate provides filesystem operations including file watching
//! and directory management with consistent error handling.

pub mod directory_manager;
pub mod file_watcher;

// Re-export all filesystem utilities
pub use directory_manager::*;
pub use file_watcher::*;

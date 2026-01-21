//! Example implementations of the unified stream processor architecture
//!
//! This module contains example implementations showing how to migrate from
//! the old EventSource trait to the new Node trait.

pub mod filesystem_node;

pub use filesystem_node::{FilesystemNode, FilesystemNodeConfig};

//! Example implementations of the unified stream processor architecture
//!
//! This module contains example implementations showing how to migrate from
//! the old EventSource trait to the new StatefulStreamProcessor trait.

pub mod filesystem_processor;

pub use filesystem_processor::FilesystemProcessor;

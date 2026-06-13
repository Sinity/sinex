//! Filesystem source (`fs`).
//!
//! Uses the runtime's content-materializing file-drop adapter plus the filesystem
//! parser, so watcher policy, source-material staging, and parser dispatch
//! share the same adapter-backed source surface as the rest of the source
//! unit host.

pub mod parser;

pub use parser::FilesystemParser;

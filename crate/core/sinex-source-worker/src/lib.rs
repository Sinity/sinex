//! sinex-source-worker — consolidated source ingestor host.
//!
//! All sources run in this binary. Shared mechanisms live in the SDK
//! (`sinex-node-sdk::parser`). Source-specific parsers live in `crate::parsers`.
//! Source modules in `crate::sources` bind mechanisms + parsers.

pub mod drain;
pub mod noop;
pub mod parse_listener;
pub mod parsers;
pub mod registry;
pub mod runner;
pub mod sources;

pub use drain::{GapEvidence, SourceWorkerDrainController};
pub use noop::NoopSourceUnit;
pub use registry::SourceUnitRegistry;
pub use runner::SourceUnitRunner;

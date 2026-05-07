//! sinex-source-worker — consolidated source-unit ingestor host.
//!
//! Hosts source-unit ingestors in a single binary. The source unit to run
//! is selected by the `--source-unit <id>` argument (or `SINEX_SOURCE_UNIT` env).
//!
//! # Usage
//!
//! ```text
//! sinex-source-worker --source-unit <id> [node-sdk args] service
//! sinex-source-worker --source-unit <id> [node-sdk args] scan
//! ```
//!
//! # Architecture
//!
//! The host follows the same pattern as [`sinex-process`]: one binary, N systemd
//! services instantiated via a `--source-unit` selector argument.
//!
//! - [`registry::SourceUnitRegistry`] — validates source-unit existence from the
//!   compile-time [`SourceUnitDescriptor`] inventory.
//! - [`runner::SourceUnitRunner`] — assembles per-unit runtime handles (drain
//!   controller, service identity) before the SDK `NodeRunner` takes over.
//! - [`drain::SourceWorkerDrainController`] — per-unit drain protocol with
//!   material tracking, active-work gating, confirmation waiting, and gap-evidence
//!   recording for crash recovery.
//!
//! # Source unit dispatch
//!
//! New source units are added by:
//! 1. Adding an `IngestorNode` implementation (either in this crate or as a
//!    dependency).
//! 2. Registering its [`SourceUnitDescriptor`] via [`register_source_unit!`].
//! 3. Adding a match arm in `main.rs`.
//!
//! The dispatch follows the exact same pattern as `sinex-process/src/main.rs`.

pub mod drain;
pub mod noop;
pub mod registry;
pub mod runner;

pub use drain::{GapEvidence, SourceWorkerDrainController};
pub use noop::NoopSourceUnit;
pub use registry::SourceUnitRegistry;
pub use runner::SourceUnitRunner;

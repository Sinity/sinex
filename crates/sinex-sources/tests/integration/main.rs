//! The sources' integration tests: one binary, one module per area.
#[cfg(feature = "full")]
mod archive;
mod dir_snapshot_memory;
#[cfg(feature = "full")]
mod framing;
#[cfg(feature = "full")]
mod golden;
mod mutable_rows;
mod producer;

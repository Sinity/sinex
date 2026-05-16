//! Material acquisition and transformation substrate.
//!
//! This module provides reusable abstractions for common material handling patterns
//! across ingestors and derived nodes. The three key components are:
//!
//! - **[retry]**: Generic retry wrapper for transient I/O errors with exponential backoff
//! - **[observation]**: Buffered batching for metadata-only events (no payload bytes)
//! - **[stream]**: Base abstraction for streaming material contexts that produce events over time

pub mod observation;
pub mod retry;
pub mod stream;

pub use observation::ObservationMaterializer;
pub use retry::{RetryableMaterialCapture, TransientErrorPredicate};
pub use stream::StreamMaterialContext;

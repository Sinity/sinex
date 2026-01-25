//! Event publishing utilities for tests.
//!
//! # Primary API
//!
//! Use `ctx.publish()` with any type implementing [`Publishable`]:
//!
//! ```rust,ignore
//! // Typed payloads (preferred - compile-time safety)
//! let event = ctx.publish(FileCreatedPayload { ... }).await?;
//!
//! // Dynamic payloads (escape hatch for runtime source/type)
//! let event = ctx.publish(DynamicPayload::new(
//!     "fs-watcher",
//!     "file.created",
//!     json!({ "path": "/test/file.txt" }),
//! )).await?;
//! ```

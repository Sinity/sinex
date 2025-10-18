#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../../docs/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../../../../docs/architecture/security-architecture.md")]

//! Thin facade that re-exports high-level services used by gateways and satellites.

/// Analytics service for processing and aggregating event data
pub mod analytics;
/// Content service for managing large binary data and media
pub mod content;
pub mod error;
/// PKM (Personal Knowledge Management) service for entity and relationship tracking
pub mod pkm;
pub mod prelude;
/// Search service for querying events and content
pub mod search;

pub use analytics::AnalyticsService;
pub use content::ContentService;
pub use error::{Result, ServiceResult, SinexError};
pub use pkm::PkmService;
pub use search::{SearchQuery, SearchService};

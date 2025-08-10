//! # Sinex Services Layer
//!
//! This crate provides high-level business logic services that abstract
//! over the raw database operations in sinex-db. Services encapsulate
//! complex workflows and provide clean APIs for the rest of the system.

/// Analytics service for processing and aggregating event data
pub mod analytics;
/// Content service for managing large binary data and media
pub mod content;
pub mod error;
/// PKM (Personal Knowledge Management) service for entity and relationship tracking
pub mod pkm;
/// Search service for querying events and content
pub mod search;

pub use analytics::AnalyticsService;
pub use content::ContentService;
pub use error::{ServiceError, ServiceResult};
pub use pkm::PkmService;
pub use search::{SearchQuery, SearchService};

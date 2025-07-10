//! # Sinex Services Layer
//! 
//! This crate provides high-level business logic services that abstract
//! over the raw database operations in sinex-db. Services encapsulate
//! complex workflows and provide clean APIs for the rest of the system.

pub mod analytics;
pub mod content;
pub mod error;
pub mod pkm;
pub mod search;

pub use analytics::AnalyticsService;
pub use content::ContentService;
pub use error::{ServiceError, ServiceResult};
pub use pkm::PkmService;
pub use search::{SearchService, SearchQuery};
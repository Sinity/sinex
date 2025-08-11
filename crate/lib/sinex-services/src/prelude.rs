//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-services crate for more ergonomic imports:
//!
//! ```rust
//! use sinex_services::prelude::*;
//!
//! // Instead of:
//! // use sinex_services::{AnalyticsService, ContentService, PkmService, SearchService};
//! // use sinex_services::{SearchQuery, SearchResult, MaterialSummary};
//! ```

// All service types
pub use crate::{AnalyticsService, ContentService, PkmService, SearchService};

// Search-related types
pub use crate::{SearchQuery, SearchResult};

// PKM-related types
pub use crate::MaterialSummary;

// Error handling
pub use crate::{Result, ServiceResult, SinexError};

#![doc = include_str!("../docs/README.md")]

//! # Sinex Services Layer
//!
//! The `sinex-services` crate provides a high-level business logic layer that orchestrates
//! operations between `sinex-db` repositories and the `sinex-gateway` handlers.
//!
//! ## Architecture & Design
//!
//! This layer is intentionally designed as a **thin facade**. Its primary responsibilities are:
//!
//! - **Orchestration**: Coordinating multi-step workflows (e.g., registering source material and
//!   creating associated entities).
//! - **Transformation**: Mapping database-optimized records into API-stable DTOs.
//! - **Business Logic**: Enforcing rules such as metadata segregation and Unicode-safe snippet
//!   extraction.
//!
//! ## Core Principles
//!
//! - **Statelessness**: Services are stateless facades around shared resource pools.
//! - **Fail-Fast**: Aggressive connection timeouts prevent analytical queries from
//!   impacting ingestion performance.
//! - **Provenance Integrity**: Standardized metadata builders ensure that every record in the
//!   knowledge graph maintains an auditable link to its source.
//!
//! For detailed architectural deep dives, see the documentation in the `docs/` directory or
//! the included structural analysis.

#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../../../../docs/current/architecture/security-architecture.md")]

//! Thin facade that re-exports high-level services used by gateways and nodes.

/// Content service for managing large binary data and media
pub mod content;
/// PKM (Personal Knowledge Management) service for entity and relationship tracking
pub mod pkm;
pub mod prelude;

pub use content::ContentService;
pub use sinex_primitives::error::{Result, SinexError};
pub use pkm::PkmService;

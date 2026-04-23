#![doc = include_str!("../docs/README.md")]

//! # Sinex Services Layer
//!
//! PKM and content orchestration have moved into their directional owners.
//! This crate remains only as an empty workspace placeholder until the follow-up
//! cleanup removes it entirely.
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

pub use sinex_primitives::error::{Result, SinexError};

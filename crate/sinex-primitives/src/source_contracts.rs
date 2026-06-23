//! Source contract vocabulary.
//!
//! This module holds typed source identity and runtime-binding declarations.
//! It intentionally does not declare advisory obligations; source correctness
//! belongs in tests, runtime validation, and deployment checks.

mod capability;
mod contract;
mod lifecycle;
mod resource;
mod runtime;
mod subject;
#[cfg(test)]
mod tests;

pub use capability::*;
pub use contract::*;
pub use lifecycle::*;
pub use resource::*;
pub use runtime::*;
pub use subject::*;

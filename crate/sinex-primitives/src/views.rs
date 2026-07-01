//! Shared human/agent view DTOs.

mod common;
mod completion;
mod debt;
mod desktop;
mod events;
mod operations;
mod sources;

#[cfg(test)]
mod tests;

pub use common::*;
pub use completion::*;
pub use debt::*;
pub use desktop::*;
pub use events::*;
pub use operations::*;
pub use sources::*;

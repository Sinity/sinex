//! Chromium History reading. Browser rows never feed shell derivation.
#[cfg(feature = "runtime")]
mod reader;
#[cfg(feature = "runtime")]
mod rows;
pub(crate) mod snapshot;

#[cfg(feature = "runtime")]
pub use reader::{begin, run};

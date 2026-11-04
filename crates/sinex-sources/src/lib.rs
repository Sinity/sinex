//! Source families and the live capture loop, built over sinex-source-core.
pub mod discovery;
pub mod live;
pub mod sources;
#[cfg(feature = "full")]
pub mod stage;
pub mod unimplemented;

#[cfg(test)]
mod test_support;

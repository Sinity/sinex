//! Build script for xtask.
//!
//! Uses shadow-rs to embed build-time metadata (git hash, build time, etc.)
//! into the binary for version info and debugging.

fn main() -> shadow_rs::SdResult<()> {
    shadow_rs::new()
}

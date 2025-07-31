//! Build script for sinex-events
//!
//! This build script ensures that payload definitions are tracked for rebuilds.
//! The actual payload discovery happens at runtime via the inventory crate.

fn main() {
    // Only rebuild when payload definitions change
    println!("cargo:rerun-if-changed=src/payloads/");
}

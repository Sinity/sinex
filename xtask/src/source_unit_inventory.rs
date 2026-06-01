//! Link shipped node crates so their inventory source-unit descriptors are
//! visible to xtask.
//!
//! Source-unit descriptors now live behind the unified daemon.  The `sinexd`
//! link is gated so the ordinary xtask developer loop can build without
//! dragging runtime introspection into every command.

#[cfg(any(feature = "runtime-introspection", test))]
extern crate sinexd as _;

// `sinex_primitives` carries the infra source-unit descriptors registered
// by `crate/lib/sinex-primitives/src/events/payloads/{blob,process,metrics}.rs`.
// Without an `extern crate` line the linker drops the inventory submissions
// even though xtask depends on `sinex_primitives` directly through `use`
// statements — Rust's linker GC can still elide statics in `.init_array`-
// equivalent sections that are not visibly referenced.
extern crate sinex_primitives as _;

pub fn link_source_unit_inventories() {}

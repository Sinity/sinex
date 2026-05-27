//! Link shipped node crates so their inventory source-unit descriptors are
//! visible to xtask.
//!
//! After the Wave-B fold (#1081) every per-domain ingestor crate has been
//! collapsed into `sinex-source-worker`, so a single `extern crate` line on
//! that crate is enough to pull every source-unit descriptor into xtask's
//! inventory view.

extern crate sinexd as _;

// `sinex_primitives` carries the infra source-unit descriptors registered
// by `crate/lib/sinex-primitives/src/events/payloads/{blob,process,metrics}.rs`.
// Without an `extern crate` line the linker drops the inventory submissions
// even though xtask depends on `sinex_primitives` directly through `use`
// statements — Rust's linker GC can still elide statics in `.init_array`-
// equivalent sections that are not visibly referenced.
extern crate sinex_primitives as _;
extern crate sinex_process as _;

pub fn link_source_unit_inventories() {}

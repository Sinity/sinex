//! Link shipped node crates so their inventory source-unit descriptors are
//! visible to xtask.

extern crate sinex_browser_ingestor as _;
extern crate sinex_desktop_ingestor as _;
extern crate sinex_document_ingestor as _;
extern crate sinex_fs_ingestor as _;
// `sinex_primitives` carries the infra source-unit descriptors registered
// by `crate/lib/sinex-primitives/src/events/payloads/{blob,process,metrics}.rs`.
// Without an `extern crate` line the linker drops the inventory submissions
// even though xtask depends on `sinex_primitives` directly through `use`
// statements — Rust's linker GC can still elide statics in `.init_array`-
// equivalent sections that are not visibly referenced.
extern crate sinex_primitives as _;
extern crate sinex_process as _;
extern crate sinex_system_ingestor as _;
extern crate sinex_terminal_ingestor as _;

pub fn link_source_unit_inventories() {}

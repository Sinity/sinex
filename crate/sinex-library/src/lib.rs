//! The media library (architecture §6.6): items, renditions, curation and
//! handoff, built on materials and admission. Intake is a scan of a library
//! root through the ordinary source path (`sinex-sources`'s `library.scan`);
//! every library change is a library request, stored as its own material and
//! admitted (`library.request`); removal is hide and excision. This crate
//! holds the library's rules: which item an acquisition belongs to, which
//! value is shown, how comparisons rank, how other applications' curation is
//! read, and how renditions and handoff work.
pub mod adopt;
pub mod db;
pub mod import;
pub mod policy;
pub mod rank;
pub mod render;

pub use adopt::{Owner, adopt, key, mint};
pub use policy::{Shown, Tag, property, tags};
pub use rank::{Ranked, bradley_terry, next_pair};

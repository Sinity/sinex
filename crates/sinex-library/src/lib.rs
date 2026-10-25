//! The media library: items, renditions, curation and opening files, built on
//! materials and admission. Intake is a scan of a library root through the
//! ordinary source path (`sinex-sources`'s `library.scan`); every library
//! change is a library request, stored as its own material and admitted
//! (`library.request`); removal is hide and excision. This crate holds the
//! library's rules: which item an acquisition belongs to, which value is shown,
//! how comparisons rank, how other applications' curation is read, and how
//! renditions are made and opened.
pub mod import;
pub mod items;
pub mod rank;
pub mod reads;
pub mod render;
pub mod shown;

pub use items::{acquisition_key, new_id, records_for};
pub use rank::{Ranked, bradley_terry, next_pair};
pub use shown::{Shown, Tag, property, tags};

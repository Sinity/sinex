//! The media library: items, renditions, curation and opening files, built on
//! materials and admission. Scans and rendered previews are evidence for
//! `library.items`; authored curation is stored as request material. This crate
//! reads the derived items and renditions, selects displayed values, imports
//! curation, ranks comparisons and opens media.
pub mod import;
pub mod items;
pub mod rank;
pub mod reads;
pub mod render;
pub mod shown;

pub use items::{copy_key, new_id};
pub use rank::{Ranked, bradley_terry, next_pair};
pub use shown::{Shown, Tag, property, tags};

//! The media library: items, renditions, curation and opening files, built on
//! materials and admission. Scans and rendered previews are evidence for
//! `library.items`; curation is authored records stored as request material
//! and read through the shared `authored` rules. This crate reads the derived
//! items and renditions, plans curation requests, selects displayed values,
//! imports curation, ranks comparisons and opens media.
pub mod curation;
pub mod import;
pub mod items;
pub mod plan;
pub mod projection;
pub mod rank;
pub mod reads;
pub mod render;
pub mod shown;

pub use items::{copy_key, new_id};
pub use shown::{Shown, Tag, property, tags};

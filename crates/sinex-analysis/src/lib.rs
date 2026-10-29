//! Statistics over derived events, as pure code. The daily metric figures are
//! derived events; this crate turns them into cells that tell zero from
//! missing by the coverage of each metric's sources, runs registered methods
//! that estimate stated questions and refuse when they cannot, writes
//! findings as typed statements, and compiles them into briefs. Results are
//! materialized by the daemon as panels, answers, findings and context packs.
//! A cell depends on explicit source coverage events. Reading the store is
//! the daemon's job; nothing here does
//! IO.
pub mod cell;
pub mod coverage;
pub mod finding;
pub mod forecast;
pub mod methods;
pub mod panel;
pub mod presentation;
pub mod span;

pub use cell::{Cell, Eligibility, Exclusion, Missing};
pub use coverage::{Completeness, Coverage};
pub use panel::{DayFigure, PanelCell, Unit};
pub use span::{Span, SpanSet};

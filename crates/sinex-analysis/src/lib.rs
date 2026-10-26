//! Statistics over derived events, as pure code. The daily metric figures are
//! derived events; this crate turns them into cells that tell zero from
//! missing by the coverage of each metric's sources, runs registered methods
//! that estimate stated questions and refuse when they cannot, writes
//! findings as typed statements, and compiles them into briefs. Results are
//! computed when asked for and not stored: a cell depends on coverage, which
//! is not an event. Reading the store is the daemon's job; nothing here does
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
pub use coverage::{CaptureMode, Completeness, Coverage};
pub use panel::{DayFigure, PanelCell, Unit};
pub use span::{Span, SpanSet};

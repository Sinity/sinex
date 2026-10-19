//! The analysis layer as pure code (architecture §6.5; spec `analysis.md`):
//! results that depend on a whole series, kept as products with the exact
//! inputs they read, not as events. Metrics turn relations into cells that
//! tell zero from missing; registered methods estimate stated questions and
//! refuse when they cannot; findings are typed statements over results; one
//! compiler turns findings into briefs. Reading and writing the store is the
//! daemon's job; nothing here does IO.
pub mod catalog;
pub mod cell;
pub mod finding;
pub mod methods;
pub mod metric;
pub mod opportunity;
pub mod panel;
pub mod present;
pub mod request;
pub mod span;

pub use cell::{Cell, Eligibility, Exclusion, Missing};
pub use metric::{MetricDef, Row};
pub use panel::{Completeness, Opportunity, PanelCell, Unit};
pub use span::{Span, SpanSet};

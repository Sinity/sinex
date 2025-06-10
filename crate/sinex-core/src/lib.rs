pub mod error;
pub mod event;
pub mod types;
pub mod unified_collector;

pub use error::{CoreError, Result};
pub use event::{RawEvent, RawEventBuilder};
pub use types::*;
pub use unified_collector::{EventType, EventSource, EventRegistry, EventOutput};
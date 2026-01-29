//! EventAssert builder for fluent event count assertions.
//!
//! This module provides a composable filter pattern for asserting event counts,
//! replacing the previous proliferation of `assert_event_count*` methods.
//!
//! # Usage
//!
//! ```rust,ignore
//! // Exact count assertion
//! ctx.assert_event().count(5).await?;
//!
//! // At least N events
//! ctx.assert_event().at_least(3).await?;
//!
//! // Filtered by source (typed constant)
//! ctx.assert_event().source(EVENT_SOURCE_FS_WATCHER).count(5).await?;
//!
//! // Filtered by source and type
//! ctx.assert_event()
//!     .source(EVENT_SOURCE_FS_WATCHER)
//!     .event_type(EVENT_TYPE_FILE_CREATED)
//!     .at_least(3)
//!     .await?;
//! ```

use crate::sandbox::context::Sandbox;
use crate::sandbox::prelude::TestResult;
use color_eyre::eyre::bail;
use sinex_db::DbPoolExt;
use sinex_primitives::{EventSource, EventType};

/// Builder for event count assertions with composable filters.
///
/// Created via `Sandbox::assert_event()`. Configure filters using method
/// chaining, then call `.count(n)` or `.at_least(n)` to assert.
pub struct EventAssert<'a> {
    ctx: &'a Sandbox,
    source_filter: Option<EventSource>,
    event_type_filter: Option<EventType>,
}

impl<'a> EventAssert<'a> {
    pub(crate) fn new(ctx: &'a Sandbox) -> Self {
        Self {
            ctx,
            source_filter: None,
            event_type_filter: None,
        }
    }

    /// Filter by event source.
    ///
    /// Accepts anything that converts to `EventSource`, including:
    /// - `EventSource` directly
    /// - `&str` (via `Into<EventSource>`)
    /// - `String` (via `Into<EventSource>`)
    pub fn source(mut self, source: impl Into<EventSource>) -> Self {
        self.source_filter = Some(source.into());
        self
    }

    /// Filter by event type.
    ///
    /// Accepts anything that converts to `EventType`, including:
    /// - `EventType` directly
    /// - `&str` (via `Into<EventType>`)
    /// - `String` (via `Into<EventType>`)
    pub fn event_type(mut self, event_type: impl Into<EventType>) -> Self {
        self.event_type_filter = Some(event_type.into());
        self
    }

    /// Assert the filtered event count equals the expected value.
    pub async fn count(self, expected: usize) -> TestResult<usize> {
        let actual = self.get_count().await?;
        if actual != expected {
            bail!(
                "Expected {} events{}, found {}",
                expected,
                self.filter_description(),
                actual
            );
        }
        Ok(actual)
    }

    /// Assert the filtered event count is at least the expected value.
    pub async fn at_least(self, expected: usize) -> TestResult<usize> {
        let actual = self.get_count().await?;
        if actual < expected {
            bail!(
                "Expected at least {} events{}, found {}",
                expected,
                self.filter_description(),
                actual
            );
        }
        Ok(actual)
    }

    /// Get the current count matching the filters.
    async fn get_count(&self) -> TestResult<usize> {
        let count = match (&self.source_filter, &self.event_type_filter) {
            (None, None) => {
                // No filters - count all events
                self.ctx.pool.events().count_all().await? as usize
            }
            (Some(source), None) => {
                // Filter by source only
                self.ctx.pool.events().count_by_source(source).await? as usize
            }
            (None, Some(event_type)) => {
                // Filter by event type only
                self.ctx
                    .pool
                    .events()
                    .count_by_event_type(event_type)
                    .await? as usize
            }
            (Some(_source), Some(_event_type)) => {
                // Combined source + type filtering not yet supported.
                // Use separate assertions for now.
                bail!(
                    "EventAssert does not yet support combined source + event_type filtering. \
                     Use separate assertions or direct repository queries."
                );
            }
        };
        Ok(count)
    }

    /// Build a description of the active filters for error messages.
    fn filter_description(&self) -> String {
        match (&self.source_filter, &self.event_type_filter) {
            (None, None) => String::new(),
            (Some(source), None) => format!(" for source '{}'", source.as_str()),
            (None, Some(event_type)) => format!(" for type '{}'", event_type.as_str()),
            (Some(source), Some(event_type)) => {
                format!(
                    " for source '{}' and type '{}'",
                    source.as_str(),
                    event_type.as_str()
                )
            }
        }
    }
}

// Enable direct `.await` on the builder (defaults to at_least(1))
impl<'a> std::future::IntoFuture for EventAssert<'a> {
    type Output = TestResult<usize>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        // Default behavior: assert at least 1 event matches
        Box::pin(self.at_least(1))
    }
}

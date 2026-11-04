//! Declaring a source. A source is a parser plus a declaration, registered in
//! one list; the registry generates the NixOS options, the source catalog rows
//! and the status view.
use crate::{DrainInput, DrainOutput, Parser, RecordError};
use sinex_model::Class;
pub use sinex_model::source::{Expectation, days};
use std::time::Duration;

/// The input shape a source reads, which picks its driver: how change in the
/// input is detected and how its bytes become material.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    AppendFile,
    SqliteRows,
    ExportArchive,
    LiveStream,
    ApiCursor,
    DirSnapshot,
    Media,
}

impl Shape {
    /// The shape's name in the catalog and the registry.
    pub fn name(self) -> &'static str {
        match self {
            Shape::AppendFile => "append-file",
            Shape::SqliteRows => "sqlite-rows",
            Shape::ExportArchive => "export-archive",
            Shape::LiveStream => "live-stream",
            Shape::ApiCursor => "api-cursor",
            Shape::DirSnapshot => "dir-snapshot",
            Shape::Media => "media",
        }
    }
}

/// Where a source's records come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// An input Sinex finds at its canonical location or is given, read by the
    /// driver for its shape.
    Input(Shape),
    /// Records written through the API by the operator or an agent, stored as
    /// the request's own bytes. There is no input to find.
    Request,
}

impl Origin {
    /// The input shape, or `None` for records written by request.
    pub fn shape(self) -> Option<Shape> {
        match self {
            Origin::Input(shape) => Some(shape),
            Origin::Request => None,
        }
    }
}

mod framing;
pub use framing::Framer;

/// One source's declaration: the single home of its facts. The catalog, the
/// NixOS options, the source registry and status are generated from it.
#[derive(Clone, Debug)]
pub struct SourceSpec {
    pub name: &'static str,
    pub origin: Origin,
    /// What the source's events are for.
    pub purpose: &'static str,
    /// When the source should have something.
    pub expect: Expectation,
    /// How long the provider keeps its own records, where it expires them
    /// (Chromium history keeps 90 days); `None` when it keeps them.
    pub expires_after: Option<Duration>,
    pub class: Class,
    pub parser: &'static str,
    pub parser_version: u32,
    pub event_types: &'static [&'static str],
    /// How the source's stored materials split into records, so a replay
    /// finds the same records the driver read.
    pub framer: Framer,
}

/// A declared source: its spec and its parser.
pub struct DeclaredSource<P> {
    pub spec: SourceSpec,
    pub parser: P,
}

/// A source with its parser type erased, so the registry can hold every source
/// in one list.
pub trait Source: Send + Sync {
    fn spec(&self) -> &SourceSpec;
    /// Drain one batch at a time, holding only the current record and batch.
    /// Parser errors are accounted record outcomes; material read errors stop
    /// iteration without admitting the failed range.
    fn drainer<'a>(
        &'a self,
        input: DrainInput<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<DrainOutput, RecordError>> + Send + 'a>, RecordError>;

    fn drain(&self, input: DrainInput<'_>) -> Result<Vec<DrainOutput>, RecordError> {
        self.drainer(input)?.collect()
    }
}

impl<P: Parser> Source for DeclaredSource<P> {
    fn spec(&self) -> &SourceSpec {
        &self.spec
    }

    fn drainer<'a>(
        &'a self,
        input: DrainInput<'a>,
    ) -> Result<Box<dyn Iterator<Item = Result<DrainOutput, RecordError>> + Send + 'a>, RecordError>
    {
        Ok(Box::new(crate::drain::Drainer::new(&self.parser, input)?))
    }
}

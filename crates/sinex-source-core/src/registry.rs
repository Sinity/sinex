//! Declaring a source. A source is a parser plus a declaration, registered in
//! one list; the registry generates the NixOS options, the source catalog rows
//! and the status view.
use crate::{DrainInput, DrainOutput, Parser, RecordError};
use sinex_model::Class;

/// The input shape a source reads, which picks its driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Driver {
    AppendFile,
    SqliteRows,
    ExportArchive,
    LiveStream,
    ApiCursor,
    DirSnapshot,
    Media,
    Producer,
    /// Records written through the API by the operator or an agent, stored as
    /// the request's own bytes (library requests).
    ApiRequest,
}

impl Driver {
    /// The driver's name in the catalog and the registry.
    pub fn name(self) -> &'static str {
        match self {
            Driver::AppendFile => "append-file",
            Driver::SqliteRows => "sqlite-rows",
            Driver::ExportArchive => "export-archive",
            Driver::LiveStream => "live-stream",
            Driver::ApiCursor => "api-cursor",
            Driver::DirSnapshot => "dir-snapshot",
            Driver::Media => "media",
            Driver::Producer => "producer",
            Driver::ApiRequest => "api-request",
        }
    }
}

mod framing;
pub use framing::Framer;

#[derive(Clone, Debug)]
pub struct SourceSpec {
    pub name: &'static str,
    pub driver: Driver,
    /// Whether excision can remove a record of this source completely. True
    /// only where every stored copy of a record is one of its readings: the
    /// driver never reads a record a second time (no rewrite, restart, re-fetch
    /// or new export brings it back) and no record repeats under a key in a
    /// later material. Excision refuses records of every other source.
    pub excisable: bool,
    /// What the source's events are for.
    pub purpose: &'static str,
    pub class: Class,
    pub parser: &'static str,
    pub parser_version: u32,
    pub event_types: &'static [&'static str],
    /// How the source's stored materials split into records, so a replay or
    /// an excision finds the same records the driver read.
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

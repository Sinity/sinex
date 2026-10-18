//! Declaring a source (architecture §5.4). A source is a parser plus a
//! declaration, registered in one list; the registry generates the NixOS
//! options, the source catalog rows and the status view.
use crate::{DrainInput, DrainOutput, Parser, RecordError};
use sinex_model::Class;

/// The input shape a source reads, which picks its driver (architecture §5.3).
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
}

#[derive(Clone, Debug)]
pub struct SourceSpec {
    pub name: &'static str,
    pub driver: Driver,
    /// Every source names its consumer: what reads its events (`5j8`).
    pub consumer: &'static str,
    pub class: Class,
    pub parser: &'static str,
    pub parser_version: u32,
    pub event_types: &'static [&'static str],
}

/// A declared source: its spec and its parser.
pub struct Declared<P> {
    pub spec: SourceSpec,
    pub parser: P,
}

/// A source with its parser type erased, so the registry can hold every source
/// in one list.
pub trait Source: Send + Sync {
    fn spec(&self) -> &SourceSpec;
    fn drain(&self, input: DrainInput<'_>) -> Result<Vec<DrainOutput>, RecordError>;
}

impl<P: Parser> Source for Declared<P> {
    fn spec(&self) -> &SourceSpec {
        &self.spec
    }

    fn drain(&self, input: DrainInput<'_>) -> Result<Vec<DrainOutput>, RecordError> {
        crate::drain(&self.parser, input)
    }
}

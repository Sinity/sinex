//! Explicit plain history. Every nonempty complete line is an untimed command.
use sinex_model::{Class, event_types::CommandImported};
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::append_file::AppendFile;
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{RecordContext, RecordError, RecordResult, Stateless, StatelessParser};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.text-history",
    origin: Origin::Input(Shape::AppendFile),
    expect: Expectation::Export,
    purpose: "explicit untimed command history",
    class: Class::Guarded,
    parser: "text-history",
    event_types: &["command.imported"],
    framer: sinex_source_core::Framer::incremental(frame),
    ..SourceSpec::DEFAULT
};

pub struct TextHistoryParser {
    pub host: String,
}

impl StatelessParser for TextHistoryParser {
    type Record<'a> = &'a [u8];
    type Payload = CommandImported;

    fn parse(
        &self,
        record: &&[u8],
        _: &RecordContext<'_>,
    ) -> Result<Vec<CommandImported>, RecordError> {
        let text = std::str::from_utf8(record).record("text history is not UTF-8")?;
        let text = text
            .strip_suffix('\n')
            .record("unfinished text history line")?;
        let text = text.strip_suffix('\r').unwrap_or(text);
        if text.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![CommandImported {
            command: text.into(),
            hostname: self.host.clone(),
            started_at: None,
            header_elapsed: None,
            paths: vec![],
            n: 0,
        }])
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    sinex_source_core::drivers::export_archive::Framing::Lines.frame(bytes, false)
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: SPEC.framer,
    }
}

input_format! { pub FORMAT = "text-history" {
    specs: [SPEC],
    parser: |b| {
        Stateless(TextHistoryParser {
            host: b.settings.host.clone(),
        })
    },
    discovery: DiscoverySpec::system(&[Location::new("input", "text-history")]),
    plan: single,
    reader: appended(driver),
} }

#[cfg(test)]
mod tests;

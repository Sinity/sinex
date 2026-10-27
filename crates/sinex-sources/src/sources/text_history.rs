//! Explicit plain history. Every nonempty complete line is an untimed command.
use crate::binding::{self, InputFormat};
use crate::drivers::append_file::AppendFile;
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use sinex_model::{Class, event_types::CommandImported};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.text-history",
    driver: Driver::AppendFile,
    excisable: false,
    purpose: "explicit untimed command history",
    class: Class::Guarded,
    parser: "text-history",
    parser_version: 1,
    event_types: &["command.imported"],
    framer: crate::Framer::incremental(frame),
};

pub struct TextHistoryParser {
    pub host: String,
}

impl Parser for TextHistoryParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = CommandImported;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        _: &RecordContext<'_>,
    ) -> Result<(Vec<CommandImported>, ()), RecordError> {
        let text = std::str::from_utf8(record)
            .map_err(|_| RecordError::new("text history is not UTF-8"))?;
        let text = text
            .strip_suffix('\n')
            .ok_or_else(|| RecordError::new("unfinished text history line"))?;
        let text = text.strip_suffix('\r').unwrap_or(text);
        if text.is_empty() {
            return Ok((vec![], ()));
        }
        Ok((
            vec![CommandImported {
                command: text.into(),
                hostname: self.host.clone(),
                started_at: None,
                header_elapsed: None,
                paths: vec![],
                n: 0,
            }],
            (),
        ))
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    crate::drivers::export_archive::Framing::Lines.frame(bytes, false)
}

pub fn source(host: String) -> DeclaredSource<TextHistoryParser> {
    DeclaredSource {
        spec: SPEC,
        parser: TextHistoryParser { host },
    }
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: SPEC.framer,
    }
}

pub const FORMAT: InputFormat = InputFormat {
    name: "text-history",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| Ok(binding::single(&SPEC, input, settings))),
    source: |b| Ok(Box::new(source(b.settings.host.clone()))),
    reader: Some(|b| {
        Ok(binding::appended(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;

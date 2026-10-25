//! Google Takeout Chrome history (`Takeout/Chrome/BrowserHistory.json`, or
//! `History.json` in older exports), on `ExportArchive`: the elements of its
//! `Browser History` array. Times are microseconds since the Unix epoch.
use super::fields;
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::browser::BrowserHistoryEntry;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "browser.takeout-chrome-history",
    driver: Driver::ExportArchive,
    consumer: "browsing recall before live capture",
    class: Class::Open,
    parser: "takeout-chrome-history",
    parser_version: 1,
    event_types: &["browser.history_entry"],
    framer: crate::Framer::export(
        frame,
        crate::drivers::export_archive::Framing::JsonArray("/Browser History"),
    ),
};

const KNOWN: &[&str] = &[
    "url",
    "title",
    "time_usec",
    "page_transition",
    "client_id",
    "favicon_url",
];

pub struct TakeoutChromeParser;

impl Parser for TakeoutChromeParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = BrowserHistoryEntry;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<BrowserHistoryEntry>, ()), RecordError> {
        let payload = BrowserHistoryEntry {
            url: fields::text(row, "url")?,
            title: fields::optional_text(row, "title"),
            visited_at: fields::from_unix_us(fields::int(row, "time_usec")?)?,
            page_transition: fields::optional_text(row, "page_transition"),
            client_id: fields::optional_text(row, "client_id"),
            favicon_url: fields::optional_text(row, "favicon_url"),
            extra: fields::extra(row, KNOWN),
        };
        Ok((vec![payload], ()))
    }
}

pub fn source() -> DeclaredSource<TakeoutChromeParser> {
    DeclaredSource {
        spec: SPEC,
        parser: TakeoutChromeParser,
    }
}

/// The entries of a stored `BrowserHistory.json`.
pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::JsonArray("/Browser History").frame(bytes, true)
}

pub fn select(member: &str) -> bool {
    member.ends_with("Chrome/BrowserHistory.json") || member.ends_with("Chrome/History.json")
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select,
        framing: Framing::JsonArray("/Browser History"),
        part_bytes: PART_BYTES,
        join: None,
    }
}

/// `--source takeout-chrome=PATH`: each Takeout Chrome history in PATH.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "takeout-chrome",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC,
            input,
            settings,
            false,
            crate::binding::any,
        ))
    }),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::stateless(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;

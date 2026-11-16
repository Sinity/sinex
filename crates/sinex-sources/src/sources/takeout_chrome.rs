//! Google Takeout Chrome history (`Takeout/Chrome/BrowserHistory.json`, or
//! `History.json` in older exports), on `ExportArchive`: the elements of its
//! `Browser History` array. Times are microseconds since the Unix epoch.
use super::fields;
use serde_json::{Map, Value};
use sinex_model::event_types::browser::BrowserHistoryEntry;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::stage::Structure;
use sinex_source_core::{Framer, RecordError, Stateless, StatelessParser};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "browser.takeout-chrome-history",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "browsing recall before live capture",
    parser: "takeout-chrome-history",
    event_types: &["browser.history_entry"],
    framer: Framer::export(frame, Framing::JsonArray("/Browser History")),
    ..SourceSpec::DEFAULT
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

impl StatelessParser for TakeoutChromeParser {
    type Record<'a> = Map<String, Value>;
    type Payload = BrowserHistoryEntry;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _context: &sinex_source_core::RecordContext<'_>,
    ) -> Result<Vec<BrowserHistoryEntry>, RecordError> {
        let payload = BrowserHistoryEntry {
            url: fields::text(row, "url")?,
            title: fields::optional_text(row, "title"),
            visited_at: fields::from_unix_us(fields::int(row, "time_usec")?)?,
            page_transition: fields::optional_text(row, "page_transition"),
            client_id: fields::optional_text(row, "client_id"),
            favicon_url: fields::optional_text(row, "favicon_url"),
            extra: fields::extra(row, KNOWN),
        };
        Ok(vec![payload])
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
    }
}

super::input_format! {
    /// `stage --parser takeout-chrome PATH`: each Takeout Chrome history in PATH.
    pub FORMAT = "takeout-chrome" {
    specs: [SPEC],
    parser: |_b| Stateless(TakeoutChromeParser),
    discovery: DiscoverySpec::system(&[Location::new("input", "takeout-chrome")]),
    accepts: [Structure::ArchiveLayout(select)],
    plan: per_file,
    reader: shared(driver),
} }

#[cfg(test)]
mod tests;

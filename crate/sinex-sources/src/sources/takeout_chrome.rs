//! Google Takeout Chrome history (`Takeout/Chrome/BrowserHistory.json`, or
//! `History.json` in older exports), on `ExportArchive`: the elements of its
//! `Browser History` array. Times are microseconds since the Unix epoch.
use super::fields;
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::browser::BrowserHistoryEntry;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "browser.takeout-chrome-history",
    driver: Driver::ExportArchive,
    consumer: "browsing recall before live capture",
    class: Class::Open,
    parser: "takeout-chrome-history",
    parser_version: 1,
    event_types: &["browser.history_entry"],
    framer: frame,
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

pub fn source() -> Declared<TakeoutChromeParser> {
    Declared {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_an_entry_and_keeps_new_fields() {
        let row = json!({"favicon_url": "https://a/f.ico", "page_transition": "LINK", "title": "A",
            "url": "https://a/", "client_id": "c==", "time_usec": 1_565_000_000_123_456i64,
            "page_transition_qualifier": "CLIENT_REDIRECT"});
        let (p, ()) = TakeoutChromeParser
            .parse(row.as_object().unwrap(), &())
            .unwrap();
        assert_eq!(p[0].visited_at.unix_ns(), 1_565_000_000_123_456_000);
        assert_eq!(p[0].page_transition.as_deref(), Some("LINK"));
        assert_eq!(
            p[0].extra.get("page_transition_qualifier"),
            Some(&json!("CLIENT_REDIRECT"))
        );
    }

    #[test]
    fn selects_both_member_names() {
        assert!(select("Takeout/Chrome/BrowserHistory.json"));
        assert!(select("Takeout/Chrome/History.json"));
        assert!(!select("Takeout/Chrome/Bookmarks.html"));
    }
}

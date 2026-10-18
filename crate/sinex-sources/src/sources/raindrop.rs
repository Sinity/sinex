//! Raindrop bookmarks, on `ExportArchive`: the CSV export the web app writes
//! (`id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite`)
//! and the API export (`raindrops-all.jsonl`, `raindrops-trash.jsonl`, one
//! raindrop object per line). Both are exports of one service and key on its
//! ID, so they are one source: every export, in either format, is a reading of
//! the same bookmarks, and the newest export's reading is the live one.
use super::fields;
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::kinds::media::BookmarkSaved;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "reading.raindrop",
    driver: Driver::ExportArchive,
    consumer: "saved reading and URL recall",
    class: Class::Open,
    parser: "raindrop-export",
    parser_version: 1,
    event_types: &["bookmark.saved"],
    framer: frame,
};

/// Raindrop's trash collection.
const TRASH_COLLECTION: i64 = -99;

/// Reads a CSV row or an API JSON line. A CSV file's header row comes first
/// and is carried to the rows after it; JSON lines carry nothing.
pub struct RaindropParser;

impl Parser for RaindropParser {
    type Record<'a> = &'a [u8];
    type Carry = Vec<String>;
    type Payload = BookmarkSaved;

    fn parse(
        &self,
        record: &&[u8],
        header: &Vec<String>,
    ) -> Result<(Vec<BookmarkSaved>, Vec<String>), RecordError> {
        if record.trim_ascii_start().starts_with(b"{") {
            let row: Map<String, Value> = serde_json::from_slice(record)
                .map_err(|e| RecordError::new(format!("not a raindrop object: {e}")))?;
            return Ok((vec![api_row(&row)?], header.clone()));
        }
        csv_row(record, header)
    }
}

const CSV_KNOWN: &[&str] = &[
    "id",
    "url",
    "title",
    "note",
    "excerpt",
    "folder",
    "tags",
    "created",
    "cover",
    "highlights",
    "favorite",
];

fn csv_row(
    record: &[u8],
    header: &[String],
) -> Result<(Vec<BookmarkSaved>, Vec<String>), RecordError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(record);
    let row = reader
        .records()
        .next()
        .ok_or_else(|| RecordError::new("empty CSV record"))?
        .map_err(|e| RecordError::new(format!("CSV: {e}")))?;
    let cells: Vec<String> = row.iter().map(str::to_owned).collect();
    if header.is_empty() {
        let mut names = cells;
        if let Some(first) = names.first_mut() {
            *first = first.trim_start_matches('\u{feff}').to_owned();
        }
        return Ok((vec![], names));
    }
    let mut object = Map::new();
    for (i, name) in header.iter().enumerate() {
        object.insert(
            name.clone(),
            Value::String(cells.get(i).cloned().unwrap_or_default()),
        );
    }
    let t = |name| fields::optional_text(&object, name);
    let tags = t("tags")
        .map(|s| {
            s.split(',')
                .map(|t| t.trim().to_owned())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let payload = BookmarkSaved {
        service: "raindrop".into(),
        native_id: fields::text(&object, "id")?,
        url: fields::text(&object, "url")?,
        title: t("title"),
        note: t("note"),
        excerpt: t("excerpt"),
        folder: t("folder"),
        tags,
        created_at: t("created").map(|c| fields::rfc3339(&c)).transpose()?,
        updated_at: None,
        cover: t("cover"),
        highlights: t("highlights").map(Value::String),
        favorite: t("favorite").map(|f| f == "true"),
        trashed_at: None,
        extra: fields::extra(&object, CSV_KNOWN),
    };
    Ok((vec![payload], header.to_vec()))
}

const API_KNOWN: &[&str] = &[
    "_id",
    "link",
    "title",
    "note",
    "excerpt",
    "tags",
    "created",
    "lastUpdate",
    "cover",
    "highlights",
    "important",
    "removed",
];

fn api_row(row: &Map<String, Value>) -> Result<BookmarkSaved, RecordError> {
    let t = |name| fields::optional_text(row, name);
    let id = match row.get("_id") {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        other => return Err(RecordError::new(format!("_id is not an ID: {other:?}"))),
    };
    let collection = row
        .get("collection")
        .and_then(|c| c.get("$id"))
        .and_then(Value::as_i64)
        .or_else(|| row.get("collectionId").and_then(Value::as_i64));
    let updated_at = t("lastUpdate").map(|u| fields::rfc3339(&u)).transpose()?;
    let trashed =
        fields::optional_bool(row, "removed") == Some(true) || collection == Some(TRASH_COLLECTION);
    Ok(BookmarkSaved {
        service: "raindrop".into(),
        native_id: id,
        url: fields::text(row, "link")?,
        title: t("title"),
        note: t("note"),
        excerpt: t("excerpt"),
        // The API names the collection by ID only (kept under `extra`); its
        // name is in the export's collections file, not on the bookmark.
        folder: None,
        tags: row
            .get("tags")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        created_at: t("created").map(|c| fields::rfc3339(&c)).transpose()?,
        updated_at,
        cover: t("cover"),
        highlights: row
            .get("highlights")
            .filter(|h| h.as_array().is_none_or(|a| !a.is_empty()))
            .cloned(),
        favorite: fields::optional_bool(row, "important"),
        trashed_at: if trashed {
            // The export gives no trash time; the last update stands for it.
            Some(updated_at.unwrap_or_else(sinex_model::Timestamp::now))
        } else {
            None
        },
        extra: fields::extra(row, API_KNOWN),
    })
}

/// A stored export's records: JSON lines, or CSV rows.
pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    if bytes.trim_ascii_start().starts_with(b"{") {
        Framing::Lines.frame(bytes, true)
    } else {
        Framing::Csv.frame(bytes, true)
    }
}

pub fn source() -> Declared<RaindropParser> {
    Declared {
        spec: SPEC,
        parser: RaindropParser,
    }
}

/// A CSV export (`.csv`) or an API export (`.jsonl`), by the file's name.
pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    let jsonl = path.extension().is_some_and(|e| e == "jsonl");
    ExportArchive {
        path,
        host,
        select: if jsonl {
            |m| m.ends_with(".jsonl")
        } else {
            |m| m.ends_with(".csv")
        },
        framing: if jsonl { Framing::Lines } else { Framing::Csv },
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn csv_header_is_carried_then_rows_read() {
        let (none, header) = RaindropParser
            .parse(
                &&b"\xef\xbb\xbfid,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite\n"[..],
                &vec![],
            )
            .unwrap();
        assert!(none.is_empty());
        assert_eq!(header[0], "id");
        let row = b"1302755715,\"Save, the World\",,In theory,https://a/,Unsorted,\"ai, ea\",2025-08-17T13:19:33.851Z,,,false\n";
        let (p, _) = RaindropParser.parse(&&row[..], &header).unwrap();
        assert_eq!(p[0].native_id, "1302755715");
        assert_eq!(p[0].title.as_deref(), Some("Save, the World"));
        assert_eq!(p[0].tags, vec!["ai", "ea"]);
        assert_eq!(p[0].favorite, Some(false));
        assert_eq!(p[0].note, None);
    }

    #[test]
    fn api_lines_and_the_trash() {
        let row = json!({"_id": 1302755715, "link": "https://a/", "title": "A", "excerpt": "", "note": "",
            "tags": ["ai"], "important": false, "removed": false, "created": "2026-10-11T06:10:25.715Z",
            "lastUpdate": "2026-10-11T06:10:29.856Z", "collection": {"$id": 39498970}, "highlights": [],
            "domain": "a"});
        let line = format!("{row}\n");
        let (p, carried) = RaindropParser
            .parse(&line.as_bytes(), &vec!["id".into()])
            .unwrap();
        assert_eq!(
            carried,
            vec!["id"],
            "a JSON line leaves the CSV header alone"
        );
        assert_eq!(
            p[0].native_id, "1302755715",
            "the same ID as the CSV row above"
        );
        assert_eq!(p[0].highlights, None);
        assert_eq!(p[0].trashed_at, None);
        assert!(p[0].extra.contains_key("collection"));
        let mut trashed = row.as_object().unwrap().clone();
        trashed.insert("collection".into(), json!({"$id": -99}));
        let line = format!("{}\n", Value::Object(trashed));
        let (p, _) = RaindropParser.parse(&line.as_bytes(), &vec![]).unwrap();
        assert!(p[0].trashed_at.is_some());
        assert_eq!(
            sinex_model::EventPayload::record_state(&p[0]),
            sinex_model::RecordState::Removed
        );
    }
}

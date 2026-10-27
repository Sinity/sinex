//! Raindrop bookmarks, on `ExportArchive`: the CSV export the web app writes
//! (`id,title,note,excerpt,url,folder,tags,created,cover,highlights,favorite`)
//! and the API export (`raindrops-all.jsonl`, `raindrops-trash.jsonl`, one
//! raindrop object per line). Both are exports of one service and key on its
//! ID, so they are one source: every export, in either format, is a reading of
//! the same bookmarks, and the newest export's reading is the live one.
use super::fields;
use serde_json::{Map, Value};
use sinex_model::Class;
use sinex_model::event_types::media::BookmarkSaved;
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "reading.raindrop",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "saved reading and URL recall",
    class: Class::Open,
    parser: "raindrop-export",
    parser_version: 1,
    event_types: &["bookmark.saved"],
    framer: sinex_source_core::Framer::detected(frame, framing),
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
        _context: &sinex_source_core::RecordContext<'_>,
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
fn framing(bytes: &[u8]) -> Framing {
    if bytes.trim_ascii_start().starts_with(b"{") {
        Framing::Lines
    } else {
        Framing::Csv
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    framing(bytes).frame(bytes, true)
}

pub fn source() -> DeclaredSource<RaindropParser> {
    DeclaredSource {
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

/// `--read raindrop=PATH`: each Raindrop export in PATH.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "raindrop",
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::per_file(
                &SPEC,
                input,
                settings,
                false,
                sinex_source_core::binding::any,
            ))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::shared(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;

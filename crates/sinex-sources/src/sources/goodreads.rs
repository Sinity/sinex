//! Goodreads library export (`library_export.csv`), on `ExportArchive`. Each
//! book gives `book.event` with action `shelved` (`Date Added`) and, when read,
//! `read` (`Date Read`); both are days. ISBNs come wrapped as `="…"`, and an
//! unrated book has rating 0.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use sinex_model::Class;
use sinex_model::event_types::media::BookEvent;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "reading.goodreads",
    driver: Driver::ExportArchive,
    consumer: "books shelved and read, for long-horizon interest",
    class: Class::Open,
    parser: "goodreads-library",
    parser_version: 1,
    event_types: &["book.event"],
    framer: frame,
};

pub struct GoodreadsParser;

impl Parser for GoodreadsParser {
    type Record<'a> = &'a [u8];
    /// The header row, read first.
    type Carry = Vec<String>;
    type Payload = BookEvent;

    fn parse(
        &self,
        record: &&[u8],
        header: &Vec<String>,
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<BookEvent>, Vec<String>), RecordError> {
        let cells = fields::csv_cells(record)?;
        if header.is_empty() {
            return Ok((vec![], cells));
        }
        let get = |name: &str| -> Option<String> {
            header
                .iter()
                .position(|h| h == name)
                .and_then(|i| cells.get(i))
                .map(|c| unwrap_formula(c))
                .filter(|c| !c.is_empty())
        };
        let int = |name: &str| get(name).and_then(|v| v.parse::<i64>().ok());
        let book_id = get("Book Id").ok_or_else(|| RecordError::new("row without a Book Id"))?;
        let title = get("Title").ok_or_else(|| RecordError::new("row without a Title"))?;
        let added = get("Date Added")
            .map(|d| fields::day(&d))
            .transpose()?
            .flatten();
        let read = get("Date Read")
            .map(|d| fields::day(&d))
            .transpose()?
            .flatten();
        let event = |action: &str, at| BookEvent {
            service: "goodreads".into(),
            book_id: book_id.clone(),
            action: action.into(),
            at,
            title: title.clone(),
            author: get("Author"),
            additional_authors: get("Additional Authors"),
            isbn: get("ISBN"),
            isbn13: get("ISBN13"),
            my_rating: int("My Rating").filter(|r| *r > 0),
            average_rating: get("Average Rating").and_then(|v| v.parse().ok()),
            publisher: get("Publisher"),
            binding: get("Binding"),
            pages: int("Number of Pages"),
            year_published: int("Year Published"),
            original_year: int("Original Publication Year"),
            shelves: get("Bookshelves")
                .map(|s| {
                    s.split(',')
                        .map(|x| x.trim().to_owned())
                        .filter(|x| !x.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            exclusive_shelf: get("Exclusive Shelf"),
            review: get("My Review"),
            private_notes: get("Private Notes"),
            read_count: int("Read Count"),
            owned_copies: int("Owned Copies"),
        };
        let mut out = vec![event("shelved", added)];
        if read.is_some() {
            out.push(event("read", read));
        }
        Ok((out, header.clone()))
    }
}

/// `="9781466855472"` as Goodreads writes ISBNs, or the cell unchanged.
fn unwrap_formula(cell: &str) -> String {
    cell.strip_prefix("=\"")
        .and_then(|c| c.strip_suffix('"'))
        .unwrap_or(cell)
        .trim()
        .to_owned()
}

pub fn source() -> Declared<GoodreadsParser> {
    Declared {
        spec: SPEC,
        parser: GoodreadsParser,
    }
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.ends_with(".csv"),
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

/// `--source goodreads=PATH`: each Goodreads export in PATH.
pub const KIND: crate::binding::Kind = crate::binding::Kind {
    name: "goodreads",
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
        Ok(crate::binding::plain(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;

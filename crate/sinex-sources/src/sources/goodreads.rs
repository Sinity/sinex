//! Goodreads library export (`library_export.csv`), on `ExportArchive`. Each
//! book gives `book.event` with action `shelved` (`Date Added`) and, when read,
//! `read` (`Date Read`); both are days. ISBNs come wrapped as `="…"`, and an
//! unrated book has rating 0.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use sinex_model::Class;
use sinex_model::kinds::media::BookEvent;
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

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER: &[u8] = b"Book Id,Title,Author,ISBN,ISBN13,My Rating,Average Rating,Date Read,Date Added,Bookshelves,Exclusive Shelf\n";

    #[test]
    fn shelved_and_read_books() {
        let (none, header) = GoodreadsParser.parse(&HEADER, &vec![]).unwrap();
        assert!(none.is_empty());
        let row = b"184,\"Equoid (Laundry, #2.9)\",C. S.,\"=\"\"1466855479\"\"\",\"=\"\"\"\"\",0,3.96,,2017/08/12,to-read,to-read\n";
        let (out, _) = GoodreadsParser.parse(&&row[..], &header).unwrap();
        assert_eq!(out.len(), 1, "unread: shelved only");
        assert_eq!(out[0].isbn.as_deref(), Some("1466855479"));
        assert_eq!(out[0].isbn13, None);
        assert_eq!(out[0].my_rating, None, "0 means unrated");
        assert_eq!(out[0].at, fields::day("2017-08-12").unwrap());
        let read = b"185,T,A,,,4,4.1,2020/01/02,2019/12/01,\"x, y\",read\n";
        let (out, _) = GoodreadsParser.parse(&&read[..], &header).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].action, "read");
        assert_eq!(out[1].my_rating, Some(4));
        assert_eq!(out[0].shelves, vec!["x", "y"]);
    }
}

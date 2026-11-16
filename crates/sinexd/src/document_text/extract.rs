//! Telling a document's format from its bytes and path, and reading the text
//! of the formats Sinex reads itself. Markup becomes plain text: block
//! elements end a line, entities are decoded, scripts and styles are dropped.
use crate::{Error, Result};
use sinex_source_core::markup::{MarkupText, element_text, html_text};
use std::io::Read;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Format {
    Pdf,
    Html,
    Markdown,
    Text,
    Docx,
    Odt,
}

impl Format {
    pub(super) fn name(self) -> &'static str {
        match self {
            Format::Pdf => "pdf",
            Format::Html => "html",
            Format::Markdown => "markdown",
            Format::Text => "text",
            Format::Docx => "docx",
            Format::Odt => "odt",
        }
    }
}

fn extension(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default()
}

const ODT_MIMETYPE: &str = "application/vnd.oasis.opendocument.text";

/// Decide eligibility from metadata before reading stored bytes. ZIP needs
/// its bytes to distinguish DOCX/ODT from unrelated archives.
pub(super) fn reads_bytes(media_type: &str, path: &str) -> bool {
    matches!(
        sinex_sources::sources::library::media_type_for_path(media_type, path),
        "application/pdf"
            | "application/zip"
            | "text/plain"
            | "text/html"
            | "application/xhtml+xml"
            | "text/markdown"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.oasis.opendocument.text"
    )
}

/// The document format of a copy, or `None` when it is not a document. The
/// bytes decide ZIP membership; text extensions distinguish document markup
/// from structured data.
pub(super) fn format(media_type: &str, path: &str, bytes: &[u8]) -> Option<Format> {
    match sinex_sources::sources::library::media_type_for_path(media_type, path) {
        "application/pdf" => Some(Format::Pdf),
        "text/html" | "application/xhtml+xml" => Some(Format::Html),
        "text/markdown" => Some(Format::Markdown),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
            Some(Format::Docx)
        }
        "application/vnd.oasis.opendocument.text" => Some(Format::Odt),
        "application/zip" => {
            let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).ok()?;
            if archive.by_name("word/document.xml").is_ok() {
                Some(Format::Docx)
            } else if archive.by_name("content.xml").is_ok()
                && archive
                    .by_name("mimetype")
                    .ok()
                    .map(|m| {
                        // Only a member as long as the expected value can
                        // match, so no more of it is read.
                        let mut s = String::new();
                        let _ = m.take(ODT_MIMETYPE.len() as u64 + 2).read_to_string(&mut s);
                        s
                    })
                    .is_some_and(|m| m.trim() == ODT_MIMETYPE)
            {
                Some(Format::Odt)
            } else {
                None
            }
        }
        "text/plain" => Some(match extension(path).as_str() {
            "html" | "htm" | "xhtml" => Format::Html,
            "md" | "markdown" => Format::Markdown,
            _ => Format::Text,
        }),
        _ => None,
    }
}

/// The title and text of a document in a format read here.
pub(super) fn text(format: Format, bytes: &[u8]) -> Result<(Option<String>, String)> {
    match format {
        Format::Text | Format::Markdown => {
            let text = std::str::from_utf8(bytes)
                .map_err(|e| Error::bad_value(format!("not UTF-8 text: {e}")))?;
            let title = (format == Format::Markdown)
                .then(|| {
                    text.lines()
                        .find_map(|l| l.strip_prefix("# ").map(|t| t.trim().to_owned()))
                })
                .flatten();
            Ok((title, text.trim().to_owned()))
        }
        Format::Html => {
            let html = std::str::from_utf8(bytes)
                .map_err(|e| Error::bad_value(format!("not UTF-8 HTML: {e}")))?;
            let title = element_text(html, "title");
            Ok((title, html_text(html)))
        }
        Format::Docx => Ok((
            None,
            member_text(bytes, "word/document.xml", &["w:p", "w:br", "w:tab"])?,
        )),
        Format::Odt => Ok((
            None,
            member_text(
                bytes,
                "content.xml",
                &["text:p", "text:h", "text:line-break"],
            )?,
        )),
        Format::Pdf => Err(Error::internal("a PDF is read with pdftotext")),
    }
}

/// The text of one member of a ZIP document, read in pieces: the member is
/// never held whole, so memory follows the text it holds, not the size it
/// decompresses to.
fn member_text(bytes: &[u8], name: &str, blocks: &[&str]) -> Result<String> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
    let mut file = archive
        .by_name(name)
        .map_err(|e| Error::from(e).within(name))?;
    let mut reader = MarkupText::new(blocks, &[]);
    let mut chunk = vec![0u8; 64 * 1024];
    let mut carry: Vec<u8> = Vec::new();
    loop {
        let read = file
            .read(&mut chunk)
            .map_err(|e| Error::from(e).within(name))?;
        if read == 0 {
            break;
        }
        carry.extend_from_slice(&chunk[..read]);
        // A character split between two pieces waits for the next one.
        let valid = match std::str::from_utf8(&carry) {
            Ok(text) => text.len(),
            Err(e) if e.error_len().is_none() => e.valid_up_to(),
            Err(_) => return Err(Error::internal(format!("{name}: not UTF-8 text"))),
        };
        reader.feed(std::str::from_utf8(&carry[..valid]).expect("checked above"));
        carry.drain(..valid);
    }
    if !carry.is_empty() {
        return Err(Error::internal(format!("{name}: not UTF-8 text")));
    }
    Ok(reader.finish())
}

//! Telling a document's format from its bytes and path, and reading the text
//! of the formats Sinex reads itself. Markup becomes plain text: block
//! elements end a line, entities are decoded, scripts and styles are dropped.
use crate::{Error, Result};
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
            Ok((
                title,
                markup_text(html, HTML_BLOCKS, &["script", "style", "head"]),
            ))
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

const HTML_BLOCKS: &[&str] = &[
    "p",
    "div",
    "br",
    "li",
    "tr",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "pre",
    "blockquote",
    "section",
    "article",
    "table",
    "ul",
    "ol",
    "dt",
    "dd",
];

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

/// The text inside the first `<name>` element.
fn element_text(markup: &str, name: &str) -> Option<String> {
    let lower = markup.to_ascii_lowercase();
    let open = element_start(&lower, &format!("<{name}"))?;
    let start = open + tag_end(&markup[open..])? + 1;
    let end = start + element_start(&lower[start..], &format!("</{name}"))?;
    let text = decode(&markup[start..end]).trim().to_owned();
    (!text.is_empty()).then_some(text)
}

fn element_start(markup: &str, prefix: &str) -> Option<usize> {
    markup.match_indices(prefix).find_map(|(at, _)| {
        markup
            .as_bytes()
            .get(at + prefix.len())
            .filter(|byte| byte.is_ascii_whitespace() || matches!(**byte, b'>' | b'/'))
            .map(|_| at)
    })
}

fn tag_end(markup: &str) -> Option<usize> {
    let mut quote = None;
    markup.char_indices().find_map(|(index, ch)| {
        match (quote, ch) {
            (Some(open), ch) if ch == open => quote = None,
            (None, '\'' | '"') => quote = Some(ch),
            (None, '>') => return Some(index),
            _ => {}
        }
        None
    })
}

/// Markup as plain text: the text between tags, entities decoded, a line end
/// after each of the `blocks` elements, nothing inside the `dropped` ones,
/// blank lines dropped and runs of spaces collapsed.
pub(super) fn markup_text(markup: &str, blocks: &[&str], dropped: &[&str]) -> String {
    let mut reader = MarkupText::new(blocks, dropped);
    reader.feed(markup);
    reader.finish()
}

/// Reads markup given in pieces of any size. It keeps only the text it has
/// produced and, inside a tag, the part of the tag's name that could still
/// match one of the named elements, so memory follows the text, not the
/// markup. A tag left open at the end gives nothing.
pub(super) struct MarkupText<'a> {
    blocks: &'a [&'a str],
    dropped: &'a [&'a str],
    /// The longest name among `blocks` and `dropped`; a longer tag name
    /// cannot match, so no more of it is kept.
    longest: usize,
    out: String,
    /// The element whose content is being dropped.
    skipping: Option<String>,
    state: State,
}

enum State {
    Text,
    /// After `&`: the reference name read so far.
    Reference(String),
    /// After `<`: the first characters, until they show a comment or a tag.
    Open(String),
    Tag(Tag),
    /// Inside `<!-- … -->`: how many `-` were just read.
    Comment(usize),
}

struct Tag {
    closing: bool,
    name: String,
    name_done: bool,
    quote: Option<char>,
    last: char,
}

impl<'a> MarkupText<'a> {
    pub(super) fn new(blocks: &'a [&'a str], dropped: &'a [&'a str]) -> Self {
        let longest = blocks
            .iter()
            .chain(dropped)
            .map(|name| name.len())
            .max()
            .unwrap_or(0);
        MarkupText {
            blocks,
            dropped,
            longest,
            out: String::new(),
            skipping: None,
            state: State::Text,
        }
    }

    pub(super) fn feed(&mut self, markup: &str) {
        for c in markup.chars() {
            self.char(c);
        }
    }

    pub(super) fn finish(mut self) -> String {
        if let State::Reference(name) = std::mem::replace(&mut self.state, State::Text) {
            self.text('&');
            for c in name.chars() {
                self.text(c);
            }
        }
        self.out
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn text(&mut self, c: char) {
        if self.skipping.is_none() {
            self.out.push(c);
        }
    }

    fn char(&mut self, c: char) {
        match std::mem::replace(&mut self.state, State::Text) {
            State::Text => match c {
                '<' => self.state = State::Open(String::new()),
                // Inside a dropped element nothing is written, references
                // included.
                '&' if self.skipping.is_none() => self.state = State::Reference(String::new()),
                c => self.text(c),
            },
            State::Reference(mut name) => match c {
                ';' => match reference(&name) {
                    Some(decoded) => self.text(decoded),
                    None => {
                        self.text('&');
                        for c in name.chars() {
                            self.text(c);
                        }
                        self.text(';');
                    }
                },
                '&' | '<' => {
                    self.text('&');
                    for c in name.chars() {
                        self.text(c);
                    }
                    self.char(c);
                }
                c if c.is_whitespace() => {
                    self.text('&');
                    for c in name.chars() {
                        self.text(c);
                    }
                    self.text(c);
                }
                c => {
                    name.push(c);
                    self.state = State::Reference(name);
                }
            },
            State::Open(mut start) => {
                start.push(c);
                if start == "!--" {
                    self.state = State::Comment(0);
                } else if "!--".starts_with(start.as_str()) {
                    self.state = State::Open(start);
                } else {
                    self.state = State::Tag(Tag {
                        closing: false,
                        name: String::new(),
                        name_done: false,
                        quote: None,
                        last: '<',
                    });
                    for c in start.chars() {
                        self.char(c);
                    }
                }
            }
            State::Comment(dashes) => {
                self.state = match c {
                    '-' => State::Comment(dashes + 1),
                    '>' if dashes >= 2 => State::Text,
                    _ => State::Comment(0),
                }
            }
            State::Tag(mut tag) => {
                match (tag.quote, c) {
                    (Some(open), c) if c == open => tag.quote = None,
                    (Some(_), _) => {}
                    (None, '\'' | '"') => tag.quote = Some(c),
                    (None, '>') => return self.tag(tag),
                    (None, c) => {
                        if !tag.name_done {
                            if c == '/' && tag.name.is_empty() {
                                tag.closing = true;
                            } else if c.is_whitespace() || c == '/' {
                                tag.name_done = !tag.name.is_empty() || c != '/';
                            } else if tag.name.len() <= self.longest {
                                tag.name.extend(c.to_lowercase());
                            }
                        }
                    }
                }
                if !c.is_whitespace() {
                    tag.last = c;
                }
                self.state = State::Tag(tag);
            }
        }
    }

    fn tag(&mut self, tag: Tag) {
        let self_closing = tag.last == '/';
        if let Some(open) = &self.skipping {
            if tag.closing && *open == tag.name {
                self.skipping = None;
            }
            return;
        }
        if !tag.closing && !self_closing && self.dropped.contains(&tag.name.as_str()) {
            self.skipping = Some(tag.name);
            return;
        }
        if self.blocks.contains(&tag.name.as_str()) {
            self.out.push('\n');
        }
    }
}

/// The character a reference name (between `&` and `;`) stands for.
fn reference(name: &str) -> Option<char> {
    match name {
        "amp" => Some('&'),
        "lt" => Some('<'),
        "gt" => Some('>'),
        "quot" => Some('"'),
        "apos" => Some('\''),
        "nbsp" => Some(' '),
        _ => name
            .strip_prefix("#x")
            .or_else(|| name.strip_prefix("#X"))
            .and_then(|h| u32::from_str_radix(h, 16).ok())
            .or_else(|| name.strip_prefix('#').and_then(|d| d.parse().ok()))
            .and_then(char::from_u32),
    }
}

/// Text with its character references and the common named entities decoded.
fn decode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        rest = &rest[amp..];
        let Some(semi) = rest[1..]
            .find(|c: char| c == ';' || c == '&' || c.is_whitespace())
            .map(|i| i + 1)
            .filter(|i| rest.as_bytes()[*i] == b';')
        else {
            out.push('&');
            rest = &rest[1..];
            continue;
        };
        let decoded = reference(&rest[1..semi]);
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &rest[semi + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

//! Telling a document's format from its bytes and path, and reading the text
//! of the formats Sinex reads itself. Markup becomes plain text: block
//! elements end a line, entities are decoded, scripts and styles are dropped.
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

/// The document format of a copy, or `None` when it is not a document. The
/// bytes decide; the extension only tells markup from plain text.
pub(super) fn format(media_type: &str, path: &str, bytes: &[u8]) -> Option<Format> {
    match media_type {
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
                    .map(|mut m| {
                        let mut s = String::new();
                        let _ = m.read_to_string(&mut s);
                        s
                    })
                    .is_some_and(|m| m.trim() == "application/vnd.oasis.opendocument.text")
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
pub(super) fn text(format: Format, bytes: &[u8]) -> Result<(Option<String>, String), String> {
    match format {
        Format::Text | Format::Markdown => {
            let text = std::str::from_utf8(bytes).map_err(|e| format!("not UTF-8 text: {e}"))?;
            let title = (format == Format::Markdown)
                .then(|| {
                    text.lines()
                        .find_map(|l| l.strip_prefix("# ").map(|t| t.trim().to_owned()))
                })
                .flatten();
            Ok((title, text.trim().to_owned()))
        }
        Format::Html => {
            let html = std::str::from_utf8(bytes).map_err(|e| format!("not UTF-8 HTML: {e}"))?;
            let title = element_text(html, "title");
            Ok((
                title,
                markup_text(html, HTML_BLOCKS, &["script", "style", "head"]),
            ))
        }
        Format::Docx => {
            let xml = member(bytes, "word/document.xml")?;
            Ok((None, markup_text(&xml, &["w:p", "w:br", "w:tab"], &[])))
        }
        Format::Odt => {
            let xml = member(bytes, "content.xml")?;
            Ok((
                None,
                markup_text(&xml, &["text:p", "text:h", "text:line-break"], &[]),
            ))
        }
        Format::Pdf => Err("a PDF is read with pdftotext".into()),
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

fn member(bytes: &[u8], name: &str) -> Result<String, String> {
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|e| e.to_string())?;
    let mut file = archive.by_name(name).map_err(|e| format!("{name}: {e}"))?;
    let mut xml = String::new();
    file.read_to_string(&mut xml)
        .map_err(|e| format!("{name}: {e}"))?;
    Ok(xml)
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
    let mut out = String::new();
    let mut rest = markup;
    let mut skipping: Option<String> = None;
    while let Some(lt) = rest.find('<') {
        if skipping.is_none() {
            out.push_str(&decode(&rest[..lt]));
        }
        if rest[lt..].starts_with("<!--") {
            rest = rest[lt + 4..]
                .find("-->")
                .map_or("", |end| &rest[lt + 4 + end + 3..]);
            continue;
        }
        let gt = tag_end(&rest[lt..]);
        let Some(gt) = gt else {
            if skipping.is_none() {
                out.push_str(&decode(&rest[lt..]));
            }
            rest = "";
            break;
        };
        let tag = &rest[lt + 1..lt + gt];
        rest = &rest[lt + gt + 1..];
        let closing = tag.starts_with('/');
        let name: String = tag
            .trim_start_matches('/')
            .split(|c: char| c.is_whitespace() || c == '/')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        if let Some(open) = &skipping {
            if closing && *open == name {
                skipping = None;
            }
            continue;
        }
        if !closing && !tag.ends_with('/') && dropped.contains(&name.as_str()) {
            skipping = Some(name);
            continue;
        }
        if blocks.contains(&name.as_str()) {
            out.push('\n');
        }
    }
    if skipping.is_none() {
        out.push_str(&decode(rest));
    }
    out.lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
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
        let entity = &rest[1..semi];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "nbsp" => Some(' '),
            _ => entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| entity.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
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

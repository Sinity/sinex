//! Markup text and character references shared by sources and documents.
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

/// The text inside the first `<name>` element.
pub fn element_text(markup: &str, name: &str) -> Option<String> {
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
pub fn text(markup: &str, blocks: &[&str], dropped: &[&str]) -> String {
    let mut reader = MarkupText::new(blocks, dropped);
    reader.feed(markup);
    reader.finish()
}

/// Reads markup given in pieces of any size. It keeps only the text it has
/// produced and, inside a tag, the part of the tag's name that could still
/// match one of the named elements, so memory follows the text, not the
/// markup. A tag left open at the end gives nothing.
pub struct MarkupText<'a> {
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
    pub fn new(blocks: &'a [&'a str], dropped: &'a [&'a str]) -> Self {
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

    pub fn feed(&mut self, markup: &str) {
        for c in markup.chars() {
            self.char(c);
        }
    }

    pub fn finish(mut self) -> String {
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
pub fn decode(text: &str) -> String {
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

pub fn html_text(html: &str) -> String {
    text(html, HTML_BLOCKS, &["script", "style", "head"])
}

#[cfg(test)]
mod tests;

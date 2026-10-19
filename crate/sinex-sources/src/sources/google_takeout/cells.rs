//! The HTML activity cell Google writes in My Activity and YouTube history
//! exports: one `outer-cell` per entry, with the service as its header, a
//! content block whose lines are separated by `<br>` (the title line, any
//! further lines, the time), and a caption block of named sections
//! (`Products:`, `Locations:`, `Details:`).
//!
//! Each export writes every time in the zone of the export day (`Aug 26, 2026,
//! 9:34:44 PM CEST` for an entry from December), so a time is read as an
//! instant at the label's offset. The time line is found by its pattern, never
//! by position: Gemini entries hold multi-line text.
use crate::{RecordError, RecordShape};
use serde::Serialize;
use sinex_model::Timestamp;

/// Where every activity cell starts.
pub const CELL: &[u8] = b"<div class=\"outer-cell";

/// One parsed activity cell.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Cell {
    pub service: String,
    /// The content lines before the time, each with its links.
    pub lines: Vec<Line>,
    pub at: Timestamp,
    pub zone_label: String,
    /// Caption sections by name, in order: `("Products", ["Search"])`.
    pub sections: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Line {
    pub text: String,
    /// `(href, link text)` in order.
    pub links: Vec<(String, String)>,
}

impl Cell {
    pub fn section(&self, name: &str) -> Vec<String> {
        self.sections
            .iter()
            .filter(|(n, _)| n == name)
            .flat_map(|(_, items)| items.iter().cloned())
            .collect()
    }
}

impl<'a> RecordShape<'a> for Cell {
    fn decode(raw: &'a [u8]) -> Result<Self, RecordError> {
        let html = std::str::from_utf8(raw)
            .map_err(|e| RecordError::new(format!("cell is not UTF-8: {e}")))?;
        parse(html)
    }

    /// The parsed cell, with the instant in place of the time text: two
    /// exports that print one moment in different zones read the same.
    fn hash(raw: &[u8], _volatile: &[&str]) -> [u8; 32] {
        match <Cell as RecordShape>::decode(raw) {
            Ok(mut cell) => {
                cell.zone_label.clear();
                let json = serde_json::to_vec(&cell).expect("cell serializes");
                *blake3::hash(&json).as_bytes()
            }
            Err(_) => *blake3::hash(raw).as_bytes(),
        }
    }
}

pub fn parse(html: &str) -> Result<Cell, RecordError> {
    let service = between(html, "mdl-typography--title\">", "</p>")
        .map(|s| text_of(s).trim().to_owned())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| RecordError::new("activity cell without a service header"))?;
    let body = between(html, "mdl-typography--body-1\">", "</div>")
        .ok_or_else(|| RecordError::new("activity cell without a content block"))?;
    let mut lines: Vec<Line> = body
        .split("<br>")
        .map(line)
        .filter(|l| !l.text.is_empty())
        .collect();
    let time_index = lines
        .iter()
        .rposition(|l| time(&l.text).is_some())
        .ok_or_else(|| RecordError::new("activity cell without a time line"))?;
    let (at, zone_label) = time(&lines[time_index].text).expect("found above")?;
    lines.truncate(time_index);
    if lines.is_empty() {
        return Err(RecordError::new("activity cell without a title line"));
    }
    let sections = between(html, "mdl-typography--caption\">", "</div>")
        .map(sections)
        .unwrap_or_default();
    Ok(Cell {
        service,
        lines,
        at,
        zone_label,
        sections,
    })
}

/// The text after the first `start` up to the next `end`.
fn between<'a>(s: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let from = s.find(start)? + start.len();
    let to = s[from..].find(end).map_or(s.len(), |i| from + i);
    Some(&s[from..to])
}

fn line(raw: &str) -> Line {
    let mut links = Vec::new();
    let mut rest = raw;
    while let Some(i) = rest.find("<a href=\"") {
        let after = &rest[i + 9..];
        let Some(q) = after.find('"') else { break };
        let href = decode(&after[..q]);
        let Some(gt) = after[q..].find('>') else {
            break;
        };
        let inner_start = q + gt + 1;
        let close = after[inner_start..]
            .find("</a>")
            .map_or(after.len(), |j| inner_start + j);
        links.push((href, text_of(&after[inner_start..close]).trim().to_owned()));
        rest = &after[close.min(after.len())..];
    }
    Line {
        text: text_of(raw).trim().to_owned(),
        links,
    }
}

fn sections(caption: &str) -> Vec<(String, Vec<String>)> {
    caption
        .split("<b>")
        .filter_map(|part| {
            let (name, items) = part.split_once("</b>")?;
            let name = text_of(name).trim().trim_end_matches(':').to_owned();
            let items = items
                .split("<br>")
                .map(|i| text_of(i).trim().to_owned())
                .filter(|i| !i.is_empty())
                .collect();
            Some((name, items))
        })
        .collect()
}

/// Text with tags removed, entities decoded and every kind of space (no-break,
/// em, narrow no-break) made a plain one.
pub fn text_of(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' if in_tag => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    decode(&out)
        .chars()
        .map(|c| if c.is_whitespace() { ' ' } else { c })
        .collect()
}

/// HTML character references.
pub fn decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        // Searched as bytes: `;` is ASCII, and a cut of the text a few bytes
        // on could fall inside a multi-byte character.
        let Some(semi) = tail.as_bytes()[..tail.len().min(12)]
            .iter()
            .position(|b| *b == b';')
        else {
            out.push('&');
            rest = &tail[1..];
            continue;
        };
        let name = &tail[1..semi];
        let decoded = match name {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "nbsp" | "emsp" | "ensp" | "thinsp" => Some(' '),
            _ => name
                .strip_prefix("#x")
                .or_else(|| name.strip_prefix("#X"))
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| name.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &tail[semi + 1..];
            }
            None => {
                out.push('&');
                rest = &tail[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// `Aug 26, 2026, 9:34:44 PM CEST` as an instant and its zone label, or
/// `None` if the line is not a time. A time line with a zone this reader does
/// not know is an error, never a guess.
pub fn time(line: &str) -> Option<Result<(Timestamp, String), RecordError>> {
    let normal: String = line.split_whitespace().collect::<Vec<_>>().join(" ");
    let (date, rest) = normal.rsplit_once(", ")?;
    let (month_day, year) = date.rsplit_once(", ")?;
    let (month, day) = month_day.split_once(' ')?;
    let month = MONTHS.iter().position(|m| *m == month)? as u8 + 1;
    let day: u8 = day.parse().ok()?;
    let year: i32 = year.parse().ok()?;
    let mut parts = rest.split(' ');
    let clock = parts.next()?;
    let meridiem = parts.next()?;
    let zone = parts.next()?;
    if parts.next().is_some() || !matches!(meridiem, "AM" | "PM") {
        return None;
    }
    let mut hms = clock.split(':').map(str::parse::<u8>);
    let (Some(Ok(h)), Some(Ok(m)), Some(Ok(s)), None) =
        (hms.next(), hms.next(), hms.next(), hms.next())
    else {
        return None;
    };
    if !(1..=12).contains(&h) {
        return None;
    }
    let hour = match (h, meridiem) {
        (12, "AM") => 0,
        (12, "PM") => 12,
        (h, "PM") => h + 12,
        (h, _) => h,
    };
    Some(instant(year, month, day, hour, m, s, zone))
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

fn instant(
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    zone: &str,
) -> Result<(Timestamp, String), RecordError> {
    let offset_hours: i8 = match zone {
        "UTC" | "GMT" | "WET" => 0,
        "CET" | "BST" | "WEST" => 1,
        "CEST" | "EET" => 2,
        "EEST" | "MSK" => 3,
        "EDT" => -4,
        "EST" | "CDT" => -5,
        "CST" | "MDT" => -6,
        "MST" | "PDT" => -7,
        "PST" => -8,
        other => {
            return Err(RecordError::new(format!(
                "unknown time zone label {other:?}"
            )));
        }
    };
    let month = time::Month::try_from(month).map_err(|e| RecordError::new(e.to_string()))?;
    let date = time::Date::from_calendar_date(year, month, day)
        .map_err(|e| RecordError::new(format!("invalid date: {e}")))?;
    let clock = time::Time::from_hms(hour, minute, second)
        .map_err(|e| RecordError::new(format!("invalid time: {e}")))?;
    let offset = time::UtcOffset::from_hms(offset_hours, 0, 0)
        .map_err(|e| RecordError::new(e.to_string()))?;
    Ok((
        Timestamp(time::PrimitiveDateTime::new(date, clock).assume_offset(offset)),
        zone.to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEARCH: &str = r#"<div class="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp"><div class="mdl-grid"><div class="header-cell mdl-cell mdl-cell--12-col"><p class="mdl-typography--title">Search<br></p></div><div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1">Searched for <a href="https://www.google.com/search?q=a+%26+b">a &amp; b</a><br>Aug 26, 2026, 9:34:44 PM CEST<br></div><div class="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1 mdl-typography--text-right"></div><div class="content-cell mdl-cell mdl-cell--12-col mdl-typography--caption"><b>Products:</b><br>&emsp;Search<br><b>Locations:</b><br>&emsp;At <a href="https://maps.example/x">this general area</a> - From your device<br></div></div></div>"#;

    #[test]
    fn reads_a_search_cell() {
        let c = parse(SEARCH).unwrap();
        assert_eq!(c.service, "Search");
        assert_eq!(c.lines.len(), 1);
        assert_eq!(c.lines[0].text, "Searched for a & b");
        assert_eq!(
            c.lines[0].links,
            vec![(
                "https://www.google.com/search?q=a+%26+b".to_owned(),
                "a & b".to_owned()
            )]
        );
        assert_eq!(
            c.at,
            crate::sources::fields::rfc3339("2026-08-26T19:34:44Z").unwrap()
        );
        assert_eq!(c.zone_label, "CEST");
        assert_eq!(c.section("Products"), vec!["Search"]);
        assert_eq!(c.section("Locations").len(), 1);
    }

    #[test]
    fn a_bare_ampersand_before_multibyte_text() {
        assert_eq!(decode("Kraków & Łódź żółć"), "Kraków & Łódź żółć");
        assert_eq!(decode("a &amp; ż&#x17c;"), "a & żż");
    }

    #[test]
    fn one_moment_in_two_zones_hashes_the_same() {
        let utc = SEARCH.replace(
            "Aug 26, 2026, 9:34:44 PM CEST",
            "Aug 26, 2026, 7:34:44 PM UTC",
        );
        assert_eq!(
            <Cell as RecordShape>::hash(SEARCH.as_bytes(), &[]),
            <Cell as RecordShape>::hash(utc.as_bytes(), &[])
        );
        let other = SEARCH.replace("9:34:44", "9:34:45");
        assert_ne!(
            <Cell as RecordShape>::hash(SEARCH.as_bytes(), &[]),
            <Cell as RecordShape>::hash(other.as_bytes(), &[])
        );
    }

    #[test]
    fn times_with_narrow_spaces_noon_and_midnight() {
        let (t, z) = time("Dec 15, 2025, 12:05:21\u{202f}AM CET")
            .unwrap()
            .unwrap();
        assert_eq!(z, "CET");
        assert_eq!(
            t,
            crate::sources::fields::rfc3339("2025-12-14T23:05:21Z").unwrap()
        );
        let (t, _) = time("Aug 7, 2019, 12:34:48 PM UTC").unwrap().unwrap();
        assert_eq!(
            t,
            crate::sources::fields::rfc3339("2019-08-07T12:34:48Z").unwrap()
        );
        assert!(time("Searched for Aug 7, 2019").is_none());
        assert!(time("Aug 7, 2019, 4:34:48 PM XYZT").unwrap().is_err());
    }

    #[test]
    fn the_time_line_is_found_by_pattern_in_multiline_text() {
        let html = SEARCH.replace(
            "<br>Aug 26, 2026",
            "<br>first answer line<br>second line<br>Aug 26, 2026",
        );
        let c = parse(&html).unwrap();
        assert_eq!(c.lines.len(), 3);
        assert_eq!(c.lines[2].text, "second line");
    }
}

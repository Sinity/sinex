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
use serde::Serialize;
use sinex_model::Timestamp;
#[cfg(test)]
use sinex_source_core::markup::decode;
use sinex_source_core::{RecordError, RecordResult, RecordShape};

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
        let html = std::str::from_utf8(raw).record("cell is not UTF-8")?;
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
        .map(|s| sinex_source_core::markup::html_text(s).trim().to_owned())
        .filter(|s| !s.is_empty())
        .record("activity cell without a service header")?;
    let body = between(html, "mdl-typography--body-1\">", "</div>")
        .record("activity cell without a content block")?;
    let mut lines: Vec<Line> = body
        .split("<br>")
        .map(line)
        .filter(|l| !l.text.is_empty())
        .collect();
    let time_index = lines
        .iter()
        .rposition(|l| time(&l.text).is_some())
        .record("activity cell without a time line")?;
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
        let href = sinex_source_core::markup::decode(&after[..q]);
        let Some(gt) = after[q..].find('>') else {
            break;
        };
        let inner_start = q + gt + 1;
        let close = after[inner_start..]
            .find("</a>")
            .map_or(after.len(), |j| inner_start + j);
        links.push((
            href,
            sinex_source_core::markup::html_text(&after[inner_start..close])
                .trim()
                .to_owned(),
        ));
        rest = &after[close.min(after.len())..];
    }
    Line {
        text: sinex_source_core::markup::html_text(raw).trim().to_owned(),
        links,
    }
}

fn sections(caption: &str) -> Vec<(String, Vec<String>)> {
    caption
        .split("<b>")
        .filter_map(|part| {
            let (name, items) = part.split_once("</b>")?;
            let name = sinex_source_core::markup::html_text(name)
                .trim()
                .trim_end_matches(':')
                .to_owned();
            let items = items
                .split("<br>")
                .map(|i| sinex_source_core::markup::html_text(i).trim().to_owned())
                .filter(|i| !i.is_empty())
                .collect();
            Some((name, items))
        })
        .collect()
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
    let month = time::Month::try_from(month).record("invalid time")?;
    let date = time::Date::from_calendar_date(year, month, day).record("invalid date")?;
    let clock = time::Time::from_hms(hour, minute, second).record("invalid time")?;
    let offset = time::UtcOffset::from_hms(offset_hours, 0, 0).record("invalid time")?;
    Ok((
        Timestamp(time::PrimitiveDateTime::new(date, clock).assume_offset(offset)),
        zone.to_owned(),
    ))
}

#[cfg(test)]
mod tests;

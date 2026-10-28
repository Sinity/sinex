//! kitty scrollback snapshots in the Sinnix lake, on `DirSnapshot`.
//!
//! A timer runs `kitty @ get-text --ansi --extent all` for every window of
//! every kitty and writes `<YYYYMMDDTHHMMSSZ>-<host>[-pid<kitty pid>]-win<id>-
//! <title>.ansi` with a `.meta.json` sidecar (window, kitty process, title,
//! working directory, time, host). Each run stores every window's whole
//! scrollback again. Each `.ansi` file is one `scrollback.captured` whose text
//! is only what is new since the previous snapshot of the same window
//! ([`Windows`]); each sidecar is one `scrollback.window`. The kitty
//! integration captures the same thing live (`session.integrations`).
use crate::sources::fields;
use crate::sources::terminal_text::strip_escapes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::event_types::machine::{ScrollbackCaptured, ScrollbackWindow};
use sinex_model::{Class, Timestamp};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordError};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "terminal.kitty-scrollback",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "terminal recall: what kitty windows showed",
    // What terminals show includes what was typed into them.
    class: Class::Guarded,
    parser: "kitty-scrollback",
    parser_version: 1,
    event_types: &["scrollback.captured", "scrollback.window"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_whole,
        sinex_source_core::drivers::export_archive::Framing::Whole,
    ),
};

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Snapshot {
        Captured(ScrollbackCaptured),
        Window(ScrollbackWindow),
    }
}

/// What a snapshot's file name says.
#[derive(Clone, Debug, PartialEq)]
pub struct Name {
    pub captured_at: Timestamp,
    pub host: String,
    pub kitty_pid: Option<u32>,
    pub window_id: i64,
}

/// Read `<YYYYMMDDTHHMMSSZ>-<host>[-pid<n>]-win<n>-<title>.<suffix>`. The
/// host may contain dashes, so the window is the first `-win<digits>` that
/// ends at a dash or a dot.
pub fn name(file: &str) -> Option<Name> {
    let (stamp, rest) = file.split_at_checked(16)?;
    let rest = rest.strip_prefix('-')?;
    let b = stamp.as_bytes();
    if b[8] != b'T' || b[15] != b'Z' {
        return None;
    }
    let iso = format!(
        "{}-{}-{}T{}:{}:{}Z",
        &stamp[0..4],
        &stamp[4..6],
        &stamp[6..8],
        &stamp[9..11],
        &stamp[11..13],
        &stamp[13..15]
    );
    let captured_at = fields::rfc3339(&iso).ok()?;
    let (before, window_id) = rest.match_indices("-win").find_map(|(at, _)| {
        let digits: String = rest[at + 4..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let after = rest[at + 4 + digits.len()..].chars().next();
        (!digits.is_empty() && matches!(after, Some('-' | '.')))
            .then(|| (&rest[..at], digits.parse::<i64>().ok()))
    })?;
    let (host, kitty_pid) = match before.rsplit_once("-pid") {
        Some((host, pid)) if !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit()) => {
            (host, pid.parse().ok())
        }
        _ => (before, None),
    };
    Some(Name {
        captured_at,
        host: host.to_owned(),
        kitty_pid,
        window_id: window_id?,
    })
}

/// The lines a window held, escape sequences and trailing spaces removed,
/// without the screen's empty rows at the end.
pub fn plain_lines(ansi: &str) -> Vec<String> {
    let mut text = String::with_capacity(ansi.len());
    strip_escapes(ansi, &mut text);
    let mut lines: Vec<String> = text.split('\n').map(|l| l.trim_end().to_owned()).collect();
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

/// How many of the last lines of a snapshot are remembered one by one: at
/// least a screen, whose rows change between snapshots, while the lines above
/// it only grow.
const TAIL_LINES: usize = 200;

/// How long a window is remembered after its last snapshot.
const WINDOW_SECS: i128 = 86_400;

/// What the previous snapshot of one window held: a hash of all but its last
/// [`TAIL_LINES`] lines, and a hash of each of those.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Previous {
    captured_ns: i128,
    prefix_lines: usize,
    prefix: String,
    tail: Vec<u64>,
}

/// The previous snapshot of each window seen within [`WINDOW_SECS`] of the
/// newest, by host, kitty process and window. Snapshot files sort by time,
/// and the drain reads them in that order.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Windows {
    windows: BTreeMap<String, Previous>,
}

fn prefix_hash(lines: &[String]) -> String {
    let mut hasher = blake3::Hasher::new();
    for line in lines {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().to_hex().to_string()
}

fn line_hash(line: &str) -> u64 {
    let hash = blake3::hash(line.as_bytes());
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("eight bytes"))
}

impl Previous {
    fn of(lines: &[String], captured_ns: i128) -> Self {
        let prefix_lines = lines.len().saturating_sub(TAIL_LINES);
        Self {
            captured_ns,
            prefix_lines,
            prefix: prefix_hash(&lines[..prefix_lines]),
            tail: lines[prefix_lines..].iter().map(|l| line_hash(l)).collect(),
        }
    }

    /// Where the lines new since this snapshot start in `lines`, when this
    /// snapshot is how `lines` begins (up to its changing last rows).
    fn continued_at(&self, lines: &[String]) -> Option<usize> {
        if lines.len() < self.prefix_lines
            || prefix_hash(&lines[..self.prefix_lines]) != self.prefix
        {
            return None;
        }
        let mut common = self.prefix_lines;
        for hash in &self.tail {
            match lines.get(common) {
                Some(line) if line_hash(line) == *hash => common += 1,
                _ => break,
            }
        }
        // Nothing of a non-empty snapshot is left where it was.
        (common > 0 || self.tail.is_empty()).then_some(common)
    }
}

impl Windows {
    /// The new lines of a snapshot of `window` and whether they continue its
    /// previous snapshot, with the windows after it.
    fn read(&self, window: String, captured_ns: i128, lines: &[String]) -> (usize, bool, Self) {
        let continued = self
            .windows
            .get(&window)
            .and_then(|previous| previous.continued_at(lines));
        let mut next = self.clone();
        next.windows.retain(|_, previous| {
            previous.captured_ns >= captured_ns - WINDOW_SECS * 1_000_000_000
        });
        next.windows
            .insert(window, Previous::of(lines, captured_ns));
        (continued.unwrap_or(0), continued.is_some(), next)
    }
}

pub struct ScrollbackParser;

impl Parser for ScrollbackParser {
    type Record<'a> = &'a [u8];
    type Carry = Windows;
    type Payload = Snapshot;

    fn parse(
        &self,
        record: &&[u8],
        windows: &Windows,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<Snapshot>, Windows), RecordError> {
        let member = context
            .member
            .ok_or_else(|| RecordError::new("a snapshot needs its directory snapshot member"))?;
        // A snapshot removed from the lake was still taken; its events stay.
        if member.details.contains_key("removed_at_ns") {
            return Ok((vec![], windows.clone()));
        }
        let file = member.path.rsplit('/').next().unwrap_or(&member.path);
        let named =
            name(file).ok_or_else(|| RecordError::new(format!("not a snapshot name: {file}")))?;
        if file.ends_with(".meta.json") {
            let sidecar: Map<String, Value> = serde_json::from_slice(record)
                .map_err(|e| RecordError::new(format!("sidecar is not a JSON object: {e}")))?;
            let text = |key: &str| fields::optional_text(&sidecar, key);
            let snapshot = text("ansi_file").unwrap_or_else(|| {
                format!("{}.ansi", file.strip_suffix(".meta.json").unwrap_or(file))
            });
            return Ok((
                vec![Snapshot::Window(ScrollbackWindow {
                    captured_at: named.captured_at,
                    host: text("hostname").or(Some(named.host)),
                    kitty_pid: sidecar
                        .get("kitty_pid")
                        .and_then(Value::as_u64)
                        .and_then(|p| u32::try_from(p).ok())
                        .or(named.kitty_pid),
                    window_id: sidecar
                        .get("window_id")
                        .and_then(Value::as_i64)
                        .unwrap_or(named.window_id),
                    title: text("title"),
                    cwd: text("cwd"),
                    snapshot,
                    extra: fields::extra(
                        &sidecar,
                        &[
                            "window_id",
                            "kitty_pid",
                            "title",
                            "cwd",
                            "captured_at",
                            "hostname",
                            "ansi_file",
                        ],
                    ),
                })],
                windows.clone(),
            ));
        }
        let ansi = std::str::from_utf8(record)
            .map_err(|e| RecordError::new(format!("snapshot is not UTF-8: {e}")))?;
        let lines = plain_lines(ansi);
        let window = format!(
            "{}/{}/{}",
            named.host,
            named.kitty_pid.map(|p| p.to_string()).unwrap_or_default(),
            named.window_id
        );
        let (start, continues, next) = windows.read(window, named.captured_at.unix_ns(), &lines);
        let title = file
            .split_once(&format!("win{}-", named.window_id))
            .map(|(_, title)| title.strip_suffix(".ansi").unwrap_or(title).to_owned());
        Ok((
            vec![Snapshot::Captured(ScrollbackCaptured {
                captured_at: named.captured_at,
                host: Some(named.host),
                kitty_pid: named.kitty_pid,
                window_id: named.window_id,
                title,
                cwd: None,
                lines: lines.len() as u64,
                continues,
                new_lines: (lines.len() - start) as u64,
                text: lines[start..].join("\n"),
                extra: Map::new(),
            })],
            next,
        ))
    }
}

pub fn source() -> DeclaredSource<ScrollbackParser> {
    DeclaredSource {
        spec: SPEC,
        parser: ScrollbackParser,
    }
}

/// Snapshot text and sidecars; the HTML renderings beside some are made from
/// the text.
pub fn select(path: &str) -> bool {
    path.ends_with(".ansi") || path.ends_with(".meta.json")
}

pub fn driver(root: PathBuf, host: String) -> DirSnapshot {
    DirSnapshot {
        root,
        host,
        removed_at_source: false,
        select,
        part_bytes: 64 << 20,
    }
}

/// `--read kitty-scrollback=DIR`: the lake's scrollback snapshots in DIR.
pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "kitty-scrollback",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[sinex_source_core::discovery::Location {
                root: "capture",
                path: "kitty-scrollback",
            }],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &[],
            request_only: false,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: Some(|input, settings| {
            Ok(sinex_source_core::binding::single(&SPEC, input, settings))
        }),
        source: |_| Ok(Box::new(source())),
        reader: Some(|b| {
            Ok(sinex_source_core::binding::walked(driver(
                b.input.clone(),
                b.settings.host.clone(),
            )))
        }),
        capture: None,
    };

#[cfg(test)]
mod tests;

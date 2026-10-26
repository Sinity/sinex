//! Coverage: the stretches of time in which a source could have recorded what
//! it records. A metric's value for a day is only as good as its sources'
//! coverage of that day, so a day with no coverage is missing, never zero.
//! Coverage is computed from records Sinex already keeps (runs, reads and the
//! records themselves), never stored as events.
use crate::span::Span;
use serde::{Deserialize, Serialize};

/// How a source captures, which decides how its coverage is bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMode {
    /// Live capture: covered while the runs that captured it were running.
    Live,
    /// A polled store (a history database or file): from its earliest record
    /// to the last time Sinex read it; each read sees everything before it.
    Polled,
    /// An export: from its earliest record through its latest record (the
    /// export date is not stored separately yet, so the records declare it).
    Export,
}

/// How much of what a source records it sees while it is covering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completeness {
    /// Everything of this kind (a shell history for commands typed on that host).
    Complete,
    /// A subset (one browser profile, not all browsing).
    Sample,
    /// Whether the source saw the whole span is unknown.
    Unknown,
}

/// What each source sees of its kind. A shell history sees the commands typed
/// in that shell, not every command run; a browser profile sees that profile;
/// the journal, the compositor and ActivityWatch see this host.
pub fn completeness(source: &str) -> Completeness {
    match source {
        "machine.journald"
        | "desktop.activitywatch"
        | "desktop.hyprland"
        | "media.spotify-history"
        | "media.mpris"
        | "desktop.media" => Completeness::Complete,
        "terminal.atuin-history"
        | "terminal.zsh-history"
        | "code.git"
        | "browser.chromium-visits"
        | "browser.takeout-chrome-history"
        | "browser.navigation"
        | "communication.weechat-log"
        | "communication.mbox"
        | "communication.maildir" => Completeness::Sample,
        _ => Completeness::Unknown,
    }
}

/// How each source captures, which bounds its coverage. The journal and
/// ActivityWatch are stores with history read through a cursor, so a read
/// sees everything before it; the compositor socket, the media players and
/// the filesystem watch see only while they run.
pub fn capture_mode(source: &str) -> CaptureMode {
    match source {
        "desktop.hyprland" | "media.mpris" | "machine.file-watch" => CaptureMode::Live,
        "media.spotify-history"
        | "browser.takeout-chrome-history"
        | "reading.raindrop"
        | "communication.mbox"
        | "communication.weechat-log" => CaptureMode::Export,
        _ => CaptureMode::Polled,
    }
}

/// One stretch in which a source could have recorded what it records.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Coverage {
    pub source: String,
    pub span: Span,
    pub completeness: Completeness,
    /// What bounds the stretch (run lifetime, earliest record to last read,
    /// export records).
    pub bounded_by: String,
}

/// What Sinex holds about one source, from which its coverage is computed.
#[derive(Clone, Debug, Default)]
pub struct CoverageEvidence {
    /// Earliest and latest `ts_orig` of the source's live events.
    pub records: Option<Span>,
    /// When Sinex last read the source's input.
    pub last_read: Option<i64>,
    /// The lifetimes of live-capture runs (an open run ends at its latest record).
    pub runs: Vec<Span>,
}

/// The coverage of `source` from its evidence.
pub fn coverage(
    source: &str,
    mode: CaptureMode,
    completeness: Completeness,
    e: &CoverageEvidence,
) -> Vec<Coverage> {
    let row = |span: Span, bounded_by: &str| Coverage {
        source: source.to_owned(),
        span,
        completeness,
        bounded_by: bounded_by.to_owned(),
    };
    match mode {
        CaptureMode::Live => e.runs.iter().map(|s| row(*s, "run lifetime")).collect(),
        CaptureMode::Polled => match (e.records, e.last_read) {
            (Some(r), Some(read)) => vec![row(
                Span::new(r.start, read.max(r.end)),
                "earliest record to last read",
            )],
            (Some(r), None) => vec![row(Span::new(r.start, r.end + 1), "records")],
            _ => Vec::new(),
        },
        CaptureMode::Export => e
            .records
            .map(|r| row(Span::new(r.start, r.end + 1), "export records"))
            .into_iter()
            .collect(),
    }
}

#[cfg(test)]
mod tests;

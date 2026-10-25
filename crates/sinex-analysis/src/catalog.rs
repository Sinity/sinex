//! The shipped catalog: metrics declared once, the completeness of each source,
//! and the recipes and standing analyses built on them. This is the counterpart
//! of Lynchpin's day panel (`operator_daily`) for the sources Sinex captures
//! today: one metric per field instead of a wide table, every cell with its
//! state.
use crate::coverage::{CaptureMode, Completeness};
use crate::metric::{
    AggLaw, Allocation, CoverageRule, IntervalShape, Lit, MetricDef, Predicate, Quantity,
    SourceRole, Term, UnitGrain,
};

const SHELLS: &[&str] = &["terminal.atuin-history", "terminal.zsh-history"];
const ACTIVITYWATCH: &[&str] = &["desktop.activitywatch"];
const SPOTIFY: &[&str] = &["media.spotify-history"];
const GIT: &[&str] = &["code.git"];
const JOURNALD: &[&str] = &["machine.journald"];
const CHROMIUM: &[&str] = &["browser.chromium-visits"];
const IRC: &[&str] = &["communication.weechat-log"];
const MAIL: &[&str] = &["communication.mbox", "communication.maildir"];

const EXIT_KNOWN: Predicate = Predicate::Present { field: "exit_code" };
const EXIT_FAILED: Predicate = Predicate::All {
    preds: &[
        EXIT_KNOWN,
        Predicate::Ne {
            field: "exit_code",
            value: Lit::Num(0.0),
        },
    ],
};
const NOT_AFK: Predicate = Predicate::Eq {
    field: "status",
    value: Lit::Str("not-afk"),
};
const SAID: Predicate = Predicate::OneOf {
    field: "kind",
    values: &["message", "action"],
};
/// syslog priorities 0-3: emergency, alert, critical, error.
const JOURNAL_ERROR: Predicate = Predicate::Le {
    field: "priority",
    value: 3.0,
};

const fn day(
    id: &'static str,
    description: &'static str,
    relation: &'static str,
    law: AggLaw,
) -> MetricDef {
    MetricDef {
        id,
        version: 1,
        description,
        relation,
        filter: None,
        unit: UnitGrain::Day,
        quantity: Quantity::Count,
        law,
        interval: None,
        allocation: Allocation::Instant,
        coverage: CoverageRule::AnyOf(SHELLS),
        dimensions: &[],
        role: SourceRole::Measured,
        deviation: false,
    }
}

pub const SHELL_COMMANDS: MetricDef = MetricDef {
    coverage: CoverageRule::AnyOf(SHELLS),
    dimensions: &["host"],
    deviation: true,
    ..day(
        "shell.commands",
        "Commands run",
        "command.fused",
        AggLaw::Count,
    )
};

pub const SHELL_FAILED_RATIO: MetricDef = MetricDef {
    quantity: Quantity::Ratio,
    ..day(
        "shell.failed_ratio",
        "Share of commands with a known exit code that failed",
        "command.fused",
        AggLaw::Ratio {
            num: Term::Count {
                filter: Some(&EXIT_FAILED),
            },
            den: Term::Count {
                filter: Some(&EXIT_KNOWN),
            },
        },
    )
};

pub const SHELL_DIRECTORIES: MetricDef = MetricDef {
    ..day(
        "shell.directories",
        "Distinct working directories",
        "command.fused",
        AggLaw::Distinct { field: "cwd" },
    )
};

pub const ACTIVE_SECONDS: MetricDef = MetricDef {
    quantity: Quantity::Seconds,
    filter: Some(NOT_AFK),
    interval: Some(IntervalShape::EndField { field: "ended_at" }),
    allocation: Allocation::ClipInterval,
    coverage: CoverageRule::Sources(ACTIVITYWATCH),
    deviation: true,
    ..day(
        "activity.active_seconds",
        "Time at the machine",
        "presence.span",
        AggLaw::UnionDuration,
    )
};

pub const FOCUS_SECONDS: MetricDef = MetricDef {
    quantity: Quantity::Seconds,
    interval: Some(IntervalShape::EndField { field: "ended_at" }),
    allocation: Allocation::ClipInterval,
    coverage: CoverageRule::Sources(ACTIVITYWATCH),
    dimensions: &["app"],
    ..day(
        "focus.seconds",
        "Time some window had focus, time away from the machine included",
        "window.span",
        AggLaw::UnionDuration,
    )
};

pub const FOCUS_SWITCHES: MetricDef = MetricDef {
    coverage: CoverageRule::Sources(ACTIVITYWATCH),
    deviation: true,
    ..day(
        "focus.switches",
        "Window focus changes",
        "window.span",
        AggLaw::Count,
    )
};

pub const MUSIC_SECONDS: MetricDef = MetricDef {
    quantity: Quantity::Seconds,
    interval: Some(IntervalShape::EndField { field: "ended_at" }),
    allocation: Allocation::ClipInterval,
    coverage: CoverageRule::Sources(SPOTIFY),
    ..day(
        "music.listen_seconds",
        "Time listening",
        "music.played",
        AggLaw::UnionDuration,
    )
};

pub const MUSIC_PLAYS: MetricDef = MetricDef {
    coverage: CoverageRule::Sources(SPOTIFY),
    ..day(
        "music.plays",
        "Tracks played",
        "music.played",
        AggLaw::Count,
    )
};

pub const GIT_COMMITS: MetricDef = MetricDef {
    coverage: CoverageRule::Sources(GIT),
    dimensions: &["repository"],
    ..day("git.commits", "Commits made", "git.commit", AggLaw::Count)
};

pub const JOURNAL_ERRORS: MetricDef = MetricDef {
    filter: Some(JOURNAL_ERROR),
    coverage: CoverageRule::Sources(JOURNALD),
    dimensions: &["unit"],
    ..day(
        "machine.journal_errors",
        "Journal entries at error priority or worse",
        "journal.entry",
        AggLaw::Count,
    )
};

pub const WEB_VISITS: MetricDef = MetricDef {
    coverage: CoverageRule::Sources(CHROMIUM),
    deviation: true,
    ..day(
        "web.visits",
        "Pages visited",
        "browser.visit",
        AggLaw::Count,
    )
};

pub const GIT_REPOSITORIES: MetricDef = MetricDef {
    coverage: CoverageRule::Sources(GIT),
    ..day(
        "git.repositories",
        "Repositories committed to",
        "git.commit",
        AggLaw::Distinct {
            field: "repository",
        },
    )
};

pub const CHAT_LINES: MetricDef = MetricDef {
    filter: Some(SAID),
    coverage: CoverageRule::Sources(IRC),
    dimensions: &["buffer"],
    ..day(
        "chat.lines",
        "Chat lines said (messages and actions)",
        "chat.message",
        AggLaw::Count,
    )
};

pub const MAIL_MESSAGES: MetricDef = MetricDef {
    coverage: CoverageRule::AnyOf(MAIL),
    ..day(
        "mail.messages",
        "Mail messages",
        "mail.message",
        AggLaw::Count,
    )
};

/// Every shipped metric.
pub const METRICS: &[MetricDef] = &[
    SHELL_COMMANDS,
    SHELL_FAILED_RATIO,
    SHELL_DIRECTORIES,
    ACTIVE_SECONDS,
    FOCUS_SECONDS,
    FOCUS_SWITCHES,
    MUSIC_SECONDS,
    MUSIC_PLAYS,
    GIT_COMMITS,
    GIT_REPOSITORIES,
    CHAT_LINES,
    MAIL_MESSAGES,
    JOURNAL_ERRORS,
    WEB_VISITS,
];

pub fn metric(id: &str) -> Option<&'static MetricDef> {
    METRICS.iter().find(|m| m.id == id)
}

/// What each source sees of its kind. A shell history sees the commands typed
/// in that shell, not every command run; a browser profile sees that profile;
/// the journal and ActivityWatch see this host.
pub fn completeness(source: &str) -> Completeness {
    match source {
        "machine.journald" | "desktop.activitywatch" | "media.spotify-history" => {
            Completeness::Complete
        }
        "terminal.atuin-history"
        | "terminal.zsh-history"
        | "code.git"
        | "browser.chromium-visits"
        | "communication.weechat-log"
        | "communication.mbox"
        | "communication.maildir" => Completeness::Sample,
        _ => Completeness::Unknown,
    }
}

/// How each source captures, which bounds its coverage. The journal and ActivityWatch
/// are stores with history read through a cursor, so a read sees everything
/// before it; the window-manager socket and the filesystem watch see only
/// while they run.
pub fn capture_mode(source: &str) -> CaptureMode {
    match source {
        "desktop.hyprland" | "machine.file-watch" => CaptureMode::Live,
        "media.spotify-history"
        | "browser.takeout-chrome-history"
        | "reading.raindrop"
        | "communication.mbox"
        | "communication.weechat-log" => CaptureMode::Export,
        _ => CaptureMode::Polled,
    }
}

/// Every source whose coverage a shipped metric depends on.
pub fn coverage_sources() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = METRICS
        .iter()
        .flat_map(|m| m.coverage.sources().iter().copied())
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// The relations the shipped metrics read.
pub fn relations() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = METRICS.iter().map(|m| m.relation).collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// The correlation family the standing lifestyle analysis tests: every pair of
/// deviation-tracked metrics at lags 0 and 1, fixed before any result is seen
/// (Lynchpin's `lifestyle_correlations` and `daytime_correlates`, as one
/// declared family).
pub fn correlation_family() -> Vec<(&'static str, &'static str, i32)> {
    let tracked: Vec<&'static str> = METRICS
        .iter()
        .filter(|m| m.deviation)
        .map(|m| m.id)
        .collect();
    let mut out = Vec::new();
    for (i, x) in tracked.iter().enumerate() {
        for y in &tracked[i + 1..] {
            for lag in [0, 1] {
                out.push((*x, *y, lag));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests;

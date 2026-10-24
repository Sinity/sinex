//! Units and their fields (spec `query.md` §3). Every unit is one table of
//! fields; the parser, the compiler, the read-back and every surface read the
//! same table, so they cannot disagree. The SQL each field lowers to lives in
//! `sinex-store::query`, keyed by unit and field name, and a test there checks
//! that every field here has one.
use crate::{Class, DbCheck, RecordState, TimeQuality};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    Id,
    Text,
    /// A path: `^` means "is or lies under", by whole components.
    Path,
    /// Recorded paths: equality or component-prefix matches any member.
    PathList,
    Integer,
    Number,
    Bool,
    Time,
    /// A closed set: a value outside it is refused, never answered empty.
    Enum(&'static [&'static str]),
    IdList,
    /// The payload object itself; compared through `payload.<path>`.
    Json,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Field {
    pub name: &'static str,
    pub ty: FieldType,
    /// How the field reads in a sentence: "exit code", "when it happened".
    pub phrase: &'static str,
}

const fn f(name: &'static str, ty: FieldType, phrase: &'static str) -> Field {
    Field { name, ty, phrase }
}

const ORIGINS: &[&str] = &["material", "derived"];
const ARCHIVE_REASONS: &[&str] = &[
    "replay",
    "newer_reading",
    "hide",
    "excise",
    "version_change",
];
const RUN_STATUS: &[&str] = &["running", "finished"];
const FAILURE_STAGES: &[&str] = &["parse"];

macro_rules! event_fields {
    ($($extra:expr),* $(,)?) => {
        &[
            f("id", FieldType::Id, "event ID"),
            f("kind", FieldType::Text, "kind"),
            f("source", FieldType::Text, "source"),
            f("host", FieldType::Text, "host"),
            f("when", FieldType::Time, "time it happened"),
            f("time_quality", FieldType::Enum(TimeQuality::VALUES), "time quality"),
            f("seen", FieldType::Time, "time Sinex read it"),
            f("written", FieldType::Time, "time it was written"),
            f("origin", FieldType::Enum(ORIGINS), "origin"),
            f("material", FieldType::Id, "material"),
            f("anchor", FieldType::Integer, "byte offset"),
            f("ordinal", FieldType::Integer, "output number"),
            f("parents", FieldType::IdList, "parent events"),
            f("producer", FieldType::Text, "producer"),
            f("run", FieldType::Id, "run"),
            f("text", FieldType::Text, "text"),
            f("key", FieldType::Text, "equivalence key"),
            f("class", FieldType::Enum(Class::VALUES), "privacy class"),
            f("state", FieldType::Enum(RecordState::VALUES), "record state"),
            f("commit_seq", FieldType::Integer, "commit position"),
            f("payload", FieldType::Json, "payload"),
            $($extra,)*
        ]
    };
}

const EVENTS: &[Field] = event_fields!();
const ARCHIVE: &[Field] = event_fields!(
    f(
        "baseline",
        FieldType::Bool,
        "visibility seeded during an upgrade"
    ),
    f(
        "known_from",
        FieldType::Integer,
        "position from which this visibility is known"
    ),
    f("archived", FieldType::Time, "time it left the live set"),
    f("reason", FieldType::Enum(ARCHIVE_REASONS), "archive reason"),
    f("operation", FieldType::Id, "operation"),
    f("replaced_by", FieldType::Id, "replacing event"),
    f(
        "archived_seq",
        FieldType::Integer,
        "commit position it left at"
    ),
);
const MATERIALS: &[Field] = &[
    f("id", FieldType::Id, "material ID"),
    f("binding", FieldType::Text, "binding"),
    f("parser", FieldType::Text, "parser"),
    f("parser_version", FieldType::Integer, "parser version"),
    f("hash", FieldType::Text, "content hash"),
    f("size", FieldType::Integer, "size in bytes"),
    f("staged", FieldType::Time, "time it was stored"),
    f("origin_path", FieldType::Path, "path it was read from"),
    f("host", FieldType::Text, "host"),
    f("part", FieldType::Integer, "part number"),
    f("continues", FieldType::Id, "previous part"),
    f("events", FieldType::Integer, "live events"),
    f("first", FieldType::Time, "earliest event time"),
    f("last", FieldType::Time, "latest event time"),
];
const RUNS: &[Field] = &[
    f("id", FieldType::Id, "run ID"),
    f("producer", FieldType::Text, "producer"),
    f("producer_version", FieldType::Text, "producer version"),
    f("host", FieldType::Text, "host"),
    f("started", FieldType::Time, "start"),
    f("ended", FieldType::Time, "end"),
    f("status", FieldType::Enum(RUN_STATUS), "status"),
    f("events", FieldType::Integer, "events emitted"),
    f("failed", FieldType::Integer, "records that failed"),
];
const SOURCES: &[Field] = &[
    f("binding", FieldType::Text, "binding"),
    f("source", FieldType::Text, "source"),
    f("driver", FieldType::Text, "driver"),
    f("version", FieldType::Integer, "version"),
    f("kinds", FieldType::Text, "event kinds"),
    f("consumer", FieldType::Text, "consumer"),
    f("events", FieldType::Integer, "live events"),
    f("first", FieldType::Time, "oldest event time"),
    f("last", FieldType::Time, "newest event time"),
    f("last_event", FieldType::Time, "newest event time"),
    f(
        "last_seen",
        FieldType::Time,
        "time the newest event was read",
    ),
    f(
        "failed_24h",
        FieldType::Integer,
        "failures in the last 24 hours",
    ),
];
const OPERATIONS: &[Field] = &[
    f("id", FieldType::Id, "operation ID"),
    f("kind", FieldType::Text, "kind"),
    f("actor", FieldType::Text, "actor"),
    f("scope", FieldType::Text, "scope"),
    f("started", FieldType::Time, "start"),
];
const FAILURES: &[Field] = &[
    f("id", FieldType::Integer, "failure ID"),
    f("when", FieldType::Time, "time it failed"),
    f("stage", FieldType::Enum(FAILURE_STAGES), "stage"),
    f("source", FieldType::Text, "source"),
    f("material", FieldType::Id, "material"),
    f("anchor", FieldType::Integer, "byte offset"),
    f("reason", FieldType::Text, "reason"),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unit {
    Events,
    Archive,
    Materials,
    Runs,
    Sources,
    Operations,
    Failures,
}

impl Unit {
    pub const ALL: [Unit; 7] = [
        Unit::Events,
        Unit::Archive,
        Unit::Materials,
        Unit::Runs,
        Unit::Sources,
        Unit::Operations,
        Unit::Failures,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Unit::Events => "events",
            Unit::Archive => "archive",
            Unit::Materials => "materials",
            Unit::Runs => "runs",
            Unit::Sources => "sources",
            Unit::Operations => "operations",
            Unit::Failures => "failures",
        }
    }

    pub fn from_name(name: &str) -> Option<Unit> {
        Unit::ALL.into_iter().find(|u| u.name() == name)
    }

    /// What the unit reads as in a sentence.
    pub fn phrase(self) -> &'static str {
        match self {
            Unit::Events => "live events",
            Unit::Archive => "archived readings",
            Unit::Materials => "stored materials",
            Unit::Runs => "runs",
            Unit::Sources => "sources",
            Unit::Operations => "operations",
            Unit::Failures => "records that failed",
        }
    }

    pub fn fields(self) -> &'static [Field] {
        match self {
            Unit::Events => EVENTS,
            Unit::Archive => ARCHIVE,
            Unit::Materials => MATERIALS,
            Unit::Runs => RUNS,
            Unit::Sources => SOURCES,
            Unit::Operations => OPERATIONS,
            Unit::Failures => FAILURES,
        }
    }

    /// The time `since`, `until` and `in` apply to when no field is named.
    pub fn main_time(self) -> &'static str {
        match self {
            Unit::Events | Unit::Archive | Unit::Failures => "when",
            Unit::Materials => "staged",
            Unit::Runs | Unit::Operations => "started",
            Unit::Sources => "last_event",
        }
    }

    /// Whether rows are events, with payloads, text and provenance.
    pub fn is_events(self) -> bool {
        matches!(self, Unit::Events | Unit::Archive)
    }

    /// Fields shown when the query names none.
    pub fn default_fields(self) -> &'static [&'static str] {
        match self {
            Unit::Events => &["id", "kind", "source", "when", "payload"],
            Unit::Archive => &[
                "id",
                "kind",
                "source",
                "when",
                "archived",
                "reason",
                "baseline",
                "known_from",
                "payload",
            ],
            Unit::Materials => &["id", "binding", "parser", "size", "staged", "events"],
            Unit::Runs => &[
                "id", "producer", "host", "started", "ended", "status", "events",
            ],
            Unit::Sources => &[
                "binding",
                "driver",
                "kinds",
                "events",
                "last_event",
                "last_seen",
                "failed_24h",
            ],
            Unit::Operations => &["id", "kind", "actor", "scope", "started"],
            Unit::Failures => &["id", "when", "source", "material", "anchor", "reason"],
        }
    }

    pub fn field(self, name: &str) -> Option<&'static Field> {
        self.fields().iter().find(|f| f.name == name)
    }
}

/// A unit alias: `events` with fixed kinds and a few promoted fields
/// (spec `query.md` §3.6). Adding one is a line here and its field SQL in the
/// store; never a code path.
#[derive(Debug, PartialEq)]
pub struct Alias {
    pub name: &'static str,
    pub kinds: &'static [&'static str],
    pub phrase: &'static str,
    pub fields: &'static [Field],
    /// Whether rows have a span (`start`, `end`), for `during` and `containing`.
    pub spans: bool,
    /// An extra fixed condition beyond the kind, by promoted field: `now` is
    /// `intervals where open = true`.
    pub open_only: bool,
}

pub const ALIASES: &[Alias] = &[
    Alias {
        name: "commands",
        kinds: &["command.fused"],
        phrase: "commands, each once however many shells saw it",
        fields: &[
            f("command", FieldType::Text, "command"),
            f("cwd", FieldType::Path, "working directory"),
            f("exit_code", FieldType::Integer, "exit code"),
            f("duration", FieldType::Number, "duration in seconds"),
            f("start", FieldType::Time, "start"),
            f("end", FieldType::Time, "end"),
            f("session", FieldType::Text, "shell session"),
            f("observed_by", FieldType::Text, "observing sources"),
        ],
        spans: true,
        open_only: false,
    },
    Alias {
        name: "shell-sessions",
        kinds: &["shell.session"],
        phrase: "recorded shell command groups",
        fields: &[
            f("host", FieldType::Text, "host"),
            f("start", FieldType::Time, "first recorded command start"),
            f(
                "observed_until",
                FieldType::Time,
                "latest recorded command extent",
            ),
            f("duration", FieldType::Number, "observed extent in seconds"),
            f("day", FieldType::Text, "logical day"),
            f("commands", FieldType::Integer, "member commands"),
            f("failed", FieldType::Integer, "failed member commands"),
            f("cwds", FieldType::PathList, "member working directories"),
        ],
        spans: true,
        open_only: false,
    },
    Alias {
        name: "sessions",
        kinds: &["activity.session.boundary"],
        phrase: "activity sessions",
        fields: &[
            f("start", FieldType::Time, "start"),
            f("end", FieldType::Time, "end"),
            f("duration", FieldType::Number, "duration in seconds"),
            f("project", FieldType::Text, "project"),
            f("open", FieldType::Bool, "still open"),
            f(
                "depth",
                FieldType::Enum(&["deep", "moderate", "shallow", "hyperfocus"]),
                "depth",
            ),
        ],
        spans: true,
        open_only: false,
    },
    Alias {
        name: "intervals",
        kinds: &["state.interval"],
        phrase: "state intervals",
        fields: &[
            f("start", FieldType::Time, "start"),
            f("end", FieldType::Time, "end"),
            f("duration", FieldType::Number, "duration in seconds"),
            f("state", FieldType::Text, "state"),
            f("subject", FieldType::Text, "subject"),
            f("project", FieldType::Text, "project"),
            f("open", FieldType::Bool, "still open"),
        ],
        spans: true,
        open_only: false,
    },
    Alias {
        name: "now",
        kinds: &["state.interval"],
        phrase: "intervals still open",
        fields: &[
            f("start", FieldType::Time, "start"),
            f("end", FieldType::Time, "end"),
            f("duration", FieldType::Number, "duration in seconds"),
            f("state", FieldType::Text, "state"),
            f("subject", FieldType::Text, "subject"),
            f("project", FieldType::Text, "project"),
            f("open", FieldType::Bool, "still open"),
        ],
        spans: true,
        open_only: true,
    },
    Alias {
        name: "days",
        kinds: &["activity.summary.daily"],
        phrase: "daily summaries",
        fields: &[
            f("day", FieldType::Text, "logical day"),
            f("active", FieldType::Number, "active seconds"),
            f("top_sources", FieldType::Text, "top sources"),
        ],
        spans: false,
        open_only: false,
    },
    Alias {
        name: "items",
        kinds: &["library.item"],
        phrase: "library items",
        fields: &[
            f("item", FieldType::Text, "item"),
            f("label", FieldType::Text, "label"),
            f("revision", FieldType::Integer, "revision"),
            f("content", FieldType::Text, "content hash"),
            f("media_type", FieldType::Text, "media type"),
        ],
        spans: false,
        open_only: false,
    },
    Alias {
        name: "media",
        kinds: &["library.acquisition"],
        phrase: "files seen in library folders",
        fields: &[
            f("path", FieldType::Path, "path"),
            f("media_type", FieldType::Text, "media type"),
            f("size", FieldType::Integer, "size in bytes"),
            f("width", FieldType::Integer, "width"),
            f("height", FieldType::Integer, "height"),
            f("duration", FieldType::Number, "duration in seconds"),
            f("content", FieldType::Text, "content hash"),
            f("stored", FieldType::Bool, "bytes stored"),
        ],
        spans: false,
        open_only: false,
    },
    Alias {
        name: "assignments",
        kinds: &["library.assignment"],
        phrase: "library tags and property values",
        fields: &[
            f("item", FieldType::Text, "item"),
            f("field", FieldType::Text, "field"),
            f("value", FieldType::Text, "value"),
            f(
                "level",
                FieldType::Enum(&["override", "import", "proposal"]),
                "level",
            ),
        ],
        spans: false,
        open_only: false,
    },
    Alias {
        name: "pages",
        kinds: &["page.visited"],
        phrase: "page visits",
        fields: &[
            f("url", FieldType::Text, "URL"),
            f("title", FieldType::Text, "title"),
            f("domain", FieldType::Text, "domain"),
        ],
        spans: false,
        open_only: false,
    },
];

impl Alias {
    pub fn from_name(name: &str) -> Option<&'static Alias> {
        ALIASES.iter().find(|a| a.name == name)
    }

    pub fn field(&self, name: &str) -> Option<&'static Field> {
        self.fields.iter().find(|f| f.name == name)
    }
}

/// Words that start a command rather than a query (spec `query.md` §2): a
/// query beginning with one is that command, never a search for the word.
pub const COMMAND_WORDS: &[&str] = &[
    "recall",
    "panel",
    "compute",
    "brief",
    "context",
    "related",
    "follow",
    "trace",
    "descendants",
    "parents",
    "walk",
    "gaps",
    "duplicates",
];

/// Every word that names a unit, alias or command, for typo suggestions.
pub fn unit_like_words() -> impl Iterator<Item = &'static str> {
    Unit::ALL
        .iter()
        .map(|u| u.name())
        .chain(ALIASES.iter().map(|a| a.name))
        .chain(COMMAND_WORDS.iter().copied())
}

/// Every unit whose closed-set field accepts `value`, as `unit.field`, so a
/// refusal can say where the value belongs.
pub fn units_accepting(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    for unit in Unit::ALL {
        for field in unit.fields() {
            if let FieldType::Enum(values) = field.ty
                && values.contains(&value)
            {
                out.push(format!("{}.{}", unit.name(), field.name));
            }
        }
    }
    for alias in ALIASES {
        for field in alias.fields {
            if let FieldType::Enum(values) = field.ty
                && values.contains(&value)
            {
                out.push(format!("{}.{}", alias.name, field.name));
            }
        }
    }
    out.dedup();
    out
}

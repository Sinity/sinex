//! Tables and their fields. Every table is one table of fields; the parser, the
//! compiler, the read-back and every surface read the same table, so they
//! cannot disagree. The SQL each field lowers to lives in `sinex-store::query`,
//! keyed by table and field name, and a test there checks that every field here
//! has one.
use crate::{ArchiveReason, Class, DbCheck, RecordState, TimeQuality};

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
const RUN_STATUS: &[&str] = &["running", "finished"];

macro_rules! event_fields {
    ($($extra:expr),* $(,)?) => {
        &[
            f("id", FieldType::Id, "event ID"),
            f("event_type", FieldType::Text, "event type"),
            f("source", FieldType::Text, "source"),
            f("host", FieldType::Text, "host"),
            f("when", FieldType::Time, "time it happened"),
            f("time_quality", FieldType::Enum(TimeQuality::VALUES), "time quality"),
            f("seen", FieldType::Time, "time Sinex read it"),
            f("written", FieldType::Time, "time it was written"),
            f("origin", FieldType::Enum(ORIGINS), "origin"),
            f("material", FieldType::Id, "material"),
            f("offset", FieldType::Integer, "byte offset"),
            f("output", FieldType::Integer, "output number"),
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
    f("archived", FieldType::Time, "time it left the live set"),
    f(
        "reason",
        FieldType::Enum(ArchiveReason::VALUES),
        "archive reason"
    ),
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
    f("stored", FieldType::Time, "time it was stored"),
    f("origin_path", FieldType::Path, "path it was read from"),
    f("host", FieldType::Text, "host"),
    f("part", FieldType::Integer, "part number"),
    f("previous_part", FieldType::Id, "previous part"),
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
    f("event_types", FieldType::Text, "event types"),
    f("purpose", FieldType::Text, "purpose"),
    f("events", FieldType::Integer, "live events"),
    f("first", FieldType::Time, "oldest event time"),
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
    f("source", FieldType::Text, "source"),
    f("material", FieldType::Id, "material"),
    f("offset", FieldType::Integer, "byte offset"),
    f("reason", FieldType::Text, "reason"),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Table {
    Events,
    Archive,
    Materials,
    Runs,
    Sources,
    Operations,
    Failures,
}

impl Table {
    pub const ALL: [Table; 7] = [
        Table::Events,
        Table::Archive,
        Table::Materials,
        Table::Runs,
        Table::Sources,
        Table::Operations,
        Table::Failures,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Table::Events => "events",
            Table::Archive => "archive",
            Table::Materials => "materials",
            Table::Runs => "runs",
            Table::Sources => "sources",
            Table::Operations => "operations",
            Table::Failures => "failures",
        }
    }

    pub fn from_name(name: &str) -> Option<Table> {
        Table::ALL.into_iter().find(|u| u.name() == name)
    }

    /// What the table reads as in a sentence.
    pub fn phrase(self) -> &'static str {
        match self {
            Table::Events => "live events",
            Table::Archive => "archived readings",
            Table::Materials => "stored materials",
            Table::Runs => "runs",
            Table::Sources => "sources",
            Table::Operations => "operations",
            Table::Failures => "records that failed",
        }
    }

    pub fn fields(self) -> &'static [Field] {
        match self {
            Table::Events => EVENTS,
            Table::Archive => ARCHIVE,
            Table::Materials => MATERIALS,
            Table::Runs => RUNS,
            Table::Sources => SOURCES,
            Table::Operations => OPERATIONS,
            Table::Failures => FAILURES,
        }
    }

    /// The time `since`, `until` and `in` apply to when no field is named.
    pub fn main_time(self) -> &'static str {
        match self {
            Table::Events | Table::Archive | Table::Failures => "when",
            Table::Materials => "stored",
            Table::Runs | Table::Operations => "started",
            Table::Sources => "last_event",
        }
    }

    /// Whether rows are events, with payloads, text and provenance.
    pub fn is_events(self) -> bool {
        matches!(self, Table::Events | Table::Archive)
    }

    /// Fields shown when the query names none.
    pub fn default_fields(self) -> &'static [&'static str] {
        match self {
            Table::Events => &["id", "event_type", "source", "when", "payload"],
            Table::Archive => &[
                "id",
                "event_type",
                "source",
                "when",
                "archived",
                "reason",
                "payload",
            ],
            Table::Materials => &["id", "binding", "parser", "size", "stored", "events"],
            Table::Runs => &[
                "id", "producer", "host", "started", "ended", "status", "events",
            ],
            Table::Sources => &[
                "binding",
                "driver",
                "event_types",
                "events",
                "last_event",
                "last_seen",
                "failed_24h",
            ],
            Table::Operations => &["id", "kind", "actor", "scope", "started"],
            Table::Failures => &["id", "when", "source", "material", "offset", "reason"],
        }
    }

    pub fn field(self, name: &str) -> Option<&'static Field> {
        self.fields().iter().find(|f| f.name == name)
    }
}

/// A table alias: `events` with fixed kinds and a few promoted fields. Adding
/// one is a line here and its field SQL in the store; never a code path.
#[derive(Debug, PartialEq)]
pub struct Alias {
    pub name: &'static str,
    pub kinds: &'static [&'static str],
    pub phrase: &'static str,
    pub fields: &'static [Field],
    /// Whether rows have a span (`start`, `end`), for `during` and `containing`.
    pub spans: bool,
    /// A fixed payload condition beyond the kind, `(field, value)`: `now` is
    /// `intervals` whose `open` is `true`, `days` are summaries whose `grain`
    /// is `day`.
    pub only: Option<(&'static str, &'static str)>,
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
        only: None,
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
        only: None,
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
            f("open", FieldType::Bool, "still open"),
        ],
        spans: true,
        only: None,
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
            f("open", FieldType::Bool, "still open"),
        ],
        spans: true,
        only: Some(("open", "true")),
    },
    Alias {
        name: "days",
        kinds: &["activity.summary"],
        phrase: "daily activity summaries",
        fields: &[
            f("day", FieldType::Text, "logical day"),
            f("commands", FieldType::Integer, "commands"),
            f("failed_commands", FieldType::Integer, "failed commands"),
            f("sessions", FieldType::Integer, "shell sessions"),
            f("session_seconds", FieldType::Number, "seconds in shell sessions"),
        ],
        spans: false,
        only: Some(("grain", "day")),
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
        only: None,
    },
    Alias {
        name: "media",
        kinds: &["library.copy"],
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
        only: None,
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
        only: None,
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

/// Words that start a command rather than a query: a query beginning with one
/// is that command, never a search for the word.
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

/// Every word that names a table, alias or command, for typo suggestions.
pub fn table_like_words() -> impl Iterator<Item = &'static str> {
    Table::ALL
        .iter()
        .map(|u| u.name())
        .chain(ALIASES.iter().map(|a| a.name))
        .chain(COMMAND_WORDS.iter().copied())
}

/// Every table whose closed-set field accepts `value`, as `table.field`, so a
/// refusal can say where the value belongs.
pub fn tables_accepting(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    for table in Table::ALL {
        for field in table.fields() {
            if let FieldType::Enum(values) = field.ty
                && values.contains(&value)
            {
                out.push(format!("{}.{}", table.name(), field.name));
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

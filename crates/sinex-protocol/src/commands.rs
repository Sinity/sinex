//! Implemented commands advertised by the CLI, API and MCP, with their
//! arguments and required access. All three surfaces use this list.
use serde::{Deserialize, Serialize};

/// Who may call a command. Each of the three fixed tokens grants one level
/// and everything below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    Read,
    /// Writes attributed to a caller.
    Agent,
    /// Irreversible changes: tombstoning.
    Operator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgKind {
    Text,
    /// An event, record, key, spanning-output or time-window citation.
    Reference,
    Integer,
    Bool,
    /// The rest of the command line, joined by spaces: a query, words, a time.
    Rest,
    /// A repeatable option; its values as a list.
    List,
    /// A list of JSON objects: on the command line a repeatable option whose
    /// values are JSON objects, over MCP an array of objects.
    Objects,
}

#[derive(Clone, Copy, Debug)]
pub struct Arg {
    pub name: &'static str,
    pub kind: ArgKind,
    pub required: bool,
    /// Given by position on the command line rather than as `--name`.
    pub positional: bool,
    pub help: &'static str,
}

#[derive(Clone, Copy, Debug)]
pub struct Command {
    /// The method name: the CLI subcommand, the API method and, prefixed with
    /// `sinex_`, the MCP tool.
    pub name: &'static str,
    /// Full command lines used in help and the development fixture.
    pub examples: &'static [&'static str],
    pub summary: &'static str,
    pub args: &'static [Arg],
    pub access: Access,
    /// Offered as an MCP tool. `follow` streams and is not.
    pub mcp: bool,
}

impl Command {
    /// The request's default execution class, shared by every transport.
    pub fn latency(&self) -> command_surface::LatencyClass {
        use command_surface::LatencyClass::*;
        match self.name {
            "status" => Status,
            "query-complete" => Completion,
            "search" | "recall" => Search,
            "query" | "context" | "timeline" | "sql" | "brief" | "forecast-report"
            | "analysis-run" | "analysis-panel" | "analysis-why" | "gaps" | "library-rank" => {
                Aggregate
            }
            _ => Read,
        }
    }
    pub fn default_deadline(&self) -> std::time::Duration {
        use command_surface::LatencyClass::*;
        std::time::Duration::from_millis(match self.latency() {
            Local => 15,
            Status | Read => 100,
            Completion => 50,
            Search => 250,
            Aggregate => 400,
        })
    }
}

const fn pos(name: &'static str, kind: ArgKind, help: &'static str) -> Arg {
    Arg {
        name,
        kind,
        required: true,
        positional: true,
        help,
    }
}

/// A positional argument that may be left out.
const fn pos_opt(name: &'static str, kind: ArgKind, help: &'static str) -> Arg {
    Arg {
        name,
        kind,
        required: false,
        positional: true,
        help,
    }
}

const fn opt(name: &'static str, kind: ArgKind, help: &'static str) -> Arg {
    Arg {
        name,
        kind,
        required: false,
        positional: false,
        help,
    }
}

/// The two records a duplicate decision is about.
const JUDGED_PAIR: &[Arg] = &[
    pos("a", ArgKind::Reference, "one record: an event citation"),
    pos(
        "b",
        ArgKind::Reference,
        "the other record: an event citation",
    ),
];

/// What an archive, restore or tombstone acts on. Exactly one scope; an
/// omitted scope is an error, never everything.
const LIFECYCLE_SCOPE: &[Arg] = &[
    pos_opt(
        "target",
        ArgKind::Rest,
        "an event reference, or a query whose event rows are the scope",
    ),
    opt(
        "material",
        ArgKind::List,
        "a whole saved source file (repeatable)",
    ),
    opt("run", ArgKind::Text, "every parsed event one run produced"),
    opt("source", ArgKind::Text, "one source's parsed events"),
    opt(
        "since",
        ArgKind::Text,
        "with --source: events at or after this time (RFC 3339)",
    ),
    opt(
        "until",
        ArgKind::Text,
        "with --source: events before this time (RFC 3339)",
    ),
    opt(
        "dry-run",
        ArgKind::Bool,
        "show what would move and what would be refused, changing nothing",
    ),
];
const LIFECYCLE_SCOPE_OR_OPERATION: &[Arg] = &[
    pos_opt(
        "target",
        ArgKind::Rest,
        "an event reference, or a query whose event rows are the scope",
    ),
    opt(
        "material",
        ArgKind::List,
        "a whole saved source file (repeatable)",
    ),
    opt("run", ArgKind::Text, "every parsed event one run produced"),
    opt("source", ArgKind::Text, "one source's parsed events"),
    opt(
        "since",
        ArgKind::Text,
        "with --source: events at or after this time (RFC 3339)",
    ),
    opt(
        "until",
        ArgKind::Text,
        "with --source: events before this time (RFC 3339)",
    ),
    opt(
        "operation",
        ArgKind::Text,
        "the events an earlier operation archived together",
    ),
    opt(
        "dry-run",
        ArgKind::Bool,
        "show what would move and what would be refused, changing nothing",
    ),
];

const PAGE: Arg = opt("page-size", ArgKind::Integer, "rows per page (default 50)");
const CURSOR: Arg = opt("cursor", ArgKind::Text, "the cursor from the previous page");

/// Advertised commands. The CLI, API discovery and MCP use this list, so
/// they cannot drift apart.
pub const COMMANDS: &[Command] = &[
    Command {
        examples: &["sinexctl status"],
        name: "status",
        summary: "How Sinex is running: saved write position, source states and consumer progress.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &[
            "sinexctl query 'events since 1h'",
            "sinexctl query --page-size 25 'commands | sort by when desc'",
        ],
        name: "query",
        summary: "Run a query in the Sinex query language; returns one page and a cursor.",
        args: &[pos("query", ArgKind::Rest, "the query text"), PAGE, CURSOR],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl recall last", "sinexctl recall yesterday"],
        name: "recall",
        summary: "What sources recorded around a time/day or moment mark, or in the latest shell session (last).",
        args: &[
            Arg {
                required: false,
                ..pos(
                    "around",
                    ArgKind::Rest,
                    "last, mark [N], a time (2026-03-02 14:05 ±20m), a day (yesterday) or nothing for now",
                )
            },
            opt(
                "cwd",
                ArgKind::Text,
                "with last: select a shell session with a member in this directory or below",
            ),
            PAGE,
            CURSOR,
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl brief --day 2026-09-14 --bytes 4000"],
        name: "brief",
        summary: "The brief for one logical day: data quality first, the day's metrics, departures from the recent level, associations over the days before, the machine.",
        args: &[
            opt(
                "day",
                ArgKind::Text,
                "YYYY-MM-DD or a stored context-pack citation; default today",
            ),
            opt(
                "bytes",
                ArgKind::Integer,
                "bytes of findings (default 4000)",
            ),
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl search rate limit"],
        name: "search",
        summary: "Full-text search over events: words (all must match), \"phrases\", word* prefixes.",
        args: &[
            pos("words", ArgKind::Rest, "the words to search for"),
            PAGE,
            CURSOR,
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl context --since 2h"],
        name: "context",
        summary: "Activity for resuming work, with source gaps; default the past two hours.",
        args: &[
            opt("since", ArgKind::Text, "duration or absolute start"),
            opt(
                "until",
                ArgKind::Text,
                "absolute end; durations count back from it",
            ),
            PAGE,
            CURSOR,
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl timeline 'where source = terminal.atuin'"],
        name: "timeline",
        summary: "Events in time order, with query filters and paging.",
        args: &[
            pos_opt(
                "query",
                ArgKind::Rest,
                "event clauses, for example where source = machine.journald since 1h",
            ),
            PAGE,
            CURSOR,
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl show sinex:ev/00000000-0000-7000-8000-000000000001"],
        name: "show",
        summary: "One parsed event, whether it is live, where it came from, and its fields.",
        args: &[pos("reference", ArgKind::Reference, "the event")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl trace sinex:ev/00000000-0000-7000-8000-000000000001"],
        name: "trace",
        summary: "The path from an event down through its parents to stored bytes.",
        args: &[
            pos("reference", ArgKind::Reference, "the event"),
            PAGE,
            CURSOR,
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl watch list"],
        name: "watch",
        summary: "Write or inspect watch rules that raise notices",
        access: Access::Agent,
        mcp: true,
        args: &[
            pos(
                "action",
                ArgKind::Text,
                "add, list, enable, disable or remove",
            ),
            opt("name", ArgKind::Text, "rule name"),
            opt(
                "rule",
                ArgKind::Text,
                "a rule as JSON; shape: docs/interfaces.md#signals",
            ),
            PAGE,
            CURSOR,
        ],
    },
    Command {
        examples: &["sinexctl kinds --kind command.executed"],
        name: "kinds",
        summary: "List event kinds",
        access: Access::Read,
        mcp: true,
        args: &[
            opt(
                "kind",
                ArgKind::Text,
                "event kind, such as command.executed",
            ),
            PAGE,
            CURSOR,
        ],
    },
    Command {
        examples: &["sinexctl fields --kind command.executed"],
        name: "fields",
        summary: "Describe the fields of an event kind",
        access: Access::Read,
        mcp: true,
        args: &[
            opt(
                "kind",
                ArgKind::Text,
                "event kind, such as command.executed",
            ),
            PAGE,
            CURSOR,
        ],
    },
    Command {
        examples: &[
            "sinexctl forecast-issue 'The sample completes' 0.7 'The sample reports completion' 2099-01-01T00:00:00Z 2026-09-14T00:00:00Z",
        ],
        name: "forecast-issue",
        summary: "Record a binary probability forecast",
        access: Access::Agent,
        mcp: true,
        args: &[
            pos("target", ArgKind::Text, "the predicted proposition"),
            pos("probability", ArgKind::Text, "probability from 0 to 1"),
            pos("criteria", ArgKind::Text, "how the outcome will be decided"),
            pos(
                "horizon",
                ArgKind::Text,
                "future resolution horizon, RFC3339",
            ),
            pos("cutoff", ArgKind::Text, "information cutoff, RFC3339"),
            opt(
                "information",
                ArgKind::List,
                "event citations available at issuance",
            ),
        ],
    },
    Command {
        examples: &[
            "sinexctl forecast-resolve sinex:ev/00000000-0000-7000-8000-000000000001 true --evidence sinex:ev/00000000-0000-7000-8000-000000000001",
        ],
        name: "forecast-resolve",
        summary: "Record an outcome and the events supporting it",
        access: Access::Agent,
        mcp: true,
        args: &[
            pos(
                "forecast",
                ArgKind::Reference,
                "issued forecast event citation",
            ),
            pos("outcome", ArgKind::Text, "true, false or unknown"),
            opt(
                "at",
                ArgKind::Text,
                "when the outcome occurred, RFC3339; unknown if omitted",
            ),
            Arg {
                required: true,
                ..opt(
                    "evidence",
                    ArgKind::List,
                    "event citations supporting the resolution",
                )
            },
        ],
    },
    Command {
        examples: &["sinexctl forecast-report"],
        name: "forecast-report",
        summary: "Brier score and calibration of visible resolved forecasts",
        access: Access::Read,
        mcp: true,
        args: &[],
    },
    Command {
        examples: &["sinexctl sources-discover"],
        name: "sources-discover",
        summary: "Which inputs this machine has, whether each is captured, and why any input is unavailable or excluded.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl mark 'Started the sample task'"],
        name: "mark",
        summary: "Mark a moment for recall, with an optional note and client-observed context.",
        args: &[
            pos_opt("text", ArgKind::Rest, "note about the moment"),
            opt("at", ArgKind::Text, "RFC3339 moment; default now"),
            opt(
                "cwd",
                ArgKind::Text,
                "directory observed by the marking client",
            ),
            opt(
                "command",
                ArgKind::Text,
                "command observed by the marking client",
            ),
            opt(
                "app",
                ArgKind::Text,
                "focused application observed by the client",
            ),
            opt(
                "title",
                ArgKind::Text,
                "focused window title observed by the client",
            ),
            opt(
                "command-record",
                ArgKind::Text,
                "the shell's own ID for the command",
            ),
            opt(
                "focus-record",
                ArgKind::Text,
                "the window manager's own ID for the focus change",
            ),
        ],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        examples: &["sinexctl save recent-commands 'commands since 1h'"],
        name: "save",
        summary: "Save a named query with scalar parameters ($name), attributed to the caller.",
        args: &[
            pos("name", ArgKind::Text, "saved query name"),
            pos("query", ArgKind::Rest, "query text with $name parameters"),
            opt(
                "parameter",
                ArgKind::List,
                "name:text, integer, number, bool, time or duration; repeatable",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl query-list"],
        name: "query-list",
        summary: "List visible saved queries, names and declared parameters.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl query-complete 'events where '"],
        name: "query-complete",
        summary: "Complete the fields a query can use here.",
        args: &[pos("query", ArgKind::Rest, "query prefix to complete")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl sql 'SELECT 1 AS number'"],
        name: "sql",
        summary: "Run one read-only SQL statement; rows as JSON objects, a page at a time.",
        args: &[pos("sql", ArgKind::Rest, "the SQL statement"), PAGE, CURSOR],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl follow 'events'"],
        name: "follow",
        summary: "Stream new events matching a query as they are committed.",
        args: &[pos("query", ArgKind::Rest, "the query text")],
        access: Access::Read,
        mcp: false,
    },
    Command {
        examples: &["sinexctl replay --dry-run"],
        name: "replay",
        summary: "Read saved source files again with current parsers and rules; earlier parsed events are archived. With no scope, read every saved file.",
        args: &[
            opt(
                "sealed",
                ArgKind::Bool,
                "use the operator's unlocked most-private values for this operation",
            ),
            opt(
                "material",
                ArgKind::List,
                "a saved source file to read again (repeatable)",
            ),
            opt(
                "binding",
                ArgKind::Text,
                "saved files from one source on one machine, such as terminal.atuin@host",
            ),
            opt(
                "kind",
                ArgKind::List,
                "event kind, such as command.executed (repeatable)",
            ),
            opt(
                "since",
                ArgKind::Text,
                "parsed events at or after this time (RFC 3339)",
            ),
            opt(
                "until",
                ArgKind::Text,
                "parsed events before this time (RFC 3339)",
            ),
            opt(
                "dry-run",
                ArgKind::Bool,
                "show what the replay would change and keep nothing",
            ),
            opt(
                "every-change",
                ArgKind::Bool,
                "list every change, not only the first few of each kind",
            ),
            opt(
                "batch-events",
                ArgKind::Integer,
                "events written per transaction instead of 10,000; smaller transactions commit more often",
            ),
        ],
        access: Access::Agent,
        mcp: false,
    },
    Command {
        examples: &["sinexctl recompute shell.sessions --compare"],
        name: "recompute",
        summary: "Recompute values from other events. With --compare, compare the current and new values and keep the comparison without writing the new values.",
        args: &[
            pos("derivation", ArgKind::Text, "the derivation"),
            opt(
                "compare",
                ArgKind::Bool,
                "show old against new without writing outputs",
            ),
            opt(
                "switch",
                ArgKind::Integer,
                "switch to the new computation using the ID of the kept comparison",
            ),
            opt(
                "since",
                ArgKind::Text,
                "only keys touched by inputs at or after this time (RFC 3339)",
            ),
            opt(
                "until",
                ArgKind::Text,
                "only keys touched by inputs before this time (RFC 3339)",
            ),
            opt(
                "examples",
                ArgKind::Integer,
                "examples of each kind of difference to show (default 5)",
            ),
        ],
        access: Access::Agent,
        mcp: false,
    },
    Command {
        examples: &["sinexctl comparisons --page-size 10"],
        name: "comparisons",
        summary: "Comparisons kept by `recompute --compare`, newest first.",
        args: &[
            pos_opt(
                "derivation",
                ArgKind::Text,
                "only comparisons of this derivation",
            ),
            PAGE,
            CURSOR,
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl crawl --allow example.org"],
        name: "crawl",
        summary: "Fetch pages from stored browser history whose host is on the allow list and that were not fetched yet (`page.fetched`). Nothing is fetched without an allow list.",
        args: &[
            opt(
                "allow",
                ArgKind::List,
                "a domain whose pages may be fetched, and its subdomains (repeatable)",
            ),
            opt(
                "url",
                ArgKind::List,
                "fetch these URLs instead of the stored history (repeatable)",
            ),
            opt("limit", ArgKind::Integer, "fetch at most this many pages"),
            opt(
                "pause",
                ArgKind::Integer,
                "seconds between two requests to the same host (default 2)",
            ),
        ],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        examples: &[
            "sinexctl same sinex:ev/00000000-0000-7000-8000-000000000001 sinex:ev/00000000-0000-7000-8000-000000000002",
        ],
        name: "same",
        summary: "Decide that two records describe the same event, replacing your earlier ruling on the pair. The operator's rulings govern; an agent's governs where the source allows agents to decide.",
        args: JUDGED_PAIR,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &[
            "sinexctl distinct sinex:ev/00000000-0000-7000-8000-000000000001 sinex:ev/00000000-0000-7000-8000-000000000002",
        ],
        name: "distinct",
        summary: "Decide that two records describe separate events, so duplicate merging keeps them apart. Replaces your earlier ruling on the pair.",
        args: JUDGED_PAIR,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &[
            "sinexctl withdraw-decision sinex:ev/00000000-0000-7000-8000-000000000001 sinex:ev/00000000-0000-7000-8000-000000000002",
        ],
        name: "withdraw-decision",
        summary: "Withdraw your ruling on two records; the source's duplicate rules decide the pair again.",
        args: JUDGED_PAIR,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl archive --source terminal.atuin --dry-run"],
        name: "archive",
        summary: "Move events and everything computed from them to the trash, reversible with restore or undo. Prints what moved; --dry-run prints what would move and changes nothing.",
        args: LIFECYCLE_SCOPE,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl restore --source terminal.atuin --dry-run"],
        name: "restore",
        summary: "Bring archived events back with everything computed from them. Refused while another parsed event for the same record is live; archive that one first.",
        args: LIFECYCLE_SCOPE_OR_OPERATION,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl tombstone --source terminal.atuin --dry-run"],
        name: "tombstone",
        summary: "Irreversibly remove archived events' fields and everything computed from them, keeping a record that they existed. A saved source file's bytes go when no event uses them. Refused for anything live. Use --dry-run first.",
        args: LIFECYCLE_SCOPE_OR_OPERATION,
        access: Access::Operator,
        mcp: false,
    },
    Command {
        examples: &["sinexctl undo 00000000-0000-7000-8000-000000000001 --dry-run"],
        name: "undo",
        summary: "Reverse an operation: archive what it made live, then restore what it archived. Undoes archive, restore, replay and undo; a tombstone is irreversible.",
        args: &[
            pos("operation", ArgKind::Text, "the operation to reverse"),
            opt(
                "dry-run",
                ArgKind::Bool,
                "show what would move and change nothing",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl backup list"],
        name: "backup",
        summary: "Pause cleanup so a backup is consistent, prepare a local backup (database, keys, event log files and capture positions), release the pause, or list open pauses. The deployment's backup tool archives the backup.",
        args: &[
            pos(
                "action",
                ArgKind::Text,
                "hold (pause cleanup), prepare, release or list",
            ),
            pos_opt("hold", ArgKind::Text, "the pause's ID, with release"),
            opt(
                "to",
                ArgKind::Text,
                "a new absolute private folder, with prepare",
            ),
            PAGE,
            CURSOR,
        ],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        examples: &["sinexctl stage /synthetic/history.zsh --follow"],
        name: "stage",
        summary: "Hand Sinex files, directories or databases to read once. Each file is read by the parser named, or by the one parser whose declared structure it has; with none or several it is stored with no parser and the candidates listed. Returns the operation, the files, and what became of each at once.",
        args: &[
            pos(
                "path",
                ArgKind::List,
                "absolute paths the daemon can read; a directory without --parser stages each file in it",
            ),
            opt(
                "parser",
                ArgKind::List,
                "the format the paths are read with, instead of detection: once for every path, or once per path in order",
            ),
            opt(
                "account",
                ArgKind::Text,
                "the account the staged export belongs to",
            ),
            opt(
                "export-date",
                ArgKind::Text,
                "when the staged export was taken",
            ),
            opt(
                "follow",
                ArgKind::Bool,
                "wait for the stage to end and return the files and counts per result",
            ),
            opt(
                "batch-events",
                ArgKind::Integer,
                "events written per transaction instead of 10,000; smaller transactions commit more often",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl stage-list"],
        name: "stage-list",
        summary: "Stage operations, newest first, with each one's files, counts per result and outcome.",
        args: &[opt(
            "active",
            ArgKind::Bool,
            "only stages that have not ended: running, or interrupted and waiting for resume",
        )],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl stage-show 00000000-0000-7000-8000-000000000001"],
        name: "stage-show",
        summary: "One stage: its files and what became of each, each product's counts per result, and how it ended.",
        args: &[pos("operation", ArgKind::Text, "the stage's operation")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl resume 00000000-0000-7000-8000-000000000001"],
        name: "resume",
        summary: "Continue an operation that stopped early (archive, restore, tombstone, undo, replay or stage) from its saved position.",
        args: &[pos("operation", ArgKind::Text, "the operation to continue")],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl privacy-rules"],
        name: "privacy-rules",
        summary: "List the privacy rules in force, or with --history every rule row ever written.",
        args: &[opt(
            "history",
            ArgKind::Bool,
            "every version and end, with author and time",
        )],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &[
            "sinexctl privacy-rule sample-rule --source terminal.atuin --kind command.executed --field /command --matcher terms --pattern sample --class open --action mask",
        ],
        name: "privacy-rule",
        summary: "Add a privacy rule, or a new version of the rule with this name. Applies to new readings; replay applies it to the past.",
        args: &[
            pos(
                "name",
                ArgKind::Text,
                "the rule's name; reusing it writes a new version",
            ),
            Arg {
                required: true,
                ..opt("source", ArgKind::Text, "the source it applies to")
            },
            opt("kind", ArgKind::Text, "only this event type"),
            opt(
                "field",
                ArgKind::Text,
                "a JSON Pointer into the payload, such as /command; default every string",
            ),
            opt(
                "matcher",
                ArgKind::Text,
                "all (default), pattern, terms, luhn or iban",
            ),
            opt(
                "pattern",
                ArgKind::Text,
                "the regular expression, or the terms separated by commas",
            ),
            opt(
                "class",
                ArgKind::Text,
                "the class a match gives: open (default), guarded or sealed",
            ),
            opt(
                "action",
                ArgKind::Text,
                "mask (default), hash, drop_field or classify",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl privacy-end sample-rule"],
        name: "privacy-end",
        summary: "End a privacy rule (weakens protection, so the operator's alone).",
        args: &[pos("name", ArgKind::Text, "the rule's name")],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        examples: &["sinexctl privacy-class terminal.atuin open"],
        name: "privacy-class",
        summary: "Set the lowest privacy level a source's events get; lowering it is the operator's alone.",
        args: &[
            pos("source", ArgKind::Text, "the source"),
            pos("class", ArgKind::Text, "open, guarded or sealed"),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl privacy-verify"],
        name: "privacy-verify",
        summary: "Count live events below the lowest privacy level their source requires. Replay applies the current privacy rules to earlier events.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl unlock /synthetic/identity.age"],
        name: "unlock",
        summary: "Unlock the most private values for a while, making them readable with the operator token.",
        args: &[
            pos(
                "identity",
                ArgKind::Text,
                "your age key file (sinexctl reads it and sends its key)",
            ),
            opt("minutes", ArgKind::Integer, "how long (default 480)"),
        ],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        examples: &["sinexctl lock"],
        name: "lock",
        summary: "Lock the most private values now.",
        args: &[],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        examples: &["sinexctl analysis-catalog"],
        name: "analysis-catalog",
        summary: "The analysis catalog: metrics with the events they count and the sources that observe them, registered methods, recipes.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl analysis-run deviations 2026-09-01 2026-09-14"],
        name: "analysis-run",
        summary: "Read the stored answer of an analysis question (correlations, trends, deviations), with how well its inputs were observed, the supporting events and the selected write position.",
        args: &[
            pos(
                "recipe",
                ArgKind::Text,
                "correlations, trends or deviations",
            ),
            pos("first", ArgKind::Text, "first logical day, YYYY-MM-DD"),
            pos("last", ArgKind::Text, "last logical day, included"),
            opt(
                "partial",
                ArgKind::Text,
                "also use partly observed days covering at least this fraction (0 to 1)",
            ),
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl analysis-panel 2026-09-01 2026-09-14 --metric shell.commands"],
        name: "analysis-panel",
        summary: "Metric cells per logical day from the daily metric figures, each with its observation state, and the coverage behind them.",
        args: &[
            pos("first", ArgKind::Text, "first logical day, YYYY-MM-DD"),
            pos("last", ArgKind::Text, "last logical day, included"),
            opt(
                "metric",
                ArgKind::List,
                "only this metric; repeat for more (default: all)",
            ),
            opt(
                "by",
                ArgKind::Text,
                "break down by this dimension (host, app, repository, unit)",
            ),
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl gaps 2026-09-01 2026-09-14"],
        name: "gaps",
        summary: "Where each source could not have recorded between two logical days: the stretches its coverage (its runs, reads and records) does not reach, or the whole span when its coverage is unknown.",
        args: &[
            pos("first", ArgKind::Text, "first logical day, YYYY-MM-DD"),
            pos("last", ArgKind::Text, "last logical day, included"),
            opt(
                "source",
                ArgKind::List,
                "only this source; repeat for more (default: every source with events)",
            ),
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl analysis-why shell.commands 2026-09-14"],
        name: "analysis-why",
        summary: "Why a metric's cell for one logical day is missing, partial or observed: its state and the coverage each of its sources had.",
        args: &[
            pos("metric", ArgKind::Text, "the metric"),
            pos("day", ArgKind::Text, "the logical day, YYYY-MM-DD"),
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl library-add /synthetic/example.png"],
        name: "library-add",
        summary: "Add a folder or file to the media library: store its bytes, describe it, and make or continue items.",
        args: &[
            pos(
                "path",
                ArgKind::Text,
                "the file or folder (read recursively)",
            ),
            opt(
                "reference",
                ArgKind::Bool,
                "index by reference only: describe and hash, but do not store the bytes",
            ),
        ],
        access: Access::Agent,
        mcp: false,
    },
    Command {
        examples: &["sinexctl library-item library:example"],
        name: "library-item",
        summary: "One library item: its revisions, renditions, copies on disk, shown tags and properties, ratings, views and collections.",
        args: &[pos("item", ArgKind::Text, "the item ID")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &[
            "sinexctl library-curate --writes '{\"act\":\"define_scale\",\"name\":\"stars\",\"question\":\"How much do you like it?\",\"range\":[0,5]}'",
        ],
        name: "library-curate",
        summary: "Curate the library in one atomic request: every write is committed or none is. \
Acts: tag, untag, reject, set, note, rate, collect, uncollect, move, judge, define_scale, define_property, define_tag, relate, dismiss, withdraw, undo. \
Each write is an object with `act` and its fields, e.g. {\"act\":\"tag\",\"target\":{\"item\":\"library:…\"},\"tag\":\"place:harbour\"}. \
Answers the records written with their revisions, or a refusal naming the current revision.",
        args: &[
            Arg {
                required: true,
                ..opt(
                    "writes",
                    ArgKind::Objects,
                    "the writes, in order; a later write sees the earlier ones",
                )
            },
            opt(
                "request-id",
                ArgKind::Text,
                "your ID for this request, reused on retry so it is written once",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        examples: &["sinexctl library-collection"],
        name: "library-collection",
        summary: "A collection's entries in order, or every collection when no name is given.",
        args: &[Arg {
            required: false,
            ..pos("collection", ArgKind::Text, "the collection's name")
        }],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl library-rank stars --next-size 3"],
        name: "library-rank",
        summary: "Items ranked on a scale from comparisons (Plackett–Luce), with intervals, rank ranges, support, groups, fit status, and the next set worth judging.",
        args: &[
            pos("scale", ArgKind::Text, "the scale's name"),
            opt(
                "judges",
                ArgKind::Text,
                "whose comparisons count: `all`, or authors separated by commas (default: the operator)",
            ),
            opt(
                "collection",
                ArgKind::Text,
                "rank this collection's items, compared or not",
            ),
            opt(
                "next-size",
                ArgKind::Integer,
                "number of items in the next set to judge (default 2)",
            ),
            opt(
                "rule",
                ArgKind::Text,
                "how the next set is chosen: information (default), random or uncompared-first",
            ),
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
        examples: &["sinexctl library-open library:example"],
        name: "library-open",
        summary: "Where to open an item's bytes: a verified copy on disk, or the stored bytes written out.",
        args: &[
            pos("item", ArgKind::Text, "the item ID"),
            opt(
                "rendition",
                ArgKind::Text,
                "a rendition ID instead of the original",
            ),
        ],
        access: Access::Read,
        mcp: false,
    },
    Command {
        examples: &["sinexctl library-viewed library:example --seconds 12.5"],
        name: "library-viewed",
        summary: "Record that an item was viewed or played, and for how long.",
        args: &[
            pos("item", ArgKind::Text, "the item ID"),
            opt("seconds", ArgKind::Text, "how long it was open"),
            opt("player", ArgKind::Text, "the program it was opened with"),
        ],
        access: Access::Agent,
        mcp: false,
    },
    Command {
        examples: &["sinexctl library-preview library:example"],
        name: "library-preview",
        summary: "Make a preview rendition of an item (a still of a video, a small image).",
        args: &[pos("item", ArgKind::Text, "the item ID")],
        access: Access::Agent,
        mcp: false,
    },
    Command {
        examples: &["sinexctl library-import /synthetic/library-sidecar.json"],
        name: "library-import",
        summary: "Import tags and metadata from sidecar files beside a folder's media (gallery-dl JSON, Hydrus tag text).",
        args: &[pos("path", ArgKind::Text, "the file or folder")],
        access: Access::Agent,
        mcp: false,
    },
];

pub fn command(name: &str) -> Option<&'static Command> {
    COMMANDS.iter().find(|c| c.name == name)
}

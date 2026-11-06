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
    pub summary: &'static str,
    pub args: &'static [Arg],
    pub access: Access,
    /// Offered as an MCP tool. `follow` streams and is not.
    pub mcp: bool,
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
    opt("material", ArgKind::List, "a whole material (repeatable)"),
    opt("run", ArgKind::Text, "every reading one run produced"),
    opt("source", ArgKind::Text, "one source's readings"),
    opt(
        "after",
        ArgKind::Text,
        "with --source: readings at or after this time (RFC 3339)",
    ),
    opt(
        "before",
        ArgKind::Text,
        "with --source: readings before this time (RFC 3339)",
    ),
    opt(
        "compare",
        ArgKind::Bool,
        "show the cascade and what would refuse, and change nothing",
    ),
];
const LIFECYCLE_SCOPE_OR_OPERATION: &[Arg] = &[
    pos_opt(
        "target",
        ArgKind::Rest,
        "an event reference, or a query whose event rows are the scope",
    ),
    opt("material", ArgKind::List, "a whole material (repeatable)"),
    opt("run", ArgKind::Text, "every reading one run produced"),
    opt("source", ArgKind::Text, "one source's readings"),
    opt(
        "after",
        ArgKind::Text,
        "with --source: readings at or after this time (RFC 3339)",
    ),
    opt(
        "before",
        ArgKind::Text,
        "with --source: readings before this time (RFC 3339)",
    ),
    opt(
        "operation",
        ArgKind::Text,
        "the chains an earlier operation archived",
    ),
    opt(
        "compare",
        ArgKind::Bool,
        "show the cascade and what would refuse, and change nothing",
    ),
];

const PAGE: Arg = opt("page", ArgKind::Integer, "rows per page (default 50)");
const CURSOR: Arg = opt("cursor", ArgKind::Text, "the cursor from the previous page");

/// Advertised commands. The CLI, API discovery and MCP use this list, so
/// they cannot drift apart.
pub const COMMANDS: &[Command] = &[
    Command {
        name: "signal",
        summary: "Author or inspect standing signal rules",
        access: Access::Agent,
        mcp: true,
        args: &[
            pos(
                "action",
                ArgKind::Text,
                "add, list, enable, disable or remove",
            ),
            opt("name", ArgKind::Text, "rule name"),
            opt("rule", ArgKind::Text, "complete shared signal rule as JSON"),
            PAGE,
            CURSOR,
        ],
    },
    Command {
        name: "kinds",
        summary: "List immutable kind declarations",
        access: Access::Read,
        mcp: true,
        args: &[opt("kind", ArgKind::Text, "event type"), PAGE, CURSOR],
    },
    Command {
        name: "fields",
        summary: "Describe payload fields from stored declarations",
        access: Access::Read,
        mcp: true,
        args: &[opt("kind", ArgKind::Text, "event type"), PAGE, CURSOR],
    },
    Command {
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
        name: "forecast-resolve",
        summary: "Record an outcome and its evidence",
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
        name: "forecast-report",
        summary: "Brier score and calibration of visible resolved forecasts",
        access: Access::Read,
        mcp: true,
        args: &[],
    },
    Command {
        name: "status",
        summary: "What Sinex holds: commit position, sources and their newest events, the search index.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "sources-discover",
        summary: "Discovery dry run: every input with its state (capturing, unavailable and why, excluded and why, on demand), and the catalog sources without an implementation whose data this host has.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
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
                "command_record",
                ArgKind::Text,
                "native command record key observed by the client",
            ),
            opt(
                "focus_record",
                ArgKind::Text,
                "native focus record key observed by the client",
            ),
        ],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        name: "query-save",
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
        name: "query-list",
        summary: "List visible saved queries, names and declared parameters.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "query-complete",
        summary: "Complete query fields from the selected table and event schemas.",
        args: &[pos("query", ArgKind::Rest, "query prefix to complete")],
        access: Access::Read,
        mcp: true,
    },
    Command {
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
        name: "timeline",
        summary: "Events in time order, with ordinary query filters and paging.",
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
        name: "query",
        summary: "Run a query in the Sinex query language; returns one page and a cursor.",
        args: &[pos("query", ArgKind::Rest, "the query text"), PAGE, CURSOR],
        access: Access::Read,
        mcp: true,
    },
    Command {
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
        name: "sql",
        summary: "Run one read-only SQL statement as the reader role; rows as JSON objects.",
        args: &[pos("sql", ArgKind::Rest, "the SQL statement")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "show",
        summary: "One event in full: reading, state, provenance, payload.",
        args: &[pos("reference", ArgKind::Reference, "the event")],
        access: Access::Read,
        mcp: true,
    },
    Command {
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
        name: "follow",
        summary: "Stream new events matching a query as they are committed.",
        args: &[pos("query", ArgKind::Rest, "the query text")],
        access: Access::Read,
        mcp: false,
    },
    Command {
        name: "replay",
        summary: "Read stored materials again with current parsers and rules; old readings are archived. With no scope, every material.",
        args: &[
            opt(
                "sealed",
                ArgKind::Bool,
                "explicitly use the operator's current sealed unlock for this operation",
            ),
            opt(
                "material",
                ArgKind::List,
                "a material to re-read (repeatable)",
            ),
            opt(
                "binding",
                ArgKind::Text,
                "every material of a binding (`atuin@host`)",
            ),
            opt(
                "event-type",
                ArgKind::List,
                "only readings of this event type (repeatable)",
            ),
            opt(
                "from",
                ArgKind::Text,
                "only readings at or after this time (RFC 3339)",
            ),
            opt(
                "until",
                ArgKind::Text,
                "only readings before this time (RFC 3339)",
            ),
            opt(
                "compare",
                ArgKind::Bool,
                "show what the replay would change and keep nothing",
            ),
            opt(
                "all",
                ArgKind::Bool,
                "list every change, not only the first few of each kind",
            ),
        ],
        access: Access::Agent,
        mcp: false,
    },
    Command {
        name: "recompute",
        summary: "Recompute a keyed derivation's outputs. With --compare, show old against new and keep the difference in derivation.comparisons, without writing outputs.",
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
                "select the producer version using a full comparison ID",
            ),
            opt(
                "from",
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
        name: "comparisons",
        summary: "Comparisons kept by `recompute --compare`, newest first.",
        args: &[
            pos_opt(
                "derivation",
                ArgKind::Text,
                "only comparisons of this derivation",
            ),
            opt("limit", ArgKind::Integer, "how many (default 20)"),
        ],
        access: Access::Read,
        mcp: true,
    },
    Command {
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
        name: "same",
        summary: "Decide two records are one occurrence: a duplicate decision that revises your earlier one on the pair. The operator's decisions govern; an agent's governs only where the family admits agent decisions.",
        args: JUDGED_PAIR,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "distinct",
        summary: "Decide two records are two occurrences, so fusion never joins them: a duplicate decision that revises your earlier one on the pair.",
        args: JUDGED_PAIR,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "withdraw-decision",
        summary: "Withdraw your duplicate decision on two records; the family's rules decide the pair again.",
        args: JUDGED_PAIR,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "archive",
        summary: "Archive readings with their whole live chains: the trash, reversible with restore or undo. Prints the cascade it moved; --compare prints it and moves nothing.",
        args: LIFECYCLE_SCOPE,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "restore",
        summary: "Bring archived readings back with the chains that left with them. Refused while another reading of the same record is live; archive that one first.",
        args: LIFECYCLE_SCOPE_OR_OPERATION,
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "tombstone",
        summary: "Irreversibly drop the payloads of archived readings and their chains, keeping skeletons; a tombstoned material's bytes go when nothing else holds them. Refused for anything live. Use --compare first.",
        args: LIFECYCLE_SCOPE_OR_OPERATION,
        access: Access::Operator,
        mcp: false,
    },
    Command {
        name: "undo",
        summary: "Reverse an operation: archive what it made live, then restore what it archived. Undoes archive, restore, replay and undo; a tombstone is irreversible.",
        args: &[
            pos("operation", ArgKind::Text, "the operation to reverse"),
            opt(
                "compare",
                ArgKind::Bool,
                "show what would move and change nothing",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "backup",
        summary: "Hold a core backup boundary, prepare a local immutable paired dump and segment artifact, release a hold, or list open holds. Producer custody is explicitly incomplete; archive custody belongs to the deployment.",
        args: &[
            pos("action", ArgKind::Text, "hold, prepare, release or list"),
            pos_opt("hold", ArgKind::Text, "the hold UUID, with release"),
            opt(
                "output",
                ArgKind::Text,
                "a new absolute private artifact directory, with prepare",
            ),
            opt("page", ArgKind::Integer, "open holds per page, with list"),
            opt(
                "cursor",
                ArgKind::Text,
                "continue the previous open-hold page",
            ),
        ],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        name: "stage",
        summary: "Hand Sinex files, directories or databases to read once. Each file is read by the parser named, or by the one parser whose declared structure it has; with none or several it is stored with no parser and the candidates listed. Returns the operation and the inventory of every member at once.",
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
                "wait for the stage to end and return its final inventory and accounting",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "stage-list",
        summary: "Stage operations, newest first, with each one's files, accounting and result.",
        args: &[opt(
            "active",
            ArgKind::Bool,
            "only stages that have not ended: running, or interrupted and waiting for resume",
        )],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "stage-show",
        summary: "One stage: its inventory with every member's disposition, each product's accounting, and how it ended.",
        args: &[pos("operation", ArgKind::Text, "the stage's operation")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "resume",
        summary: "Continue an operation that stopped early (archive, restore, tombstone, undo, replay or stage) from its saved position.",
        args: &[pos("operation", ArgKind::Text, "the operation to continue")],
        access: Access::Agent,
        mcp: true,
    },
    Command {
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
            opt("type", ArgKind::Text, "only this event type"),
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
        name: "privacy-end",
        summary: "End a privacy rule (weakens protection, so the operator's alone).",
        args: &[pos("name", ArgKind::Text, "the rule's name")],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        name: "privacy-class",
        summary: "Set a source's class floor; lowering it is the operator's alone.",
        args: &[
            pos("source", ArgKind::Text, "the source"),
            pos("class", ArgKind::Text, "open, guarded or sealed"),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "privacy-verify",
        summary: "Count live events below their source's class floor (admitted before the floor was raised; a replay fixes them).",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "unlock",
        summary: "Unlock the sealed class for a while: the operator's sealed values become readable to the operator token.",
        args: &[
            pos(
                "identity",
                ArgKind::Text,
                "the age identity file (sinexctl reads it and sends its key)",
            ),
            opt("minutes", ArgKind::Integer, "how long (default 480)"),
        ],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        name: "lock",
        summary: "Lock the sealed class now.",
        args: &[],
        access: Access::Operator,
        mcp: false,
    },
    Command {
        name: "analysis-catalog",
        summary: "The analysis catalog: metrics with the events they count and the sources that observe them, registered methods, recipes.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "analysis-run",
        summary: "Read the maintained answer of a canonical analysis question (correlations, trends, deviations), with its stored coverage, evidence and selected commit position.",
        args: &[
            pos(
                "recipe",
                ArgKind::Text,
                "correlations, trends or deviations",
            ),
            pos("from", ArgKind::Text, "first logical day, YYYY-MM-DD"),
            pos("to", ArgKind::Text, "last logical day, included"),
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
        name: "analysis-panel",
        summary: "Metric cells per logical day from the daily metric figures, each with its observation state, and the coverage behind them.",
        args: &[
            pos("from", ArgKind::Text, "first logical day, YYYY-MM-DD"),
            pos("to", ArgKind::Text, "last logical day, included"),
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
        name: "gaps",
        summary: "Where each source could not have recorded between two logical days: the stretches its coverage (its runs, reads and records) does not reach, or the whole span when its coverage is unknown.",
        args: &[
            pos("from", ArgKind::Text, "first logical day, YYYY-MM-DD"),
            pos("to", ArgKind::Text, "last logical day, included"),
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
        name: "brief",
        summary: "The brief for one logical day: data quality first, the day's metrics, departures from the recent level, associations over the days before, the machine.",
        args: &[
            opt(
                "day",
                ArgKind::Text,
                "YYYY-MM-DD or a stored context-pack citation; default today",
            ),
            opt(
                "budget",
                ArgKind::Integer,
                "bytes of findings (default 4000)",
            ),
        ],
        access: Access::Read,
        mcp: true,
    },
    // The media library.
    Command {
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
        name: "library-item",
        summary: "One library item: its revisions, renditions, copies on disk, shown tags and properties, ratings, views and collections.",
        args: &[pos("item", ArgKind::Text, "the item ID")],
        access: Access::Read,
        mcp: true,
    },
    Command {
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
                "request",
                ArgKind::Text,
                "your ID for this request, reused on retry so it is written once",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
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
                "k",
                ArgKind::Integer,
                "how many items the next set holds (default 2)",
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
        name: "library-preview",
        summary: "Make a preview rendition of an item (a still of a video, a small image).",
        args: &[pos("item", ArgKind::Text, "the item ID")],
        access: Access::Agent,
        mcp: false,
    },
    Command {
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

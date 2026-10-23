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
    /// Permanent changes: excision.
    Operator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgKind {
    Text,
    /// A citation: an event ID, `sinex:ev/…`, `sinex:occ/…` or `sinex:eq/…`.
    Reference,
    Integer,
    Bool,
    /// The rest of the command line, joined by spaces: a query, words, a time.
    Rest,
    /// A repeatable option; its values as a list.
    List,
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

const PAGE: Arg = opt("page", ArgKind::Integer, "rows per page (default 50)");
const CURSOR: Arg = opt("cursor", ArgKind::Text, "the cursor from the previous page");

/// Advertised commands. The CLI, API discovery and MCP use this list, so
/// they cannot drift apart.
pub const COMMANDS: &[Command] = &[
    Command {
        name: "status",
        summary: "What Sinex holds: commit position, sources and their newest events, the search index.",
        args: &[],
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
        summary: "What sources recorded around a time/day, or in the latest shell group (last).",
        args: &[
            Arg {
                required: false,
                ..pos(
                    "around",
                    ArgKind::Rest,
                    "last, a time (2026-03-02 14:05 ±20m), a day (yesterday) or nothing for now",
                )
            },
            opt(
                "cwd",
                ArgKind::Text,
                "with last: select a shell group with a member in this directory or below",
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
        summary: "The chain from an event down through its parents to stored bytes.",
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
                "resume",
                ArgKind::Text,
                "continue an interrupted replay operation, skipping the chains it finished",
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
        summary: "Recompute a keyed derivation's outputs. With --compare, compute into the comparison lane and show old against new without writing.",
        args: &[
            pos("derivation", ArgKind::Text, "the derivation"),
            opt(
                "compare",
                ArgKind::Bool,
                "compute into the comparison lane and show old against new",
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
        name: "hide",
        summary: "Take an event out of views, counts and re-imports; reversible with unhide.",
        args: &[pos("reference", ArgKind::Reference, "the event")],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "unhide",
        summary: "Put a hidden event back.",
        args: &[pos("reference", ArgKind::Reference, "the event")],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "excise",
        summary: "Permanently delete an event's every reading, or a whole material and its bytes; it never comes back by any route.",
        args: &[
            pos_opt("reference", ArgKind::Reference, "the event"),
            opt(
                "material",
                ArgKind::Text,
                "a whole material instead of one event",
            ),
            opt(
                "dry-run",
                ArgKind::Bool,
                "report exactly what would be removed, and remove nothing",
            ),
        ],
        access: Access::Operator,
        mcp: false,
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
        summary: "The analysis catalog: metrics with the sources that observe them, registered methods, recipes.",
        args: &[],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "analysis-run",
        summary: "Run an analysis recipe (correlations, trends, deviations) over logical days and store its result; asking again at the same commit position returns the stored result.",
        args: &[
            pos(
                "recipe",
                ArgKind::Text,
                "correlations, trends or deviations",
            ),
            pos("from", ArgKind::Text, "first logical day, YYYY-MM-DD"),
            pos("to", ArgKind::Text, "last logical day, included"),
            opt(
                "standing",
                ArgKind::Bool,
                "record it as the standing question's newest answer",
            ),
            opt(
                "partial",
                ArgKind::Text,
                "also use partly observed days covering at least this fraction (0 to 1)",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "analysis-panel",
        summary: "Metric cells per logical day, each with its observation state, and the coverage behind them.",
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
        name: "analysis-results",
        summary: "The newest stored analysis results.",
        args: &[opt("limit", ArgKind::Integer, "how many (default 20)")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "analysis-show",
        summary: "A stored analysis result: its status, the inputs it read, its tables and findings.",
        args: &[pos("result", ArgKind::Text, "the result ID")],
        access: Access::Read,
        mcp: true,
    },
    Command {
        name: "brief",
        summary: "The brief for one logical day: data quality first, the day's metrics, departures from the recent level, associations, the machine.",
        args: &[
            opt("day", ArgKind::Text, "YYYY-MM-DD; default today"),
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
        name: "library-tag",
        summary: "Tag an item (namespace:tag). `-tag` removes an imported tag; the operator's value wins over imports and proposals.",
        args: &[
            pos("item", ArgKind::Text, "the item ID"),
            pos("tag", ArgKind::Text, "the tag, e.g. place:croatia"),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "library-untag",
        summary: "Withdraw your own assignments of a tag to an item.",
        args: &[
            pos("item", ArgKind::Text, "the item ID"),
            pos("tag", ArgKind::Text, "the tag"),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "library-set",
        summary: "Set a single-choice property of an item, or set it to empty; replaces your earlier value.",
        args: &[
            pos("item", ArgKind::Text, "the item ID"),
            pos("field", ArgKind::Text, "the property, e.g. medium"),
            opt("value", ArgKind::Text, "the value; omit with --empty"),
            opt(
                "empty",
                ArgKind::Bool,
                "set to empty, which stops imported and proposed values showing",
            ),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "library-collect",
        summary: "Add an item to the end of a collection, creating the collection if needed.",
        args: &[
            pos("collection", ArgKind::Text, "the collection's name"),
            pos("item", ArgKind::Text, "the item ID"),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "library-uncollect",
        summary: "Remove an item's entries from a collection (withdrawn, still inspectable).",
        args: &[
            pos("collection", ArgKind::Text, "the collection's name"),
            pos("item", ArgKind::Text, "the item ID"),
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
        name: "library-rate",
        summary: "Rate an item on a scale (default: stars out of 5).",
        args: &[
            pos("item", ArgKind::Text, "the item ID"),
            pos("value", ArgKind::Text, "the rating, a number"),
            opt("scale", ArgKind::Text, "the scale's name (default stars)"),
            opt("of", ArgKind::Text, "the top of the scale (default 5)"),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "library-compare",
        summary: "Record that one item is better than another on a scale; rankings are fitted from every comparison.",
        args: &[
            pos("winner", ArgKind::Text, "the better item"),
            pos("loser", ArgKind::Text, "the worse item"),
            opt("scale", ArgKind::Text, "the scale's name (default overall)"),
        ],
        access: Access::Agent,
        mcp: true,
    },
    Command {
        name: "library-rank",
        summary: "Items ranked by pairwise comparisons on a scale, with the pair worth comparing next.",
        args: &[opt(
            "scale",
            ArgKind::Text,
            "the scale's name (default overall)",
        )],
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

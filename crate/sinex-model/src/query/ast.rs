//! The bound query: parsed, type-checked against the unit's fields, with every
//! relative time resolved once against the [`Context`](super::Context).
use super::units::{Alias, Field, Unit};
use jiff::Timestamp;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub source: Source,
    pub filter: Option<Expr>,
    pub stages: Vec<Stage>,
    /// Whether the query contains words, so rows are ranked by relevance
    /// unless a `sort` says otherwise.
    pub ranked: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    Unit {
        unit: Unit,
        alias: Option<&'static Alias>,
    },
    /// A provenance walk from one reference (spec `query.md` §7).
    Walk { walk: Walk, reference: Reference },
    /// Everything recorded in a window, across every source (§6.3).
    Recall { from: Timestamp, to: Timestamp },
}

impl Source {
    /// The unit whose fields the rows carry. Walks and recall return events.
    pub fn unit(&self) -> Unit {
        match self {
            Source::Unit { unit, .. } => *unit,
            Source::Walk { .. } | Source::Recall { .. } => Unit::Events,
        }
    }

    pub fn alias(&self) -> Option<&'static Alias> {
        match self {
            Source::Unit { alias, .. } => *alias,
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Walk {
    /// From the event down through its parents to material bytes.
    Trace,
    /// Everything derived from it, recursively.
    Descendants,
    /// One level up.
    Parents,
}

/// A citation (spec `query.md` §6.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reference {
    /// `sinex:ev/<id>` or a bare event ID: one interpretation, live or archived.
    Event(Uuid),
    /// `sinex:occ/<material>/<anchor>[/<event type>[/<ordinal>]]`.
    Occurrence {
        material: Uuid,
        anchor: i64,
        event_type: Option<String>,
        ordinal: Option<i16>,
    },
    /// `sinex:eq/<source>/<event type>/<equivalence key>`.
    Equivalence {
        source: String,
        event_type: String,
        key: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Compare {
        lhs: Operand,
        op: Op,
        rhs: Value,
    },
    Exists(Operand),
    /// Full-text words or a phrase.
    Words(Words),
    /// A half-open time range on a time operand: `[from, to)`.
    Within {
        lhs: Operand,
        from: Timestamp,
        to: Timestamp,
    },
    /// The difference of two time fields: `seen - when > 1d`.
    TimeDiff {
        a: FieldRef,
        b: FieldRef,
        op: Op,
        seconds: f64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Words {
    /// Normalized words: case-folded, NFC.
    pub words: Vec<String>,
    /// A quoted phrase: the words in order, adjacent.
    pub phrase: bool,
    /// A trailing `*`: the last word is a prefix.
    pub prefix: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Operand {
    Field(FieldRef),
    TimeFn(TimeFn, FieldRef),
}

impl Operand {
    pub fn name(&self) -> String {
        match self {
            Operand::Field(f) => f.name(),
            Operand::TimeFn(func, f) => format!("{}({})", func.name(), f.name()),
        }
    }

    /// The column name a grouped stage gives this key.
    pub fn column(&self) -> String {
        match self {
            Operand::Field(f) => f.name().replace('.', "_"),
            Operand::TimeFn(func, _) => func.name().to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldRef {
    Field(&'static Field),
    /// `payload.a.b`: a path into the payload.
    Payload(Vec<String>),
    /// A column of a grouped stage: a key or an aggregate.
    Group(String),
}

impl FieldRef {
    pub fn name(&self) -> String {
        match self {
            FieldRef::Field(f) => f.name.to_string(),
            FieldRef::Payload(path) => format!("payload.{}", path.join(".")),
            FieldRef::Group(name) => name.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeFn {
    Day,
    Hour,
    Week,
    Weekday,
    Month,
    MonthDay,
}

impl TimeFn {
    pub const ALL: [TimeFn; 6] = [
        TimeFn::Day,
        TimeFn::Hour,
        TimeFn::Week,
        TimeFn::Weekday,
        TimeFn::Month,
        TimeFn::MonthDay,
    ];

    pub fn name(self) -> &'static str {
        match self {
            TimeFn::Day => "day",
            TimeFn::Hour => "hour",
            TimeFn::Week => "week",
            TimeFn::Weekday => "weekday",
            TimeFn::Month => "month",
            TimeFn::MonthDay => "month_day",
        }
    }

    pub fn from_name(name: &str) -> Option<TimeFn> {
        TimeFn::ALL.into_iter().find(|f| f.name() == name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// Contains, case-insensitive.
    Contains,
    /// Starts with; on a path, "is or lies under".
    Prefix,
}

impl Op {
    pub fn symbol(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Lt => "<",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::Contains => "~",
            Op::Prefix => "^",
        }
    }

    pub fn ordering(self) -> bool {
        matches!(self, Op::Lt | Op::Le | Op::Gt | Op::Ge)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Text(String),
    /// A text value with `*` wildcards.
    Glob(String),
    Integer(i64),
    Number(f64),
    Bool(bool),
    Time(Timestamp),
    Id(Uuid),
    /// Any of: `field = [a, b]`.
    List(Vec<Value>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Stage {
    /// A filter on the rows of the stage before it (after a grouping, on the
    /// groups).
    Where(Expr),
    Join(Join),
    AsOf(AsOf),
    Group(Group),
    Sort(Vec<SortKey>),
    Limit(u64),
    Fields(Vec<FieldRef>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub kind: JoinKind,
    pub inner: Box<Query>,
    /// Which time of an inner row the join measures from.
    pub at: TimePoint,
    /// The window, for `near`, `before` and `after`.
    pub within_secs: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Near,
    Before,
    After,
    During,
    Containing,
}

impl JoinKind {
    pub fn name(self) -> &'static str {
        match self {
            JoinKind::Near => "near",
            JoinKind::Before => "before",
            JoinKind::After => "after",
            JoinKind::During => "during",
            JoinKind::Containing => "containing",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimePoint {
    When,
    Start,
    End,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AsOf {
    Time(Timestamp),
    Seq(i64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Group {
    pub keys: Vec<Operand>,
    pub aggregates: Vec<Aggregate>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Aggregate {
    pub function: AggFn,
    pub field: Option<FieldRef>,
}

impl Aggregate {
    /// The column the aggregate becomes: `count`, `sum_duration`.
    pub fn column(&self) -> String {
        match &self.field {
            None => self.function.name().to_string(),
            Some(f) => format!("{}_{}", self.function.name(), f.name().replace('.', "_")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    CountDistinct,
}

impl AggFn {
    pub const ALL: [AggFn; 6] = [
        AggFn::Count,
        AggFn::Sum,
        AggFn::Min,
        AggFn::Max,
        AggFn::Avg,
        AggFn::CountDistinct,
    ];

    pub fn name(self) -> &'static str {
        match self {
            AggFn::Count => "count",
            AggFn::Sum => "sum",
            AggFn::Min => "min",
            AggFn::Max => "max",
            AggFn::Avg => "avg",
            AggFn::CountDistinct => "count_distinct",
        }
    }

    pub fn from_name(name: &str) -> Option<AggFn> {
        AggFn::ALL.into_iter().find(|f| f.name() == name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortKey {
    pub key: SortBy,
    pub descending: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortBy {
    Operand(Operand),
    /// Full-text relevance.
    Rank,
}

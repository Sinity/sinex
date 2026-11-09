//! The bound query: parsed, type-checked against the table's fields, with every
//! relative time resolved once against the [`Context`](super::Context).
use super::tables::{Alias, Field, Table};
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
    Table {
        table: Table,
        alias: Option<&'static Alias>,
    },
    /// A provenance walk from one reference.
    Walk { walk: Walk, reference: Reference },
    /// Everything recorded in a window, across every source.
    Recall { from: Timestamp, to: Timestamp },
    /// The latest recorded shell session matching a session-field filter.
    RecallLast { filter: Option<Expr> },
    /// The Nth newest visible operator moment mark.
    RecallMark { number: u64, window: f64 },
}

impl Source {
    /// The table whose fields the rows carry. Walks and recall return events.
    pub fn table(&self) -> Table {
        match self {
            Source::Table { table, .. } => *table,
            Source::Walk { .. }
            | Source::Recall { .. }
            | Source::RecallLast { .. }
            | Source::RecallMark { .. } => Table::Events,
        }
    }

    pub fn alias(&self) -> Option<&'static Alias> {
        match self {
            Source::Table { alias, .. } => *alias,
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

/// A citation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reference {
    /// `sinex:ev/<id>` or a bare event ID: one reading, live or archived.
    Event(Uuid),
    /// The live spanning output containing this absolute instant.
    At { at: Timestamp, event_type: String },
    /// A half-open absolute time window.
    Window { from: Timestamp, to: Timestamp },
    /// `sinex:record/<material>/<offset>[/<event type>[/<output>]]`: the live
    /// reading of the record starting at that byte offset.
    Record {
        material: Uuid,
        offset: i64,
        event_type: Option<String>,
        output: Option<u64>,
    },
    /// `sinex:key/<source>/<event type>/<equivalence key>`.
    Key {
        source: String,
        event_type: String,
        key: String,
    },
}

/// A condition. `and`, `or` and `not` nest one level each, so a query nests
/// as deeply as its text; copying, comparing, printing and dropping go one
/// level at a time through [`query_language::depth::deeper`].
pub enum Expr {
    Order(Box<Join>),
    Truth {
        value: query_language::semantics::Truth,
        operand: Operand,
    },
    /// A supplied vector reference ranks the filtered relation; it is not a predicate.
    Similar {
        reference: Uuid,
    },
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
    /// Natural word matching restricted to one projected text field.
    FieldWords {
        operand: Operand,
        words: Words,
    },
    /// A half-open time range on a time operand: `[from, to)`.
    Within {
        lhs: Operand,
        from: Timestamp,
        to: Timestamp,
    },
    /// Spanning states that held at an instant or during a half-open window.
    Overlaps {
        from: Timestamp,
        to: Option<Timestamp>,
    },
}

impl Drop for Expr {
    fn drop(&mut self) {
        let leaf = || Expr::Similar {
            reference: Uuid::nil(),
        };
        match self {
            Expr::And(left, right) | Expr::Or(left, right) => {
                let left = std::mem::replace(&mut **left, leaf());
                let right = std::mem::replace(&mut **right, leaf());
                query_language::depth::deeper(move || drop((left, right)));
            }
            Expr::Not(inner) => {
                let inner = std::mem::replace(&mut **inner, leaf());
                query_language::depth::deeper(move || drop(inner));
            }
            _ => {}
        }
    }
}

impl Clone for Expr {
    fn clone(&self) -> Self {
        query_language::depth::deeper(|| match self {
            Expr::Order(join) => Expr::Order(join.clone()),
            Expr::Truth { value, operand } => Expr::Truth {
                value: value.clone(),
                operand: operand.clone(),
            },
            Expr::Similar { reference } => Expr::Similar {
                reference: *reference,
            },
            Expr::And(left, right) => Expr::And(left.clone(), right.clone()),
            Expr::Or(left, right) => Expr::Or(left.clone(), right.clone()),
            Expr::Not(inner) => Expr::Not(inner.clone()),
            Expr::Compare { lhs, op, rhs } => Expr::Compare {
                lhs: lhs.clone(),
                op: op.clone(),
                rhs: rhs.clone(),
            },
            Expr::Exists(operand) => Expr::Exists(operand.clone()),
            Expr::Words(words) => Expr::Words(words.clone()),
            Expr::FieldWords { operand, words } => Expr::FieldWords {
                operand: operand.clone(),
                words: words.clone(),
            },
            Expr::Within { lhs, from, to } => Expr::Within {
                lhs: lhs.clone(),
                from: *from,
                to: *to,
            },
            Expr::Overlaps { from, to } => Expr::Overlaps {
                from: *from,
                to: *to,
            },
        })
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        query_language::depth::deeper(|| match (self, other) {
            (Expr::Order(a), Expr::Order(b)) => a == b,
            (
                Expr::Truth { value, operand },
                Expr::Truth {
                    value: other_value,
                    operand: other_operand,
                },
            ) => value == other_value && operand == other_operand,
            (Expr::Similar { reference }, Expr::Similar { reference: other }) => reference == other,
            (Expr::And(a, b), Expr::And(c, d)) | (Expr::Or(a, b), Expr::Or(c, d)) => {
                a == c && b == d
            }
            (Expr::Not(a), Expr::Not(b)) => a == b,
            (
                Expr::Compare { lhs, op, rhs },
                Expr::Compare {
                    lhs: other_lhs,
                    op: other_op,
                    rhs: other_rhs,
                },
            ) => lhs == other_lhs && op == other_op && rhs == other_rhs,
            (Expr::Exists(a), Expr::Exists(b)) => a == b,
            (Expr::Words(a), Expr::Words(b)) => a == b,
            (
                Expr::FieldWords { operand, words },
                Expr::FieldWords {
                    operand: other_operand,
                    words: other_words,
                },
            ) => operand == other_operand && words == other_words,
            (
                Expr::Within { lhs, from, to },
                Expr::Within {
                    lhs: other_lhs,
                    from: other_from,
                    to: other_to,
                },
            ) => lhs == other_lhs && from == other_from && to == other_to,
            (
                Expr::Overlaps { from, to },
                Expr::Overlaps {
                    from: other_from,
                    to: other_to,
                },
            ) => from == other_from && to == other_to,
            _ => false,
        })
    }
}

impl std::fmt::Debug for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        query_language::depth::deeper(|| match self {
            Expr::Order(join) => f.debug_tuple("Order").field(join).finish(),
            Expr::Truth { value, operand } => f
                .debug_struct("Truth")
                .field("value", value)
                .field("operand", operand)
                .finish(),
            Expr::Similar { reference } => f
                .debug_struct("Similar")
                .field("reference", reference)
                .finish(),
            Expr::And(left, right) => f.debug_tuple("And").field(left).field(right).finish(),
            Expr::Or(left, right) => f.debug_tuple("Or").field(left).field(right).finish(),
            Expr::Not(inner) => f.debug_tuple("Not").field(inner).finish(),
            Expr::Compare { lhs, op, rhs } => f
                .debug_struct("Compare")
                .field("lhs", lhs)
                .field("op", op)
                .field("rhs", rhs)
                .finish(),
            Expr::Exists(operand) => f.debug_tuple("Exists").field(operand).finish(),
            Expr::Words(words) => f.debug_tuple("Words").field(words).finish(),
            Expr::FieldWords { operand, words } => f
                .debug_struct("FieldWords")
                .field("operand", operand)
                .field("words", words)
                .finish(),
            Expr::Within { lhs, from, to } => f
                .debug_struct("Within")
                .field("lhs", lhs)
                .field("from", from)
                .field("to", to)
                .finish(),
            Expr::Overlaps { from, to } => f
                .debug_struct("Overlaps")
                .field("from", from)
                .field("to", to)
                .finish(),
        })
    }
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

/// A value a condition compares. Arithmetic and the functions nest one level
/// each, so copying, comparing, printing and dropping go one level at a time
/// through [`query_language::depth::deeper`].
pub enum Operand {
    Field(FieldRef),
    TimeFn(TimeFn, FieldRef),
    Literal {
        value: Value,
        kind: query_language::catalog::FieldKind,
    },
    Arithmetic {
        operator: query_language::ast::Arithmetic,
        left: Box<Operand>,
        right: Box<Operand>,
        kind: query_language::catalog::FieldKind,
    },
    Length(Box<Operand>),
    Absolute(Box<Operand>),
    Text(Box<Operand>),
    Glob {
        left: Box<Operand>,
        right: Box<Operand>,
    },
    CaseEqual {
        left: Box<Operand>,
        right: Box<Operand>,
        value: Box<Operand>,
    },
    Named {
        operand: Box<Operand>,
        name: String,
    },
}

impl Drop for Operand {
    fn drop(&mut self) {
        let leaf = || Operand::Literal {
            value: Value::Unknown,
            kind: query_language::catalog::FieldKind::Text,
        };
        let mut children = Vec::new();
        match self {
            Operand::Arithmetic { left, right, .. } | Operand::Glob { left, right } => {
                children.push(std::mem::replace(&mut **left, leaf()));
                children.push(std::mem::replace(&mut **right, leaf()));
            }
            Operand::CaseEqual { left, right, value } => {
                children.push(std::mem::replace(&mut **left, leaf()));
                children.push(std::mem::replace(&mut **right, leaf()));
                children.push(std::mem::replace(&mut **value, leaf()));
            }
            Operand::Length(inner)
            | Operand::Absolute(inner)
            | Operand::Text(inner)
            | Operand::Named { operand: inner, .. } => {
                children.push(std::mem::replace(&mut **inner, leaf()));
            }
            _ => return,
        }
        query_language::depth::deeper(move || drop(children));
    }
}

impl Clone for Operand {
    fn clone(&self) -> Self {
        query_language::depth::deeper(|| match self {
            Operand::Field(field) => Operand::Field(field.clone()),
            Operand::TimeFn(function, field) => Operand::TimeFn(function.clone(), field.clone()),
            Operand::Literal { value, kind } => Operand::Literal {
                value: value.clone(),
                kind: kind.clone(),
            },
            Operand::Arithmetic {
                operator,
                left,
                right,
                kind,
            } => Operand::Arithmetic {
                operator: operator.clone(),
                left: left.clone(),
                right: right.clone(),
                kind: kind.clone(),
            },
            Operand::Length(inner) => Operand::Length(inner.clone()),
            Operand::Absolute(inner) => Operand::Absolute(inner.clone()),
            Operand::Text(inner) => Operand::Text(inner.clone()),
            Operand::Glob { left, right } => Operand::Glob {
                left: left.clone(),
                right: right.clone(),
            },
            Operand::CaseEqual { left, right, value } => Operand::CaseEqual {
                left: left.clone(),
                right: right.clone(),
                value: value.clone(),
            },
            Operand::Named { operand, name } => Operand::Named {
                operand: operand.clone(),
                name: name.clone(),
            },
        })
    }
}

impl PartialEq for Operand {
    fn eq(&self, other: &Self) -> bool {
        query_language::depth::deeper(|| match (self, other) {
            (Operand::Field(a), Operand::Field(b)) => a == b,
            (Operand::TimeFn(f, a), Operand::TimeFn(g, b)) => f == g && a == b,
            (
                Operand::Literal { value, kind },
                Operand::Literal {
                    value: other_value,
                    kind: other_kind,
                },
            ) => value == other_value && kind == other_kind,
            (
                Operand::Arithmetic {
                    operator,
                    left,
                    right,
                    kind,
                },
                Operand::Arithmetic {
                    operator: other_operator,
                    left: other_left,
                    right: other_right,
                    kind: other_kind,
                },
            ) => {
                operator == other_operator
                    && kind == other_kind
                    && left == other_left
                    && right == other_right
            }
            (Operand::Length(a), Operand::Length(b))
            | (Operand::Absolute(a), Operand::Absolute(b))
            | (Operand::Text(a), Operand::Text(b)) => a == b,
            (
                Operand::Glob { left, right },
                Operand::Glob {
                    left: other_left,
                    right: other_right,
                },
            ) => left == other_left && right == other_right,
            (
                Operand::CaseEqual { left, right, value },
                Operand::CaseEqual {
                    left: other_left,
                    right: other_right,
                    value: other_value,
                },
            ) => left == other_left && right == other_right && value == other_value,
            (
                Operand::Named { operand, name },
                Operand::Named {
                    operand: other_operand,
                    name: other_name,
                },
            ) => name == other_name && operand == other_operand,
            _ => false,
        })
    }
}

impl std::fmt::Debug for Operand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        query_language::depth::deeper(|| match self {
            Operand::Field(field) => f.debug_tuple("Field").field(field).finish(),
            Operand::TimeFn(function, field) => f
                .debug_tuple("TimeFn")
                .field(function)
                .field(field)
                .finish(),
            Operand::Literal { value, kind } => f
                .debug_struct("Literal")
                .field("value", value)
                .field("kind", kind)
                .finish(),
            Operand::Arithmetic {
                operator,
                left,
                right,
                kind,
            } => f
                .debug_struct("Arithmetic")
                .field("operator", operator)
                .field("left", left)
                .field("right", right)
                .field("kind", kind)
                .finish(),
            Operand::Length(inner) => f.debug_tuple("Length").field(inner).finish(),
            Operand::Absolute(inner) => f.debug_tuple("Absolute").field(inner).finish(),
            Operand::Text(inner) => f.debug_tuple("Text").field(inner).finish(),
            Operand::Glob { left, right } => f
                .debug_struct("Glob")
                .field("left", left)
                .field("right", right)
                .finish(),
            Operand::CaseEqual { left, right, value } => f
                .debug_struct("CaseEqual")
                .field("left", left)
                .field("right", right)
                .field("value", value)
                .finish(),
            Operand::Named { operand, name } => f
                .debug_struct("Named")
                .field("operand", operand)
                .field("name", name)
                .finish(),
        })
    }
}

impl Operand {
    pub fn name(&self) -> String {
        query_language::depth::deeper(|| match self {
            Operand::Field(f) => f.name(),
            Operand::TimeFn(func, f) => format!("{}({})", func.name(), f.name()),
            Operand::Literal { value, .. } => format!("{value:?}"),
            Operand::Arithmetic {
                operator,
                left,
                right,
                ..
            } => format!("({} {} {})", left.name(), operator.symbol(), right.name()),
            Operand::Length(value) => format!("len({})", value.name()),
            Operand::Absolute(value) => format!("abs({})", value.name()),
            Operand::Text(value) => format!("text({})", value.name()),
            Operand::Glob { left, right } => format!("glob({}, {})", left.name(), right.name()),
            Operand::CaseEqual { left, right, value } => format!(
                "case_eq({}, {}, {})",
                left.name(),
                right.name(),
                value.name()
            ),
            Operand::Named { name, .. } => name.clone(),
        })
    }

    /// The shared semantic kind, before physical payload narrowing.
    pub fn kind(&self) -> query_language::catalog::FieldKind {
        use query_language::catalog::FieldKind as K;
        match self {
            Self::Field(FieldRef::Native(field)) => super::syntax::field_kind(field.field_type()),
            Self::Field(FieldRef::Field(field)) => super::syntax::field_kind(field.ty),
            Self::Field(FieldRef::TypedPayload { ty, .. }) => super::syntax::field_kind(*ty),
            Self::Field(FieldRef::TypedGroup(column)) => column.info().field.kind.clone(),
            Self::Field(_) => K::Json,
            Self::TimeFn(TimeFn::Hour | TimeFn::Year, _) | Self::Length(_) => K::Integer,
            Self::TimeFn(TimeFn::Day | TimeFn::Week | TimeFn::Month, _) => K::Time,
            Self::TimeFn(_, _) => K::Text,
            Self::Literal { kind, .. } | Self::Arithmetic { kind, .. } => kind.clone(),
            Self::CaseEqual { value, .. } => value.kind(),
            Self::Absolute(value) => value.kind(),
            Self::Text(_) => K::Text,
            Self::Glob { .. } => K::Bool,
            Self::Named { operand, .. } => operand.kind(),
        }
    }
    pub fn field_type(&self) -> Result<super::FieldType, super::QueryError> {
        super::typed::operand_type(self)
    }
    pub fn unnamed(&self) -> &Self {
        match self {
            Self::Named { operand, .. } => operand.unnamed(),
            other => other,
        }
    }
    /// The column name a grouped stage gives this key.
    pub fn column(&self) -> String {
        query_language::depth::deeper(|| match self {
            Operand::Field(f) => f.name(),
            Operand::TimeFn(func, _) => func.name().to_string(),
            Operand::Literal { .. } => "value".into(),
            Operand::Arithmetic {
                operator,
                left,
                right,
                ..
            } => format!("{}_{}_{}", left.column(), operator.word(), right.column()),
            Operand::Length(_) => "len".into(),
            Operand::Absolute(_) => "abs".into(),
            Operand::Text(_) => "text".into(),
            Operand::CaseEqual { .. } => "case_eq".into(),
            Operand::Glob { .. } => "glob".into(),
            Operand::Named { name, .. } => name.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldRef {
    Native(NativeField),
    Field(&'static Field),
    /// `payload.a.b`: a path into the payload.
    Payload(Vec<String>),
    /// A payload path bound to the registered event schemas.
    TypedPayload {
        path: Vec<String>,
        ty: super::FieldType,
    },
    /// A column of a grouped stage: a key or an aggregate.
    Group(String),
    /// A shared-bound output column whose physical type is refreshed by binding.
    TypedGroup(BoundColumn),
}

/// Product-owned values generated from the source relation's reading context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeField {
    Identity,
    Snippet,
    SpanStart,
    SpanEnd,
}
impl NativeField {
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "id" => Self::Identity,
            "snippet" => Self::Snippet,
            "start" => Self::SpanStart,
            "end" => Self::SpanEnd,
            _ => return None,
        })
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Identity => "id",
            Self::Snippet => "snippet",
            Self::SpanStart => "start",
            Self::SpanEnd => "end",
        }
    }
    pub fn field_type(self) -> super::FieldType {
        match self {
            Self::Identity | Self::Snippet => super::FieldType::Text,
            _ => super::FieldType::Time,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundColumn {
    info: std::sync::Arc<query_language::scope::FieldInfo>,
    physical: Option<super::FieldType>,
}
impl BoundColumn {
    pub(super) fn new(info: query_language::scope::FieldInfo) -> Self {
        Self {
            info: std::sync::Arc::new(info),
            physical: None,
        }
    }
    pub fn name(&self) -> &str {
        &self.info.field.name
    }
    pub fn field_type(&self) -> Option<super::FieldType> {
        self.physical
    }
    /// Calendar label lineage retained through projection and grouping.
    pub fn time_function(&self) -> Option<TimeFn> {
        use query_language::scope::{FieldInfo, Origin, TypeRule};
        fn function(info: &FieldInfo) -> Option<TimeFn> {
            match &info.origin {
                Origin::Group { input, .. } | Origin::Matched { input } => function(input),
                Origin::Function {
                    arguments,
                    rule: TypeRule::Input(index),
                    ..
                } => arguments.get(*index).and_then(function),
                Origin::Aggregate {
                    inputs,
                    rule: TypeRule::Input(index),
                    ..
                } => inputs.get(*index).and_then(function),
                Origin::Function {
                    name,
                    rule: TypeRule::Fixed,
                    ..
                } => TimeFn::from_name(name),
                _ => None,
            }
        }
        function(&self.info)
    }
    pub(super) fn info(&self) -> &query_language::scope::FieldInfo {
        &self.info
    }
    pub fn generated(name: impl Into<String>, ty: super::FieldType) -> Self {
        Self {
            info: std::sync::Arc::new(query_language::scope::FieldInfo {
                field: query_language::catalog::Field {
                    name: name.into(),
                    kind: super::syntax::field_kind(ty),
                    ..Default::default()
                },
                origin: query_language::scope::Origin::Generated,
            }),
            physical: Some(ty),
        }
    }
    pub(super) fn clear_binding(&mut self) {
        self.physical = None;
    }
    pub(super) fn bind(&mut self, ty: super::FieldType) {
        self.physical = Some(ty);
    }
}

impl FieldRef {
    pub fn name(&self) -> String {
        match self {
            FieldRef::Native(field) => field.name().into(),
            FieldRef::Field(f) => f.name.to_string(),
            FieldRef::Payload(path) | FieldRef::TypedPayload { path, .. } => {
                format!("payload.{}", path.join("."))
            }
            FieldRef::Group(name) => name.clone(),
            FieldRef::TypedGroup(column) => column.name().to_owned(),
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
    Year,
}

impl TimeFn {
    pub const ALL: [TimeFn; 7] = [
        TimeFn::Day,
        TimeFn::Hour,
        TimeFn::Week,
        TimeFn::Weekday,
        TimeFn::Month,
        TimeFn::MonthDay,
        TimeFn::Year,
    ];

    pub fn name(self) -> &'static str {
        match self {
            TimeFn::Day => "day",
            TimeFn::Hour => "hour",
            TimeFn::Week => "week",
            TimeFn::Weekday => "weekday",
            TimeFn::Month => "month",
            TimeFn::MonthDay => "month_day",
            TimeFn::Year => "year",
        }
    }

    pub fn from_name(name: &str) -> Option<TimeFn> {
        TimeFn::ALL.into_iter().find(|f| f.name() == name)
    }
}

pub use query_language::ast::Op;
pub trait OrderingOp {
    fn ordering(self) -> bool;
}
impl OrderingOp for Op {
    fn ordering(self) -> bool {
        matches!(self, Self::Lt | Self::Le | Self::Gt | Self::Ge)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Unknown,
    Text(String),
    /// A text parameter checked for its type while saving a definition.
    PlaceholderText(String),
    /// A text value with `*` wildcards.
    Integer(i64),
    Number(f64),
    Bool(bool),
    Time(Timestamp),
    Id(Uuid),
    /// Any of: `field = [a, b]`.
    List(Vec<Value>),
    /// A value evaluated on the current row rather than a supplied literal.
    Operand(Box<Operand>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Stage {
    /// A filter on the rows of the stage before it (after a grouping, on the
    /// groups).
    Where(Expr),
    Join(Join),
    AsOf(AsOf),
    Group(Group),
    /// Select original rows by their order within each key group.
    GroupFirst {
        keys: Vec<Operand>,
        count: u64,
    },
    Sort(Vec<SortKey>),
    Limit(u64),
    Fields(Vec<Operand>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub on: JoinOn,
    /// Empty selects the language's nearest matching row; explicit keys select first.
    pub pick: Vec<SortKey>,
    pub inner: Box<Query>,
    /// Every visible column of the bound inner relation, with physical lineage.
    pub columns: Vec<Operand>,
    /// Matched fields actually read by the containing native relation.
    pub required: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JoinOn {
    /// Current pair and semijoin predicates share the declared time anchors.
    Order {
        relation: JoinKind,
        within_secs: Option<String>,
        at: TimePoint,
    },
    Key {
        left: FieldRef,
        right: FieldRef,
    },
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
    MatchWhen,
    MatchStart,
    MatchEnd,
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
    pub fill: Option<Fill>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fill {
    pub from: Timestamp,
    pub to: Timestamp,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Aggregate {
    pub function: AggFn,
    pub argument: Option<Operand>,
    pub name: Option<String>,
}

impl Aggregate {
    /// The column the aggregate becomes: `count`, `sum_duration`.
    pub fn column(&self) -> String {
        if let Some(name) = &self.name {
            return name.clone();
        }
        match &self.argument {
            None => self.function.name().to_string(),
            Some(f) => format!("{}_{}", self.function.name(), f.column()),
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

impl Query {
    pub fn finite_time_bounds(&self, field: &FieldRef) -> Option<(Timestamp, Timestamp)> {
        super::fill::finite_time_bounds(self, field)
    }
    pub fn similar_reference(&self) -> Result<Option<Uuid>, super::QueryError> {
        fn visit(
            e: &Expr,
            allowed: bool,
            found: &mut Option<Uuid>,
        ) -> Result<(), super::QueryError> {
            query_language::depth::deeper(|| {
                match e {
                    Expr::Similar { reference } => {
                        if !allowed {
                            return Err(super::QueryError::new(
                                "similar is a ranking directive and cannot appear under OR or NOT",
                            ));
                        }
                        if found.is_some_and(|old| old != *reference) {
                            return Err(super::QueryError::new(
                                "a query must choose one supplied similarity vector",
                            ));
                        }
                        *found = Some(*reference);
                    }
                    Expr::And(a, b) => {
                        visit(a, allowed, found)?;
                        visit(b, allowed, found)?;
                    }
                    Expr::Or(a, b) => {
                        visit(a, false, found)?;
                        visit(b, false, found)?;
                    }
                    Expr::Not(e) => visit(e, false, found)?,
                    _ => {}
                }
                Ok(())
            })
        }
        let mut found = None;
        if let Some(e) = &self.filter {
            visit(e, true, &mut found)?;
        }
        for stage in &self.stages {
            if let Stage::Where(e) = stage {
                visit(e, true, &mut found)?;
            }
        }
        if found.is_some() && self.stages.iter().any(|s| matches!(s, Stage::Group(_))) {
            return Err(super::QueryError::new(
                "similar ranks event rows before grouping",
            ));
        }
        Ok(found)
    }
}
impl Expr {
    /// Search terms in this relation, excluding negated predicates and inner queries.
    pub fn positive_words(&self) -> Vec<&Words> {
        fn original_text(info: &query_language::scope::FieldInfo) -> bool {
            use query_language::scope::Origin;
            match &info.origin {
                Origin::Catalog { field, .. } => field == "text",
                Origin::Group { input, .. } => original_text(input),
                _ => false,
            }
        }
        match self {
            Self::Words(words) => vec![words],
            Self::FieldWords { operand, words }
                if match operand {
                    Operand::Field(FieldRef::Field(field)) => field.name == "text",
                    Operand::Field(FieldRef::TypedGroup(column)) => original_text(column.info()),
                    _ => false,
                } =>
            {
                vec![words]
            }
            Self::And(left, right) | Self::Or(left, right) => {
                let mut words = left.positive_words();
                words.extend(right.positive_words());
                words
            }
            _ => Vec::new(),
        }
    }

    fn positive_order_count(&self) -> usize {
        match self {
            Self::Order(_) => 1,
            Self::And(left, right) => {
                (left.positive_order_count() + right.positive_order_count()).min(2)
            }
            _ => 0,
        }
    }
    /// Only one order under positive AND conditions exposes its chosen row.
    /// Every predicate operand still reads the relation entering that filter.
    pub fn exposed_order(&self) -> Option<&Join> {
        if self.positive_order_count() != 1 {
            return None;
        }
        match self {
            Self::Order(join) => Some(join),
            Self::And(left, right) => left.exposed_order().or_else(|| right.exposed_order()),
            _ => None,
        }
    }
    pub(super) fn exposed_order_mut(&mut self) -> Option<&mut Join> {
        if self.positive_order_count() != 1 {
            return None;
        }
        match self {
            Self::Order(join) => Some(join),
            Self::And(left, right) => left
                .exposed_order_mut()
                .or_else(|| right.exposed_order_mut()),
            _ => None,
        }
    }

    /// Remove rank directives structurally before compiling the predicate.
    pub fn predicate(&self) -> Option<Expr> {
        match self {
            Self::Similar { .. } => None,
            Self::And(a, b) => match (a.predicate(), b.predicate()) {
                (Some(a), Some(b)) => Some(Self::And(Box::new(a), Box::new(b))),
                (a, b) => a.or(b),
            },
            _ => Some(self.clone()),
        }
    }
}

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

#[derive(Clone, Debug, PartialEq)]
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

impl Operand {
    pub fn name(&self) -> String {
        match self {
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
        }
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
        match self {
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
        }
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

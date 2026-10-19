//! SQL fragments with their parameters. A fragment is text and values in
//! order; rendering numbers the placeholders with sea-query's raw SQL builder,
//! so a fragment can be built before the text around it and nested anywhere.
use sea_query::raw_sql::RawSqlQueryBuilder;
use sea_query::{PostgresQueryBuilder, Value};
use sqlx::Postgres;
use sqlx::postgres::PgArguments;
use sqlx::query::QueryScalar;

#[derive(Clone, Debug, Default)]
pub(crate) struct Frag {
    pieces: Vec<Piece>,
}

#[derive(Clone, Debug)]
enum Piece {
    Text(String),
    Param(Value),
}

impl Frag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn text(s: impl Into<String>) -> Self {
        let mut f = Self::new();
        f.push(s);
        f
    }

    pub fn push(&mut self, s: impl Into<String>) -> &mut Self {
        self.pieces.push(Piece::Text(s.into()));
        self
    }

    pub fn param(&mut self, v: impl Into<Value>) -> &mut Self {
        self.pieces.push(Piece::Param(v.into()));
        self
    }

    pub fn append(&mut self, other: Frag) -> &mut Self {
        self.pieces.extend(other.pieces);
        self
    }

    /// `parts` joined by `sep`.
    pub fn join(parts: Vec<Frag>, sep: &str) -> Frag {
        let mut out = Frag::new();
        for (i, p) in parts.into_iter().enumerate() {
            if i > 0 {
                out.push(sep);
            }
            out.append(p);
        }
        out
    }

    /// `(<self>)`.
    pub fn parenthesized(self) -> Frag {
        let mut out = Frag::text("(");
        out.append(self);
        out.push(")");
        out
    }

    /// The SQL text with numbered placeholders, and the parameters in order,
    /// as values that can cross an `.await`.
    pub fn render(&self) -> (String, Vec<Param>) {
        let mut builder = RawSqlQueryBuilder::new(PostgresQueryBuilder);
        let mut values = Vec::new();
        for piece in &self.pieces {
            match piece {
                Piece::Text(t) => {
                    builder.push_fragment(t);
                }
                Piece::Param(v) => {
                    builder.push_parameters(1);
                    values.push(Param::from(v.clone()));
                }
            }
        }
        (builder.finish(), values)
    }
}

/// A parameter of a rendered statement. sea-query's values are not `Send`;
/// these are, so a rendered statement can be awaited on any thread.
#[derive(Clone, Debug)]
pub(crate) enum Param {
    Bool(Option<bool>),
    Int(Option<i32>),
    BigInt(Option<i64>),
    Double(Option<f64>),
    Text(Option<String>),
    Uuid(Option<uuid::Uuid>),
    Time(Option<time::OffsetDateTime>),
}

impl From<Value> for Param {
    fn from(v: Value) -> Self {
        match v {
            Value::Bool(b) => Param::Bool(b),
            Value::Int(i) => Param::Int(i),
            Value::BigInt(i) => Param::BigInt(i),
            Value::Double(d) => Param::Double(d),
            Value::String(s) => Param::Text(s),
            Value::Uuid(u) => Param::Uuid(u),
            Value::TimeDateTimeWithTimeZone(t) => Param::Time(t),
            other => unreachable!("the compiler emits no {other:?} parameters"),
        }
    }
}

/// Bind parameters to a scalar sqlx query, in order.
pub(crate) fn bind_scalar<'q, O>(
    mut q: QueryScalar<'q, Postgres, O, PgArguments>,
    values: Vec<Param>,
) -> QueryScalar<'q, Postgres, O, PgArguments> {
    for v in values {
        q = match v {
            Param::Bool(b) => q.bind(b),
            Param::Int(i) => q.bind(i),
            Param::BigInt(i) => q.bind(i),
            Param::Double(d) => q.bind(d),
            Param::Text(s) => q.bind(s),
            Param::Uuid(u) => q.bind(u),
            Param::Time(t) => q.bind(t),
        };
    }
    q
}

#[cfg(test)]
mod tests;

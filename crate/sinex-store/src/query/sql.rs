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

    pub fn render(&self) -> (String, Vec<Value>) {
        let mut builder = RawSqlQueryBuilder::new(PostgresQueryBuilder);
        let mut values = Vec::new();
        for piece in &self.pieces {
            match piece {
                Piece::Text(t) => {
                    builder.push_fragment(t);
                }
                Piece::Param(v) => {
                    builder.push_parameters(1);
                    values.push(v.clone());
                }
            }
        }
        (builder.finish(), values)
    }
}

/// Bind sea-query values to a scalar sqlx query, in order.
pub(crate) fn bind_scalar<'q, O>(
    mut q: QueryScalar<'q, Postgres, O, PgArguments>,
    values: Vec<Value>,
) -> QueryScalar<'q, Postgres, O, PgArguments> {
    for v in values {
        q = match v {
            Value::Bool(b) => q.bind(b),
            Value::BigInt(i) => q.bind(i),
            Value::Int(i) => q.bind(i),
            Value::Double(d) => q.bind(d),
            Value::String(s) => q.bind(s),
            Value::Uuid(u) => q.bind(u),
            Value::TimeDateTimeWithTimeZone(t) => q.bind(t),
            Value::Json(j) => q.bind(j.map(|j| *j)),
            other => unreachable!("the compiler emits no {other:?} parameters"),
        };
    }
    q
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_number_in_text_order_across_nesting() {
        let mut inner = Frag::text("x = ");
        inner.param(1i64);
        let mut outer = Frag::text("SELECT ");
        outer
            .param("a")
            .push(" WHERE ")
            .append(inner.parenthesized());
        let (sql, values) = outer.render();
        assert_eq!(sql, "SELECT $1 WHERE (x = $2)");
        assert_eq!(values.len(), 2);
    }
}

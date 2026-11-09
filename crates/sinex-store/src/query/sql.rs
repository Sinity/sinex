//! SQL fragments with their parameters. A fragment is text and values in
//! order; rendering numbers the placeholders `$1, $2, …` in text order, so a
//! fragment can be built before the text around it and nested anywhere.
use std::fmt::Write as _;

use sqlx::Postgres;
use sqlx::postgres::PgArguments;
use sqlx::query::QueryScalar;

#[derive(Clone, Debug, Default)]
pub(crate) struct SqlFragment {
    pieces: Vec<FragmentPart>,
}

#[derive(Clone, Debug)]
enum FragmentPart {
    Text(String),
    Param(Param),
}

impl SqlFragment {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn text(s: impl Into<String>) -> Self {
        let mut f = Self::new();
        f.push(s);
        f
    }

    pub(crate) fn push(&mut self, s: impl Into<String>) -> &mut Self {
        self.pieces.push(FragmentPart::Text(s.into()));
        self
    }

    pub(crate) fn param(&mut self, v: impl Into<Param>) -> &mut Self {
        self.pieces.push(FragmentPart::Param(v.into()));
        self
    }

    pub(crate) fn append(&mut self, other: SqlFragment) -> &mut Self {
        self.pieces.extend(other.pieces);
        self
    }

    /// `parts` joined by `sep`.
    pub(crate) fn join(parts: Vec<SqlFragment>, sep: &str) -> SqlFragment {
        let mut out = SqlFragment::new();
        for (i, p) in parts.into_iter().enumerate() {
            if i > 0 {
                out.push(sep);
            }
            out.append(p);
        }
        out
    }

    /// `(<self>)`.
    pub(crate) fn parenthesized(self) -> SqlFragment {
        let mut out = SqlFragment::text("(");
        out.append(self);
        out.push(")");
        out
    }

    /// The SQL text with numbered placeholders, and the parameters in order.
    pub(crate) fn render(&self) -> (String, Vec<Param>) {
        let mut sql = String::new();
        let mut values = Vec::new();
        for piece in &self.pieces {
            match piece {
                FragmentPart::Text(t) => sql.push_str(t),
                FragmentPart::Param(v) => {
                    values.push(v.clone());
                    write!(sql, "${}", values.len()).expect("writing to a String cannot fail");
                }
            }
        }
        (sql, values)
    }
}

/// A parameter of a rendered statement.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Param {
    Bool(Option<bool>),
    Int(Option<i32>),
    BigInt(Option<i64>),
    Double(Option<f64>),
    Text(Option<String>),
    Uuid(Option<uuid::Uuid>),
    Time(Option<time::OffsetDateTime>),
}

impl From<bool> for Param {
    fn from(v: bool) -> Self {
        Param::Bool(Some(v))
    }
}

impl From<i32> for Param {
    fn from(v: i32) -> Self {
        Param::Int(Some(v))
    }
}

impl From<i64> for Param {
    fn from(v: i64) -> Self {
        Param::BigInt(Some(v))
    }
}

impl From<f64> for Param {
    fn from(v: f64) -> Self {
        Param::Double(Some(v))
    }
}

impl From<String> for Param {
    fn from(v: String) -> Self {
        Param::Text(Some(v))
    }
}

impl From<&str> for Param {
    fn from(v: &str) -> Self {
        Param::Text(Some(v.to_owned()))
    }
}

impl From<uuid::Uuid> for Param {
    fn from(v: uuid::Uuid) -> Self {
        Param::Uuid(Some(v))
    }
}

impl From<time::OffsetDateTime> for Param {
    fn from(v: time::OffsetDateTime) -> Self {
        Param::Time(Some(v))
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

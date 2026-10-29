//! Follow reads advance only through materialized, bounded commit ranges.
use super::*;
use sinex_model::query::{Op, SortBy, SortKey};

/// Safe upper position for event subscriptions. Requiring the search projection
/// for every event query also covers text fields selected or used by joins.
pub async fn follow_position(conn: &mut PgConnection) -> Result<i64> {
    let committed = current_seq(conn).await?;
    let (indexed, _) =
        crate::commit_log::consumer_position(conn, crate::read_models::SEARCH_TEXT).await?;
    Ok(committed.min(indexed))
}

/// Keep every page inside the captured (after, through] interval, including
/// commits admitted between the position read and the first query page.
pub fn follow_query(query: &Query, after: i64, through: i64) -> Query {
    let field = || {
        FieldRef::Field(
            Table::Events
                .field("commit_seq")
                .expect("events have commit_seq"),
        )
    };
    let range = Expr::And(
        Box::new(Expr::Compare {
            lhs: Operand::Field(field()),
            op: Op::Gt,
            rhs: sinex_model::query::Value::Integer(after),
        }),
        Box::new(Expr::Compare {
            lhs: Operand::Field(field()),
            op: Op::Le,
            rhs: sinex_model::query::Value::Integer(through),
        }),
    );
    let mut query = query.clone();
    query.filter = Some(match query.filter.take() {
        Some(filter) => Expr::And(Box::new(filter), Box::new(range)),
        None => range,
    });
    query.stages.push(Stage::Sort(vec![SortKey {
        key: SortBy::Operand(Operand::Field(field())),
        descending: false,
    }]));
    query
}

//! Recall selects its shell anchor through the same compiler and commit view
//! as ordinary queries. Its bounds describe recorded extent, not a proven end.
use super::*;
use sinex_model::query::{Alias, Op, SortBy, SortKey, Value};

pub(super) async fn latest(
    conn: &mut PgConnection,
    filter: Option<Expr>,
    ctx: &Context,
    seq: i64,
) -> Result<Option<Json>> {
    let alias = Alias::from_name("shell-sessions").expect("shell sessions declared");
    let selector = Query {
        source: Source::Unit {
            unit: Unit::Events,
            alias: Some(alias),
        },
        filter,
        stages: vec![
            Stage::Sort(vec![SortKey {
                key: SortBy::Operand(Operand::Field(FieldRef::Field(
                    Unit::Events.field("when").expect("when declared"),
                ))),
                descending: true,
            }]),
            Stage::Limit(1),
        ],
        ranked: false,
    };
    let compiled = Compiler::new(ctx, seq).compile(&selector)?;
    let rendered = render_page(compiled, 0, 1);
    let (text, values) = rendered.rows;
    Ok(sql::bind_scalar(sqlx::query_scalar(&text), values)
        .fetch_optional(conn)
        .await?)
}

/// A shell group can consist of one point, or end at its last untimed-duration
/// command. Both recorded endpoints belong to this observed envelope.
pub(super) fn observed_window(from: jiff::Timestamp, to: jiff::Timestamp) -> Expr {
    let when = || {
        Operand::Field(FieldRef::Field(
            Unit::Events.field("when").expect("when declared"),
        ))
    };
    Expr::And(
        Box::new(Expr::Compare {
            lhs: when(),
            op: Op::Ge,
            rhs: Value::Time(from),
        }),
        Box::new(Expr::Compare {
            lhs: when(),
            op: Op::Le,
            rhs: Value::Time(to),
        }),
    )
}

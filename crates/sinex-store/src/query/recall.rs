//! Recall selects its shell anchor through the same compiler and commit view
//! as ordinary queries. Its bounds describe recorded extent, not a proven end.
use super::*;
use sinex_model::query::{Alias, Op, SortBy, SortKey, Value};

pub(super) enum Selection {
    Window {
        from: jiff::Timestamp,
        to: jiff::Timestamp,
        inclusive: bool,
        anchor: Option<Json>,
    },
    NoKnownSession,
}

pub(super) async fn select(
    conn: &mut PgConnection,
    source: &Source,
    ctx: &Context,
    seq: i64,
) -> Result<Selection> {
    match source {
        Source::Recall { from, to } => Ok(Selection::Window {
            from: *from,
            to: *to,
            inclusive: false,
            anchor: None,
        }),
        Source::RecallLast { filter } => {
            let Some(anchor) = latest(conn, filter.clone(), ctx, seq).await? else {
                return Ok(Selection::NoKnownSession);
            };
            let time = |field: &str| -> Result<jiff::Timestamp> {
                anchor[field]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        Error::Invalid(
                            "selected shell group's observed bounds are unavailable to this caller"
                                .into(),
                        )
                    })
            };
            Ok(Selection::Window {
                from: time("start")?,
                to: time("observed_until")?,
                inclusive: true,
                anchor: Some(anchor),
            })
        }
        _ => unreachable!("recall reader takes a recall source"),
    }
}

impl Selection {
    pub(super) fn filter(&self) -> Expr {
        let when = || {
            Operand::Field(FieldRef::Field(
                Unit::Events.field("when").expect("when declared"),
            ))
        };
        match self {
            Self::NoKnownSession => {
                // An empty input relation still runs the caller's stages,
                // including count, fields and grouping.
                let known = Expr::Exists(when());
                Expr::And(
                    Box::new(known.clone()),
                    Box::new(Expr::Not(Box::new(known))),
                )
            }
            Self::Window {
                from,
                to,
                inclusive: true,
                ..
            } => observed_window(*from, *to),
            Self::Window { from, to, .. } => Expr::Within {
                lhs: when(),
                from: *from,
                to: *to,
            },
        }
    }

    pub(super) async fn account(&self, conn: &mut PgConnection, seq: i64) -> Result<Json> {
        match self {
            Self::NoKnownSession => Ok(serde_json::json!({
                "anchor": null, "status": "no_known_timed_shell_session",
                "reason": "No matching recorded shell group; untimed observations or pending derivations may still exist."
            })),
            Self::Window {
                from,
                to,
                inclusive,
                anchor,
            } => {
                let mut account = walk::recall_summary(conn, *from, *to, seq, *inclusive).await?;
                if let Some(anchor) = anchor {
                    account["anchor"] = serde_json::json!({"kind":"shell_group","bounds":"observed_envelope","row":anchor});
                }
                Ok(account)
            }
        }
    }
}

async fn latest(
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
    let rendered = render_page(compiled, 0, 1, false);
    let (text, values) = rendered.rows;
    Ok(sql::bind_scalar(sqlx::query_scalar(&text), values)
        .fetch_optional(conn)
        .await?)
}

/// A shell group can consist of one point, or end at its last untimed-duration
/// command. Both recorded endpoints belong to this observed envelope.
fn observed_window(from: jiff::Timestamp, to: jiff::Timestamp) -> Expr {
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

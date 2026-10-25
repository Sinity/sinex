//! Recall selects its shell session through the same compiler and commit view
//! as ordinary queries. Its bounds describe recorded extent, not a proven end.
use super::*;
use sinex_model::query::{Alias, Op, SortBy, SortKey, Value};

pub(super) enum Selection {
    Window {
        from: jiff::Timestamp,
        to: jiff::Timestamp,
        inclusive: bool,
        session: Option<Json>,
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
            session: None,
        }),
        Source::RecallLast { filter } => {
            let Some(session) = latest(conn, filter.clone(), ctx, seq).await? else {
                return Ok(Selection::NoKnownSession);
            };
            let time = |field: &str| -> Result<jiff::Timestamp> {
                session[field]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| {
                        Error::Invalid(
                            "the selected shell session's times are unavailable to this caller"
                                .into(),
                        )
                    })
            };
            Ok(Selection::Window {
                from: time("start")?,
                to: time("observed_until")?,
                inclusive: true,
                session: Some(session),
            })
        }
        _ => unreachable!("recall reader takes a recall source"),
    }
}

impl Selection {
    pub(super) fn filter(&self) -> Expr {
        let when = || {
            Operand::Field(FieldRef::Field(
                Table::Events.field("when").expect("when declared"),
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
                "session": null, "status": "no_known_timed_shell_session",
                "reason": "No matching recorded shell session; untimed observations or pending derivations may still exist."
            })),
            Self::Window {
                from,
                to,
                inclusive,
                session,
            } => {
                let mut account = walk::recall_summary(conn, *from, *to, seq, *inclusive).await?;
                if let Some(session) = session {
                    account["session"] =
                        serde_json::json!({"bounds":"recorded_commands","row":session});
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
        source: Source::Table {
            table: Table::Events,
            alias: Some(alias),
        },
        filter,
        stages: vec![
            Stage::Sort(vec![SortKey {
                key: SortBy::Operand(Operand::Field(FieldRef::Field(
                    Table::Events.field("when").expect("when declared"),
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

/// A shell session can consist of one point, or end at its last command of
/// unknown duration. Both recorded ends belong to the window.
fn observed_window(from: jiff::Timestamp, to: jiff::Timestamp) -> Expr {
    let when = || {
        Operand::Field(FieldRef::Field(
            Table::Events.field("when").expect("when declared"),
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

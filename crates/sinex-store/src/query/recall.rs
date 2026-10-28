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
    NoKnownMark,
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
        Source::RecallMark { number, window } => {
            let offset = i64::try_from(number.saturating_sub(1)).map_err(|_| {
                Error::Invalid("mark number is outside the archive's row range".into())
            })?;
            let mark = sqlx::query_scalar!(
                r#"SELECT jsonb_build_object('id',id,'when',ts_orig,'payload',payload) AS "row!"
                FROM core.visible_events WHERE event_type=$3 AND ts_orig IS NOT NULL
                AND payload IS NOT NULL AND NOT payload ? 'withheld'
                AND commit_seq <= $1 AND NOT audit.is_archived_at(id,$1)
                ORDER BY ts_orig DESC, id DESC OFFSET $2 LIMIT 1"#,
                seq,
                offset,
                <sinex_model::event_types::query::Mark as sinex_model::EventPayload>::EVENT_TYPE
            )
            .fetch_optional(&mut *conn)
            .await?;
            let Some(mark) = mark else {
                return Ok(Selection::NoKnownMark);
            };
            let at: jiff::Timestamp = mark["when"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| Error::Invalid("mark time is unavailable".into()))?;
            let width = jiff::SignedDuration::from_secs_f64(*window);
            let from = at
                .checked_sub(width)
                .map_err(|e| Error::Invalid(e.to_string()))?;
            let to = at
                .checked_add(width)
                .map_err(|e| Error::Invalid(e.to_string()))?;
            Ok(Selection::Window {
                from,
                to,
                inclusive: false,
                session: Some(serde_json::json!({"mark":mark})),
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
            Self::NoKnownSession | Self::NoKnownMark => {
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
            } => Expr::Or(
                Box::new(observed_window(*from, *to)),
                Box::new(Expr::Overlaps {
                    from: *from,
                    to: Some(*to),
                }),
            ),
            Self::Window { from, to, .. } => Expr::Or(
                Box::new(Expr::Within {
                    lhs: when(),
                    from: *from,
                    to: *to,
                }),
                Box::new(Expr::Overlaps {
                    from: *from,
                    to: Some(*to),
                }),
            ),
        }
    }

    pub(super) async fn account(
        &self,
        conn: &mut PgConnection,
        seq: i64,
        rows: &Query,
        ctx: &Context,
    ) -> Result<Json> {
        match self {
            Self::NoKnownMark => Ok(
                serde_json::json!({"mark":null,"status":"no_known_moment_mark","reason":"No visible moment mark matches this number."}),
            ),
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
                let mut account =
                    walk::recall_summary(conn, *from, *to, seq, *inclusive, rows, ctx).await?;
                if let Some(session) = session {
                    if let Some(mark) = session.get("mark") {
                        account["mark"] = mark.clone();
                        account["links"] = mark_links(conn, mark, *from, seq).await?;
                    } else {
                        account["session"] =
                            serde_json::json!({"bounds":"recorded_commands","row":session});
                    }
                }
                account["citation"] = serde_json::json!(format!("sinex:window/{from}/{to}"));
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

async fn mark_links(
    conn: &mut PgConnection,
    mark: &Json,
    from: jiff::Timestamp,
    seq: i64,
) -> Result<Json> {
    let at = mark["when"]
        .as_str()
        .ok_or_else(|| Error::Invalid("mark time unavailable".into()))?;
    let at: jiff::Timestamp = at
        .parse()
        .map_err(|_| Error::Invalid("mark time invalid".into()))?;
    let ctx = Context::utc(at);
    let quote = |s: &str| serde_json::to_string(s).expect("strings serialize");
    let mut questions = Vec::new();
    if let Some(host) = mark["payload"]["host"].as_str() {
        questions.push((
            "session",
            format!(
                "shell-sessions where host = {} and overlaps {at}",
                quote(host)
            ),
        ));
    }
    let mut focus = format!("focus where overlaps {at}");
    for field in ["app", "title"] {
        if let Some(value) = mark["payload"][field].as_str() {
            focus.push_str(&format!(" and {field} = {}", quote(value)));
        }
    }
    questions.push(("focus", focus));
    if let Some(command) = mark["payload"]["command"].as_str() {
        let mut text = format!(
            "commands where command = {} and when >= {from} and when <= {at}",
            quote(command)
        );
        if let Some(cwd) = mark["payload"]["cwd"].as_str() {
            text.push_str(&format!(" and cwd = {}", quote(cwd)));
        }
        text.push_str(" sort when desc limit 1");
        questions.push(("command", text));
    }
    let mut links = serde_json::Map::new();
    for (name, text) in questions {
        let text = format!("{text} fields id,event_type,source,key,material,offset,output");
        let query = sinex_model::query::bind(&text, &ctx).map_err(|e| Error::Invalid(e.message))?;
        let (text, values) = {
            let compiled = Compiler::new(&ctx, seq).compile(&query)?;
            let mut sql = SqlFragment::text(format!(
                "SELECT {} FROM (",
                super::json_object(&compiled.columns)
            ));
            sql.append(compiled.answer).push(") q");
            sql.render()
        };
        let mut rows: Vec<Json> = super::sql::bind_scalar(sqlx::query_scalar(&text), values)
            .fetch_all(&mut *conn)
            .await?;
        for row in &mut rows {
            row["citation"] = if name == "session" {
                serde_json::json!(format!(
                    "sinex:at/{at}/{}",
                    row["event_type"].as_str().unwrap_or("")
                ))
            } else if let (Some(source), Some(kind), Some(key)) = (
                row["source"].as_str(),
                row["event_type"].as_str(),
                row["key"].as_str(),
            ) {
                serde_json::json!(format!("sinex:key/{source}/{kind}/{key}"))
            } else {
                serde_json::json!(format!("sinex:ev/{}", row["id"].as_str().unwrap_or("")))
            };
        }
        links.insert(name.into(), Json::Array(rows));
    }
    Ok(Json::Object(links))
}

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
                r#"SELECT jsonb_build_object('id',id,'source',source,'event_type',event_type,'key',equivalence_key,'material',material_id,'offset',anchor,'output',output_index,'when',ts_orig,'payload',payload) AS "row!"
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
    let (text,values)={
        let compiled=super::compile_query(conn,ctx,seq,&selector).await?;
        let mut rows=SqlFragment::text(format!("SELECT {} FROM (",super::json_object(&compiled.columns)));
        rows.append(compiled.answer).push(") q");rows.render()
    };
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
            let compiled = super::compile_query(conn,&ctx,seq,&query).await?;
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

/// Presentation products are outputs of recall, never inputs to themselves.
pub(super) fn presentation_types() -> &'static [&'static str] {
    &[
        <sinex_model::event_types::analysis::AnalysisRequest as sinex_model::EventPayload>::EVENT_TYPE,
        <sinex_model::event_types::analysis::AnalysisPanel as sinex_model::EventPayload>::EVENT_TYPE,
        <sinex_model::event_types::analysis::RecipeAnswer as sinex_model::EventPayload>::EVENT_TYPE,
        <sinex_model::event_types::analysis::Finding as sinex_model::EventPayload>::EVENT_TYPE,
        <sinex_model::event_types::work::ContextPack as sinex_model::EventPayload>::EVENT_TYPE,
    ]
}

pub(super) fn input_filter() -> Expr {
    Expr::Not(Box::new(Expr::Compare {
        lhs: Operand::Field(FieldRef::Field(
            Table::Events
                .field("event_type")
                .expect("event type declared"),
        )),
        op: Op::Eq,
        rhs: Value::List(
            presentation_types()
                .iter()
                .map(|kind| Value::Text((*kind).into()))
                .collect(),
        ),
    }))
}

fn record_reference(row: &Json) -> Result<Json> {
    if let (Some(source), Some(kind), Some(key)) = (
        row["source"].as_str(),
        row["event_type"].as_str(),
        row["key"].as_str(),
    ) {
        return Ok(serde_json::json!({"source":source,"event_type":kind,"key":key}));
    }
    if row["material"].is_string() && row["offset"].is_number() {
        return Ok(
            serde_json::json!({"material":row["material"],"offset":row["offset"],"output":row["output"],"event_type":row["event_type"]}),
        );
    }
    row["id"]
        .as_str()
        .map(|id| serde_json::json!({"event":id}))
        .ok_or_else(|| Error::Invalid("selected context has no record coordinates".into()))
}

impl Selection {
    pub(super) fn retained(&self, source: &Source) -> Result<Json> {
        Ok(match self {
            Self::NoKnownSession => serde_json::json!({"kind":"no_session"}),
            Self::NoKnownMark => serde_json::json!({"kind":"no_mark"}),
            Self::Window {
                from,
                to,
                inclusive,
                session: None,
            } => {
                serde_json::json!({"kind":"window","from":from.to_string(),"to":to.to_string(),"inclusive":inclusive})
            }
            Self::Window {
                session: Some(row), ..
            } => {
                if let Some(mark) = row.get("mark") {
                    let radius = match source {
                        Source::RecallMark { window, .. } => *window,
                        _ => return Err(Error::Invalid("mark selection has no radius".into())),
                    };
                    serde_json::json!({"kind":"mark","anchor":record_reference(mark)?,"radius_seconds":radius})
                } else {
                    serde_json::json!({"kind":"session","anchor":record_reference(row)?})
                }
            }
        })
    }

    pub(super) fn anchor_ids(&self) -> Vec<uuid::Uuid> {
        let Self::Window {
            session: Some(row), ..
        } = self
        else {
            return vec![];
        };
        let row = row.get("mark").unwrap_or(row);
        row["id"]
            .as_str()
            .and_then(|id| id.parse().ok())
            .into_iter()
            .collect()
    }
}

async fn anchor_id(conn: &mut PgConnection, anchor: &Json, seq: i64) -> Result<Option<uuid::Uuid>> {
    if let (Some(source), Some(kind), Some(key)) = (
        anchor["source"].as_str(),
        anchor["event_type"].as_str(),
        anchor["key"].as_str(),
    ) {
        return Ok(sqlx::query_scalar("SELECT id FROM core.visible_events WHERE source=$1 AND event_type=$2 AND equivalence_key=$3 AND commit_seq<=$4 AND NOT audit.is_archived_at(id,$4) AND payload IS NOT NULL AND NOT payload ? 'withheld' ORDER BY commit_seq DESC LIMIT 1")
            .bind(source).bind(kind).bind(key).bind(seq).fetch_optional(conn).await?);
    }
    if let (Some(material), Some(offset)) = (anchor["material"].as_str(), anchor["offset"].as_i64())
    {
        let material = material
            .parse::<uuid::Uuid>()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let output = match anchor.get("output") {
            None | Some(Json::Null) => 0,
            Some(value) => value
                .as_u64()
                .ok_or_else(|| Error::Invalid("invalid retained output index".into()))?,
        };
        let output = i64::try_from(output)
            .map_err(|_| Error::Invalid("retained output index beyond i64".into()))?;
        return Ok(sqlx::query_scalar("SELECT id FROM core.visible_events WHERE material_id=$1 AND anchor=$2 AND output_index=$3 AND commit_seq<=$4 AND NOT audit.is_archived_at(id,$4) AND payload IS NOT NULL AND NOT payload ? 'withheld' ORDER BY commit_seq DESC LIMIT 1")
            .bind(material).bind(offset).bind(output).bind(seq).fetch_optional(conn).await?);
    }
    let id = anchor["event"]
        .as_str()
        .and_then(|id| id.parse::<uuid::Uuid>().ok())
        .ok_or_else(|| Error::Invalid("invalid retained context coordinates".into()))?;
    Ok(sqlx::query_scalar("SELECT id FROM core.visible_events WHERE id=$1 AND commit_seq<=$2 AND NOT audit.is_archived_at(id,$2) AND payload IS NOT NULL AND NOT payload ? 'withheld'")
        .bind(id).bind(seq).fetch_optional(conn).await?)
}

pub(super) async fn retained(
    conn: &mut PgConnection,
    retained: &sinex_model::event_types::analysis::QuerySelection,
    ctx: &Context,
    seq: i64,
) -> Result<Selection> {
    let selected = &retained.selected;
    match selected["kind"].as_str() {
        Some("no_session") => Ok(Selection::NoKnownSession),
        Some("no_mark") => Ok(Selection::NoKnownMark),
        Some("window") => {
            let time = |name| {
                selected[name]
                    .as_str()
                    .and_then(|time| time.parse().ok())
                    .ok_or_else(|| Error::Invalid("invalid retained window".into()))
            };
            Ok(Selection::Window {
                from: time("from")?,
                to: time("to")?,
                inclusive: selected["inclusive"].as_bool().unwrap_or(false),
                session: None,
            })
        }
        Some("session") => {
            let Some(id) = anchor_id(conn, &selected["anchor"], seq).await? else {
                return Ok(Selection::NoKnownSession);
            };
            let filter = Expr::Compare {
                lhs: Operand::Field(FieldRef::Field(
                    Table::Events.field("id").expect("event id declared"),
                )),
                op: Op::Eq,
                rhs: Value::Id(id),
            };
            select(
                conn,
                &Source::RecallLast {
                    filter: Some(filter),
                },
                ctx,
                seq,
            )
            .await
        }
        Some("mark") => {
            let Some(id) = anchor_id(conn, &selected["anchor"], seq).await? else {
                return Ok(Selection::NoKnownMark);
            };
            let mark: Json = sqlx::query_scalar("SELECT jsonb_build_object('id',id,'source',source,'event_type',event_type,'key',equivalence_key,'material',material_id,'offset',anchor,'output',output_index,'when',ts_orig,'payload',payload) FROM core.visible_events WHERE id=$1 AND commit_seq<=$2 AND NOT audit.is_archived_at(id,$2)")
                .bind(id).bind(seq).fetch_one(conn).await?;
            let at: jiff::Timestamp = mark["when"]
                .as_str()
                .and_then(|time| time.parse().ok())
                .ok_or_else(|| Error::Invalid("retained mark time is unavailable".into()))?;
            let radius = selected["radius_seconds"]
                .as_f64()
                .filter(|radius| radius.is_finite() && *radius > 0.0)
                .ok_or_else(|| Error::Invalid("invalid retained mark radius".into()))?;
            let width = jiff::SignedDuration::from_secs_f64(radius);
            Ok(Selection::Window {
                from: at
                    .checked_sub(width)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                to: at
                    .checked_add(width)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                inclusive: false,
                session: Some(serde_json::json!({"mark":mark})),
            })
        }
        _ => Err(Error::Invalid("unknown retained recall selector".into())),
    }
}

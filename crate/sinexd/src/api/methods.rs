//! Executing requests (architecture §7.2): one function per command of the
//! table, shared by every surface. Reads use the read pool; writes go to the
//! writer as batches, so admission stays the only way anything is stored.
use super::mask::mask_json;
use crate::writer::{Priority, WriterHandle};
use serde_json::{Value, json};
use sinex_model::query::{self, Context, QueryError};
use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, OperationId, RunId};
use sinex_protocol::{DATA_NOTICE, Reply, Request};
use sinex_store::query::{Answer, Page};
use sqlx::PgPool;

/// The daemon's read side and its handle on the writer.
#[derive(Clone)]
pub struct Api {
    pub reads: PgPool,
    pub writer: Option<WriterHandle>,
    /// The content store, for excision's blanked bytes.
    pub blobs: sinex_store::Blobs,
    pub zone: jiff::tz::TimeZone,
    pub day_start: jiff::civil::Time,
}

fn refused(reason: impl Into<String>) -> Reply {
    Reply::Refused {
        reason: reason.into(),
        not_built: false,
    }
}

fn not_built(reason: impl Into<String>) -> Reply {
    Reply::Refused {
        reason: reason.into(),
        not_built: true,
    }
}

/// A result carrying captured text: masked, with the data notice first.
fn data(result: Value) -> Reply {
    let mut out = json!({ "notice": DATA_NOTICE });
    if let (Some(o), Value::Object(r)) = (out.as_object_mut(), mask_json(result)) {
        o.extend(r);
    }
    Reply::Ok { result: out }
}

impl Api {
    pub fn context(&self) -> Context {
        Context::new(jiff::Timestamp::now(), self.zone.clone(), self.day_start)
    }

    pub async fn call(&self, request: Request, actor: &str) -> Reply {
        match request {
            Request::Status => match self.status().await {
                Ok(v) => Reply::Ok { result: v },
                Err(e) => refused(e.to_string()),
            },
            Request::Query {
                query,
                page,
                cursor,
            } => self.query(&query, Page { size: page, cursor }).await,
            Request::Search {
                words,
                page,
                cursor,
            } => {
                self.query(
                    &format!("events where {words}"),
                    Page { size: page, cursor },
                )
                .await
            }
            Request::Recall {
                around,
                page,
                cursor,
            } => {
                let text = match around.as_deref().map(str::trim) {
                    Some(a) if !a.is_empty() => format!("recall {a}"),
                    _ => "recall".to_string(),
                };
                self.query(&text, Page { size: page, cursor }).await
            }
            Request::Trace {
                reference,
                page,
                cursor,
            } => {
                self.query(&format!("trace {reference}"), Page { size: page, cursor })
                    .await
            }
            Request::Sql { sql } => {
                let mut conn = match self.reads.acquire().await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match sinex_store::query::read_sql::run(&mut conn, &sql).await {
                    Ok(rows) => data(json!({ "total": rows.len(), "rows": rows })),
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::Show { reference } => {
                let r = match query::parse_reference(&reference) {
                    Ok(r) => r,
                    Err(e) => return query_refusal(&reference, e),
                };
                let mut conn = match self.reads.acquire().await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match sinex_store::query::show(&mut conn, &r).await {
                    Ok(v) => data(json!({ "event": v })),
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::Hide { reference } => {
                self.archive(&reference, actor, |target, operation| {
                    ArchiveRequest::Hide { target, operation }
                })
                .await
            }
            Request::Unhide { reference } => {
                self.archive(&reference, actor, |target, operation| {
                    ArchiveRequest::Unhide { target, operation }
                })
                .await
            }
            Request::Excise { reference } => self.excise(&reference, actor).await,
            Request::Follow { .. } => refused("follow streams; use the API's follow stream"),
            Request::Stage { .. } => not_built(
                "stage needs the staging path of the source framework, which this daemon does not run yet (architecture §5.4)",
            ),
            Request::Replay { .. } => not_built(
                "replay is not offered over the API yet; run `sinexd admin replay` (architecture §5.5)",
            ),
            Request::Recompute { .. } => not_built(
                "recompute is not offered over the API yet; run `sinexd admin recompute` (architecture §6.3)",
            ),
            Request::Same { .. } | Request::Distinct { .. } => not_built(
                "duplicate decisions need the fusion derivation's decision records, not built yet (architecture §6.4)",
            ),
            Request::Mark { .. } => not_built(
                "moment marks need the operator.mark event type, not registered yet (architecture §7.2)",
            ),
        }
    }

    async fn query(&self, text: &str, page: Page) -> Reply {
        let ctx = self.context();
        let bound = match query::bind(text, &ctx) {
            Ok(q) => q,
            Err(e) => return query_refusal(text, e),
        };
        let mut conn = match self.reads.acquire().await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        match sinex_store::query::run(&mut conn, text, &bound, &ctx, &page).await {
            Ok(answer) => data(answer_json(answer)),
            Err(e) => refused(e.to_string()),
        }
    }

    /// Hide, unhide or excise the event a citation names, through the writer.
    async fn archive(
        &self,
        reference: &str,
        actor: &str,
        request: impl FnOnce(EventId, OperationId) -> ArchiveRequest,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let r = match query::parse_reference(reference) {
            Ok(r) => r,
            Err(e) => return query_refusal(reference, e),
        };
        let id = {
            let mut conn = match self.reads.acquire().await {
                Ok(c) => c,
                Err(e) => return refused(e.to_string()),
            };
            match sinex_store::query::resolve(&mut conn, &r).await {
                Ok(id) => id,
                Err(e) => return refused(e.to_string()),
            }
        };
        let operation = OperationId::new();
        let batch = Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: actor.to_string(),
            },
            records: Vec::new(),
            archive: vec![request(EventId(id), operation)],
        };
        match writer.submit(Priority::Live, batch).await {
            Ok(_) => Reply::Ok {
                result: json!({ "event": id, "operation": operation }),
            },
            Err(e) => refused(e.to_string()),
        }
    }

    /// Excise the record a citation names, with its bytes (architecture §3.4).
    async fn excise(&self, reference: &str, actor: &str) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let r = match query::parse_reference(reference) {
            Ok(r) => r,
            Err(e) => return query_refusal(reference, e),
        };
        let mut conn = match self.reads.acquire().await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let id = match sinex_store::query::resolve(&mut conn, &r).await {
            Ok(id) => id,
            Err(e) => return refused(e.to_string()),
        };
        let target = sinex_store::excision::Target::Event(EventId(id));
        match crate::operate::excise_with(&mut conn, &self.blobs, writer, target, actor).await {
            Ok(summary) => Reply::Ok {
                result: json!({ "event": id, "excised": summary }),
            },
            Err(e) => refused(e),
        }
    }

    async fn status(&self) -> Result<Value, sqlx::Error> {
        let mut conn = self.reads.acquire().await?;
        let row = sqlx::query!(
            r#"SELECT
                 (SELECT value FROM core.counters WHERE name = 'commit_seq') AS "commit_seq!",
                 (SELECT commit_seq FROM core.consumer_positions WHERE consumer = $1) AS search_position,
                 (SELECT max(version) FROM sinex_schema.applied) AS schema_version"#,
            sinex_store::read_models::SEARCH_TEXT
        )
        .fetch_one(&mut *conn)
        .await?;
        drop(conn);
        let sources = match self
            .query(
                "sources sort binding",
                Page {
                    size: Some(1000),
                    cursor: None,
                },
            )
            .await
        {
            Reply::Ok { result } => result.get("rows").cloned().unwrap_or(Value::Null),
            Reply::Refused { reason, .. } => json!({ "unavailable": reason }),
        };
        Ok(json!({
            "commit_seq": row.commit_seq,
            "search_index": {
                "position": row.search_position,
                "behind": row.commit_seq - row.search_position.unwrap_or(0),
            },
            "schema_version": row.schema_version,
            "writer": self.writer.is_some(),
            "zone": self.context().zone_name(),
            "day_start": self.day_start.to_string(),
            "sources": sources,
        }))
    }
}

fn query_refusal(text: &str, e: QueryError) -> Reply {
    Reply::Refused {
        reason: e.render(text),
        not_built: e.not_built,
    }
}

fn answer_json(a: Answer) -> Value {
    serde_json::to_value(a).expect("answers serialize")
}

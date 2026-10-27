//! Executing requests: one function per command of the table, shared by every
//! surface. Reads use the read pool; writes go to the writer as batches, so
//! admission stays the only way anything is stored.
use super::mask::{mask_json, mask_str};
use crate::writer::{Priority, WriterHandle};
use serde_json::{Value, json};
use sinex_model::query::{self, Context, QueryError};
use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, OperationId, RunId};
use sinex_protocol::{Access, DATA_NOTICE, Reply, Request};
use sinex_store::query::{Answer, Page};
use sqlx::PgPool;

mod analysis;
mod forecast;
mod operations;
mod privacy;

/// The daemon's read side and its handle on the writer.
#[derive(Clone)]
pub(crate) struct Api {
    pub reads: PgPool,
    pub writer: Option<WriterHandle>,
    /// The content store.
    pub blobs: sinex_store::Blobs,
    pub zone: jiff::tz::TimeZone,
    pub day_start: jiff::civil::Time,
    /// Classes the agent token is granted besides `open` (`guarded` only by
    /// the operator's explicit grant; never `sealed`).
    pub agent_grants: Vec<sinex_model::Class>,
    /// The sealed class's unlock session, shared by every call.
    pub unlock: std::sync::Arc<tokio::sync::Mutex<Option<Unlocked>>>,
    /// This machine's name, for library requests and scans.
    pub host: String,
}

/// An unlock session: the operator's identity until it expires, and the data
/// keys it has unwrapped.
pub(crate) struct Unlocked {
    unlock: crate::sealed_access::Unlock,
    keys: std::collections::HashMap<String, age::secrecy::zeroize::Zeroizing<[u8; 32]>>,
}

impl Drop for Unlocked {
    fn drop(&mut self) {
        self.unlock.revoke();
    }
}

/// Who is calling: the actor recorded on writes and the classes whose values
/// they may read.
#[derive(Clone, Debug)]
pub(crate) struct Caller {
    pub actor: &'static str,
    pub grants: String,
}

type Conn = sqlx::pool::PoolConnection<sqlx::Postgres>;

fn refused(reason: impl Into<String>) -> Reply {
    Reply::Refused {
        reason: reason.into(),
        not_built: false,
    }
}

/// An RFC 3339 time argument.
fn timestamp(s: &str) -> Result<sinex_model::Timestamp, String> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map(sinex_model::Timestamp)
        .map_err(|e| format!("{s}: not an RFC 3339 time: {e}"))
}

fn not_built(reason: impl Into<String>) -> Reply {
    Reply::Refused {
        reason: reason.into(),
        not_built: true,
    }
}

/// A result carrying captured text, with the data notice first. Masking
/// happens once, in `call`, for every result.
fn data(result: Value) -> Reply {
    let mut out = json!({ "notice": DATA_NOTICE });
    if let (Some(o), Value::Object(r)) = (out.as_object_mut(), result) {
        o.extend(r);
    }
    Reply::Ok { result: out }
}

fn collect_sealed(value: &Value, out: &mut Vec<String>) {
    if let Some(r) = sinex_store::sealed::key_ref(value) {
        out.push(r.to_string());
        return;
    }
    match value {
        Value::String(text) => {
            if let Some(value) = sinex_store::sealed::sealed_text(text) {
                collect_sealed(&value, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|v| collect_sealed(v, out)),
        Value::Object(map) => map.values().for_each(|v| collect_sealed(v, out)),
        _ => {}
    }
}

fn unseal_in(
    value: &mut Value,
    keys: &std::collections::HashMap<String, age::secrecy::zeroize::Zeroizing<[u8; 32]>>,
) {
    if let Some(r) = sinex_store::sealed::key_ref(value) {
        if let Some(key) = keys.get(r)
            && let Ok(plain) = sinex_store::sealed::unseal_payload(value, key)
        {
            *value = plain;
        }
        return;
    }
    match value {
        Value::String(text) => {
            if let Some(mut sealed) = sinex_store::sealed::sealed_text(text) {
                unseal_in(&mut sealed, keys);
                if !sinex_store::sealed::is_sealed(&sealed) {
                    *value = sealed;
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|v| unseal_in(v, keys)),
        Value::Object(map) => map.values_mut().for_each(|v| unseal_in(v, keys)),
        _ => {}
    }
}

impl Api {
    /// The caller for a token's access. The operator token holds `guarded`;
    /// the agent token what the operator granted it; the read token nothing.
    pub(crate) async fn caller(&self, access: Access) -> Caller {
        let (actor, grants): (&'static str, Vec<&str>) = match access {
            Access::Read => ("reader", vec![]),
            Access::Agent => (
                "agent",
                self.agent_grants
                    .iter()
                    .filter(|c| **c == sinex_model::Class::Guarded)
                    .map(|_| "guarded")
                    .collect(),
            ),
            Access::Operator => {
                let mut session = self.unlock.lock().await;
                if session.as_ref().is_some_and(|s| s.unlock.check().is_err()) {
                    *session = None;
                }
                if session.is_some() {
                    ("operator", vec!["guarded", "sealed"])
                } else {
                    ("operator", vec!["guarded"])
                }
            }
        };
        Caller {
            actor,
            grants: grants.join(","),
        }
    }

    /// A read connection that sees only what `caller` may (the visibility
    /// views read `sinex.grants`).
    pub(crate) async fn acquire(&self, caller: &Caller) -> Result<Conn, sqlx::Error> {
        let mut conn = self.reads.acquire().await?;
        sinex_store::query::set_grants(&mut conn, &caller.grants).await?;
        Ok(conn)
    }

    pub(crate) fn context(&self) -> Context {
        Context::new(jiff::Timestamp::now(), self.zone.clone(), self.day_start)
    }

    pub(crate) async fn call(&self, request: Request, access: Access) -> Reply {
        let caller = self.caller(access).await;
        let reply = self.dispatch(request, &caller).await;
        // Sealed values are unsealed for a caller holding the class, then
        // every string is masked on its way out (unsealing comes first, so the
        // masker never sees ciphertext).
        match reply {
            Reply::Ok { mut result } => {
                if caller.grants.split(',').any(|g| g == "sealed")
                    && let Err(e) = self.unseal(&mut result).await
                {
                    return refused(mask_str(&e));
                }
                match mask_json(result) {
                    Ok(result) => Reply::Ok { result },
                    Err(reason) => refused(reason),
                }
            }
            Reply::Refused { reason, not_built } => Reply::Refused {
                reason: mask_str(&reason),
                not_built,
            },
        }
    }

    /// Replace every sealed payload in `value` with its plaintext, unwrapping
    /// each data key once per session.
    async fn unseal(&self, value: &mut Value) -> Result<(), String> {
        let mut refs = Vec::new();
        collect_sealed(value, &mut refs);
        if refs.is_empty() {
            return Ok(());
        }
        let mut session = self.unlock.lock().await;
        let Some(s) = session.as_mut() else {
            return Ok(());
        };
        s.unlock.check()?;
        let mut conn = self.reads.acquire().await.map_err(|e| e.to_string())?;
        for r in refs {
            if s.keys.contains_key(&r) {
                continue;
            }
            if let Some(wrapped) = sinex_store::sealed::wrapped_key(&mut conn, &r)
                .await
                .map_err(|e| e.to_string())?
            {
                let key = s.unlock.unwrap(&wrapped)?;
                s.keys.insert(r, age::secrecy::zeroize::Zeroizing::new(key));
            }
        }
        s.unlock.check()?;
        unseal_in(value, &s.keys);
        Ok(())
    }

    async fn dispatch(&self, request: Request, caller: &Caller) -> Reply {
        let actor = caller.actor;
        match request {
            Request::Status => match self.status(caller).await {
                Ok(v) => Reply::Ok { result: v },
                Err(e) => refused(e.to_string()),
            },
            Request::Query {
                query,
                page,
                cursor,
            } => {
                self.query(caller, &query, Page { size: page, cursor })
                    .await
            }
            Request::Search {
                words,
                page,
                cursor,
            } => {
                self.query(
                    caller,
                    &format!("events where {words}"),
                    Page { size: page, cursor },
                )
                .await
            }
            Request::Recall {
                around,
                cwd,
                page,
                cursor,
            } => {
                let mut text = match around.as_deref().map(str::trim) {
                    Some(a) if !a.is_empty() => format!("recall {a}"),
                    _ => "recall".to_string(),
                };
                if let Some(cwd) = cwd {
                    if around.as_deref().map(str::trim) != Some("last") {
                        return refused(
                            "cwd applies only to recall last; use a session-field query for other filters",
                        );
                    }
                    text.push_str(" where cwds ^ ");
                    text.push_str(&serde_json::to_string(&cwd).expect("strings serialize"));
                }
                self.query(caller, &text, Page { size: page, cursor }).await
            }
            Request::Trace {
                reference,
                page,
                cursor,
            } => {
                self.query(
                    caller,
                    &format!("trace {reference}"),
                    Page { size: page, cursor },
                )
                .await
            }
            Request::Sql { sql } => {
                let mut conn = match self.acquire(caller).await {
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
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match sinex_store::query::show(&mut conn, &r).await {
                    Ok(v) => data(json!({ "event": v })),
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::Hide { reference } => {
                self.archive(caller, &reference, actor, |target, operation| {
                    ArchiveRequest::Hide { target, operation }
                })
                .await
            }
            Request::Unhide { reference } => {
                self.archive(caller, &reference, actor, |target, operation| {
                    ArchiveRequest::Unhide { target, operation }
                })
                .await
            }
            Request::Excise {
                reference,
                material,
                dry_run,
            } => {
                self.excise(
                    caller,
                    reference.as_deref(),
                    material.as_deref(),
                    dry_run.unwrap_or(false),
                    actor,
                )
                .await
            }
            Request::Replay {
                sealed,
                material,
                binding,
                event_type,
                from,
                until,
                compare,
                resume,
                all,
            } => {
                let scope = (|| -> Result<crate::replay::Scope, String> {
                    Ok(crate::replay::Scope {
                        materials: material
                            .iter()
                            .map(|m| {
                                uuid::Uuid::parse_str(m).map_err(|e| format!("material {m}: {e}"))
                            })
                            .collect::<Result<_, _>>()?,
                        binding,
                        event_types: (!event_type.is_empty()).then_some(event_type),
                        from: from.as_deref().map(timestamp).transpose()?,
                        until: until.as_deref().map(timestamp).transpose()?,
                    })
                })();
                let resume = resume
                    .as_deref()
                    .map(|r| uuid::Uuid::parse_str(r).map(OperationId))
                    .transpose();
                match (scope, resume) {
                    (Ok(scope), Ok(resume)) => {
                        self.replay(
                            caller,
                            crate::replay::Options {
                                scope,
                                compare: compare.unwrap_or(false),
                                resume,
                                actor: actor.to_string(),
                                unlock: None,
                            },
                            all.unwrap_or(false),
                            sealed.unwrap_or(false),
                        )
                        .await
                    }
                    (Err(e), _) => refused(e),
                    (_, Err(e)) => refused(format!("resume: {e}")),
                }
            }
            Request::Recompute {
                derivation,
                compare,
                from,
                until,
                examples,
            } => {
                let range = match (
                    from.as_deref().map(timestamp).transpose(),
                    until.as_deref().map(timestamp).transpose(),
                ) {
                    (Ok(None), Ok(None)) => None,
                    (Ok(f), Ok(u)) => Some((
                        f.unwrap_or(sinex_model::Timestamp(time::OffsetDateTime::UNIX_EPOCH)),
                        u.unwrap_or_else(sinex_model::Timestamp::now),
                    )),
                    (Err(e), _) | (_, Err(e)) => return refused(e),
                };
                self.recompute(
                    caller,
                    &derivation,
                    compare.unwrap_or(false),
                    range,
                    usize::try_from(examples.unwrap_or(5)).unwrap_or(usize::MAX),
                )
                .await
            }
            Request::Comparisons { derivation, limit } => {
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                let limit = i64::try_from(limit.unwrap_or(20)).unwrap_or(i64::MAX);
                match sinex_store::derivations::comparisons(&mut conn, derivation.as_deref(), limit)
                    .await
                {
                    Ok(kept) => {
                        let rows: Vec<Value> = kept
                            .into_iter()
                            .map(|c| {
                                json!({
                                    "id": c.id,
                                    "derivation": c.derivation,
                                    "version": c.version,
                                    "from": c.range.and_then(|r| r.0),
                                    "until": c.range.and_then(|r| r.1),
                                    "made_at": c.made_at,
                                    "keys": c.keys,
                                    "unchanged": c.unchanged,
                                    "added": c.added,
                                    "changed": c.changed,
                                    "removed": c.removed,
                                    "examples": c.examples,
                                })
                            })
                            .collect();
                        Reply::Ok {
                            result: json!({ "total": rows.len(), "comparisons": rows }),
                        }
                    }
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::Crawl {
                allow,
                url,
                limit,
                pause,
            } => {
                self.crawl(caller, allow, url, limit, pause.unwrap_or(2))
                    .await
            }
            r @ (Request::ForecastIssue { .. }
            | Request::ForecastResolve { .. }
            | Request::ForecastReport) => match self.forecast(r, caller).await {
                Ok(v) => data(v),
                Err(e) => refused(e),
            },
            Request::AnalysisCatalog
            | Request::AnalysisRun { .. }
            | Request::AnalysisPanel { .. }
            | Request::Gaps { .. }
            | Request::AnalysisWhy { .. }
            | Request::Brief { .. } => self.analysis(caller, request).await,
            Request::Follow { .. } => refused("follow streams; use the API's follow stream"),
            Request::Stage { .. } => not_built(
                "stage needs the staging path of the source framework, which this daemon does not run yet",
            ),
            Request::Same { .. } | Request::Distinct { .. } => not_built(
                "duplicate decisions need the fusion derivation's decision records, not built yet",
            ),
            Request::Mark { .. } => {
                not_built("moment marks need the operator.mark event type, not registered yet")
            }
            r @ (Request::PrivacyRules { .. }
            | Request::PrivacyRule { .. }
            | Request::PrivacyEnd { .. }
            | Request::PrivacyClass { .. }
            | Request::Unlock { .. }
            | Request::Lock
            | Request::PrivacyVerify) => self.privacy(r, caller).await,
            r @ (Request::LibraryAdd { .. }
            | Request::LibraryItem { .. }
            | Request::LibraryTag { .. }
            | Request::LibraryUntag { .. }
            | Request::LibrarySet { .. }
            | Request::LibraryCollect { .. }
            | Request::LibraryUncollect { .. }
            | Request::LibraryCollection { .. }
            | Request::LibraryRate { .. }
            | Request::LibraryCompare { .. }
            | Request::LibraryRank { .. }
            | Request::LibraryOpen { .. }
            | Request::LibraryViewed { .. }
            | Request::LibraryPreview { .. }
            | Request::LibraryImport { .. }) => match self.library(r, caller).await {
                Ok(v) => data(v),
                Err(e) => refused(e),
            },
        }
    }

    /// The media library's commands (`crate::library`). Reads see only what
    /// the caller may, through the same visibility as every other read.
    async fn library(&self, request: Request, caller: &Caller) -> Result<Value, String> {
        let mut conn = self.acquire(caller).await.map_err(|e| e.to_string())?;
        crate::library::dispatch(
            &mut conn,
            self.writer.as_ref(),
            &self.blobs,
            &self.host,
            request,
            caller.actor,
        )
        .await
    }

    async fn query(&self, caller: &Caller, text: &str, page: Page) -> Reply {
        let ctx = match sinex_store::query::context_for_page(&self.context(), &page) {
            Ok(ctx) => ctx,
            Err(e) => return refused(e.to_string()),
        };
        let bound = match query::bind(text, &ctx) {
            Ok(q) => q,
            Err(e) => return query_refusal(text, e),
        };
        let mut conn = match self.acquire(caller).await {
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
        caller: &Caller,
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
            let mut conn = match self.acquire(caller).await {
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

    async fn status(&self, caller: &Caller) -> Result<Value, String> {
        let mut conn = self.acquire(caller).await.map_err(|e| e.to_string())?;
        let row = sinex_store::diagnostics::positions(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        let telemetry = sinex_store::telemetry::latest(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        drop(conn);
        let sources = match self
            .query(
                caller,
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
        // The daemon's own switch, which pauses the binding runner.
        let private = sinex_transport::private_mode::PrivateMode::default();
        let switch = private.state();
        Ok(json!({
            "commit_seq": row.commit_seq,
            "private_mode": {
                "state": switch.name(),
                "switch": private.path(),
                "reason": switch.reason(),
            },
            "search_index": {
                "position": row.search_position,
                "behind": row.commit_seq - row.search_position.unwrap_or(0),
            },
            "schema_version": row.schema_version,
            "writer": self.writer.is_some(),
            "zone": self.context().zone_name(),
            "day_start": self.day_start.to_string(),
            "sources": sources,
            // The daemon's newest sample of each of its parts.
            "telemetry": telemetry,
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

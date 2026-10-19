//! Executing requests (architecture §7.2): one function per command of the
//! table, shared by every surface. Reads use the read pool; writes go to the
//! writer as batches, so admission stays the only way anything is stored.
use super::mask::mask_json;
use crate::writer::{Priority, WriterHandle};
use serde_json::{Value, json};
use sinex_model::query::{self, Context, QueryError};
use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, OperationId, RunId};
use sinex_protocol::{Access, DATA_NOTICE, Reply, Request};
use sinex_store::privacy;
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
    /// Classes the agent token is granted besides `open` (`guarded` only by
    /// the operator's explicit grant; never `sealed`).
    pub agent_grants: Vec<sinex_model::Class>,
    /// The sealed class's unlock session, shared by every call.
    pub unlock: std::sync::Arc<tokio::sync::Mutex<Option<Unlocked>>>,
    /// This machine's name, for library requests and scans.
    pub host: String,
}

/// An unlock session (spec `privacy.md` §4): the operator's identity until it
/// expires, and the data keys it has unwrapped.
pub struct Unlocked {
    identity: sinex_store::sealed::Identity,
    until: std::time::Instant,
    keys: std::collections::HashMap<String, [u8; 32]>,
}

/// Who is calling: the actor recorded on writes and the classes whose values
/// they may read.
#[derive(Clone, Debug)]
pub struct Caller {
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
        Value::Array(items) => items.iter().for_each(|v| collect_sealed(v, out)),
        Value::Object(map) => map.values().for_each(|v| collect_sealed(v, out)),
        _ => {}
    }
}

fn unseal_in(value: &mut Value, keys: &std::collections::HashMap<String, [u8; 32]>) {
    if let Some(r) = sinex_store::sealed::key_ref(value) {
        if let Some(key) = keys.get(r)
            && let Ok(plain) = sinex_store::sealed::unseal_payload(value, key)
        {
            *value = plain;
        }
        return;
    }
    match value {
        Value::Array(items) => items.iter_mut().for_each(|v| unseal_in(v, keys)),
        Value::Object(map) => map.values_mut().for_each(|v| unseal_in(v, keys)),
        _ => {}
    }
}

impl Api {
    /// The caller for a token's access. The operator token holds `guarded`;
    /// the agent token what the operator granted it; the read token nothing.
    pub async fn caller(&self, access: Access) -> Caller {
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
                if session
                    .as_ref()
                    .is_some_and(|s| s.until <= std::time::Instant::now())
                {
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
    pub async fn acquire(&self, caller: &Caller) -> Result<Conn, sqlx::Error> {
        let mut conn = self.reads.acquire().await?;
        sqlx::query_scalar!(
            "SELECT set_config('sinex.grants', $1, false)",
            caller.grants
        )
        .fetch_one(&mut *conn)
        .await?;
        Ok(conn)
    }

    pub fn context(&self) -> Context {
        Context::new(jiff::Timestamp::now(), self.zone.clone(), self.day_start)
    }

    pub async fn call(&self, request: Request, access: Access) -> Reply {
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
                    return refused(e);
                }
                Reply::Ok {
                    result: mask_json(result),
                }
            }
            refusal => refusal,
        }
    }

    /// Replace every sealed payload in `value` with its plaintext, unwrapping
    /// each data key once per session. An excised key leaves the value sealed.
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
        let mut conn = self.reads.acquire().await.map_err(|e| e.to_string())?;
        for r in refs {
            if s.keys.contains_key(&r) {
                continue;
            }
            if let Some(wrapped) = sinex_store::sealed::wrapped_key(&mut conn, &r)
                .await
                .map_err(|e| e.to_string())?
            {
                let key = sinex_store::sealed::unwrap(&wrapped, &s.identity)
                    .map_err(|e| e.to_string())?;
                s.keys.insert(r, key);
            }
        }
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
                page,
                cursor,
            } => {
                let text = match around.as_deref().map(str::trim) {
                    Some(a) if !a.is_empty() => format!("recall {a}"),
                    _ => "recall".to_string(),
                };
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
                            },
                            all.unwrap_or(false),
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
                match sinex_store::derived::comparisons(&mut conn, derivation.as_deref(), limit)
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
            Request::AnalysisCatalog
            | Request::AnalysisRun { .. }
            | Request::AnalysisPanel { .. }
            | Request::AnalysisWhy { .. }
            | Request::AnalysisResults { .. }
            | Request::AnalysisShow { .. }
            | Request::Brief { .. } => self.analysis(caller, request).await,
            Request::Follow { .. } => refused("follow streams; use the API's follow stream"),
            Request::Stage { .. } => not_built(
                "stage needs the staging path of the source framework, which this daemon does not run yet (architecture §5.4)",
            ),
            Request::Same { .. } | Request::Distinct { .. } => not_built(
                "duplicate decisions need the fusion derivation's decision records, not built yet (architecture §6.4)",
            ),
            Request::Mark { .. } => not_built(
                "moment marks need the operator.mark event type, not registered yet (architecture §7.2)",
            ),
            Request::PrivacyRules { history } => {
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::list_rules(&mut conn, history.unwrap_or(false)).await {
                    Ok(rows) => Reply::Ok {
                        result: json!({ "total": rows.len(), "rules": rows }),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::PrivacyRule {
                name,
                source,
                event_type,
                field,
                matcher,
                pattern,
                class,
                action,
            } => {
                let class = match privacy::parse_class(class.as_deref().unwrap_or("open")) {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                let action = match privacy::Action::parse(action.as_deref().unwrap_or("mask")) {
                    Ok(a) => a,
                    Err(e) => return refused(e.to_string()),
                };
                let rule = privacy::NewRule {
                    name,
                    source,
                    event_type,
                    field,
                    matcher: matcher.unwrap_or_else(|| "all".into()),
                    matcher_arg: pattern,
                    class,
                    action,
                    author: actor.to_string(),
                };
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::add_rule(&mut conn, &rule).await {
                    Ok(seq) => Reply::Ok {
                        result: json!({ "rule": rule.name, "seq": seq,
                            "applies": "to new readings; replay the source to apply it to stored readings" }),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::PrivacyEnd { name } => {
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::end_rule(&mut conn, &name, actor).await {
                    Ok(seq) => Reply::Ok {
                        result: json!({ "rule": name, "ended": true, "seq": seq }),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::PrivacyClass { source, class } => {
                let class = match privacy::parse_class(&class) {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                if actor != "operator" {
                    let current =
                        match privacy::load(&mut conn, std::iter::once(source.as_str())).await {
                            Ok(set) => set.floors.get(&source).copied().unwrap_or_default(),
                            Err(e) => return refused(e.to_string()),
                        };
                    if class < current {
                        return refused(format!(
                            "{source}'s floor is {}; lowering it weakens protection, which only the operator token may do",
                            privacy::class_name(current)
                        ));
                    }
                }
                match privacy::set_source_class(&mut conn, &source, class, actor).await {
                    Ok(seq) => Reply::Ok {
                        result: json!({ "source": source, "class": privacy::class_name(class), "seq": seq,
                            "applies": "to new readings; replay the source to raise stored readings" }),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
            Request::Unlock { identity, minutes } => {
                let identity = match sinex_store::sealed::parse_identity(&identity) {
                    Ok(i) => i,
                    Err(e) => return refused(e.to_string()),
                };
                let minutes = minutes.unwrap_or(480);
                *self.unlock.lock().await = Some(Unlocked {
                    identity,
                    until: std::time::Instant::now() + std::time::Duration::from_secs(minutes * 60),
                    keys: Default::default(),
                });
                Reply::Ok {
                    result: json!({ "unlocked": "sealed", "minutes": minutes }),
                }
            }
            Request::Lock => {
                let was = self.unlock.lock().await.take().is_some();
                Reply::Ok {
                    result: json!({ "locked": "sealed", "was_unlocked": was }),
                }
            }
            Request::PrivacyVerify => {
                let mut conn = match self.acquire(caller).await {
                    Ok(c) => c,
                    Err(e) => return refused(e.to_string()),
                };
                match privacy::verify_classes(&mut conn).await {
                    Ok(v) => Reply::Ok {
                        result: serde_json::to_value(v).expect("verification serializes"),
                    },
                    Err(e) => refused(e.to_string()),
                }
            }
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
        let ctx = self.context();
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

    /// The analysis calls. Results carry their status, as-of position, caveats
    /// and citations; findings are rendered from their slots, so every surface
    /// reads the same text.
    async fn analysis(&self, caller: &Caller, request: Request) -> Reply {
        use crate::analysis::{self, Recipe};
        if let Request::AnalysisCatalog = request {
            let metrics: Vec<Value> = sinex_analysis::catalog::METRICS
                .iter()
                .map(|m| {
                    json!({"id": m.id, "version": m.version, "description": m.description,
                           "relation": m.relation, "sources": m.coverage.sources(),
                           "dimensions": m.dimensions, "role": m.role})
                })
                .collect();
            let methods: Vec<Value> = sinex_analysis::methods::catalog()
                .into_iter()
                .map(|(id, version, summary)| json!({"id": id, "version": version, "summary": summary}))
                .collect();
            return data(json!({
                "metrics": metrics, "methods": methods,
                "recipes": ["correlations", "trends", "deviations"],
            }));
        }
        let settings = match analysis::Settings::from_api(&self.zone, self.day_start) {
            Ok(s) => s,
            Err(e) => return refused(e),
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let outcome: Result<Value, String> = async {
            match request {
                Request::AnalysisRun {
                    recipe,
                    from,
                    to,
                    standing,
                    partial,
                } => {
                    let recipe = Recipe::parse(&recipe)?;
                    let min_observed_fraction = partial
                        .map(|p| {
                            p.parse::<f64>()
                                .ok()
                                .filter(|f| (0.0..=1.0).contains(f))
                                .ok_or_else(|| format!("`{p}` is not a fraction from 0 to 1"))
                        })
                        .transpose()?;
                    let eligibility = sinex_analysis::Eligibility {
                        min_observed_fraction,
                        admit_recorded: false,
                    };
                    let run = analysis::run(
                        &mut conn,
                        &settings,
                        recipe,
                        analysis::parse_day(&from)?,
                        analysis::parse_day(&to)?,
                        eligibility,
                        standing.unwrap_or(false),
                    )
                    .await?;
                    Ok(json!({
                        "result": run.result.id, "state": run.result.state, "reused": run.reused,
                        "tables": run.tables,
                        "findings": run.findings.iter().map(|f| json!({"text": f.markdown(), "finding": f})).collect::<Vec<_>>(),
                    }))
                }
                Request::AnalysisPanel {
                    from,
                    to,
                    metric,
                    by,
                } => {
                    let defs = metric_defs(&metric.unwrap_or_default())?;
                    let (from, to) = (analysis::parse_day(&from)?, analysis::parse_day(&to)?);
                    let as_of = sinex_store::analysis::head(&mut conn)
                        .await
                        .map_err(|e| e.to_string())?;
                    let mut sources: Vec<&str> = defs
                        .iter()
                        .flat_map(|d| d.coverage.sources().iter().copied())
                        .collect();
                    sources.sort_unstable();
                    sources.dedup();
                    let coverage = analysis::coverage(&mut conn, &sources, as_of).await?;
                    let range = settings.day_span(from, to)?;
                    let p = analysis::panel(&mut conn, &settings, &defs, range, as_of, &coverage)
                        .await?;
                    Ok(json!({
                        "text": analysis::table(&p, &defs, by.as_deref()),
                        "as_of": as_of, "day_setting": settings.label,
                        "coverage": coverage, "cells": p.cells,
                    }))
                }
                Request::AnalysisWhy { metric, day } => {
                    let def = metric_defs(&[metric])?[0];
                    let text =
                        analysis::why_not(&mut conn, &settings, def, analysis::parse_day(&day)?)
                            .await?;
                    Ok(json!({ "text": text }))
                }
                Request::AnalysisResults { limit } => {
                    let limit = limit.map_or(20, |l| i64::try_from(l).unwrap_or(i64::MAX));
                    let rows = sinex_store::analysis::recent_results(&mut conn, None, limit)
                        .await
                        .map_err(|e| e.to_string())?;
                    let results: Vec<Value> = rows
                        .into_iter()
                        .map(|(id, recipe, status, state, at)| {
                            json!({"result": id, "recipe": recipe, "status": status,
                                   "state": state,
                                   "created_at": at.format(&time::format_description::well_known::Rfc3339).unwrap_or_default()})
                        })
                        .collect();
                    Ok(json!({ "results": results }))
                }
                Request::AnalysisShow { result } => {
                    let id: uuid::Uuid = result
                        .parse()
                        .map_err(|e| format!("`{result}` is not a result ID: {e}"))?;
                    let view = sinex_store::analysis::result(&mut conn, id)
                        .await
                        .map_err(|e| e.to_string())?
                        .ok_or_else(|| format!("no result {id}"))?;
                    let findings: Vec<Value> = view
                        .findings
                        .iter()
                        .filter_map(analysis::stored_finding)
                        .map(|f| json!({"text": f.markdown(), "finding": f}))
                        .collect();
                    Ok(json!({
                        "result": view.id, "recipe": view.recipe, "as_of": view.as_of,
                        "status": view.status, "reason": view.reason, "state": view.state,
                        "state_reason": view.state_reason, "inputs_read": view.inputs_read,
                        "tables": view.tables, "findings": findings,
                    }))
                }
                Request::Brief { day, budget } => {
                    let day = match day {
                        Some(d) => analysis::parse_day(&d)?,
                        None => settings
                            .civil
                            .day(sinex_model::Timestamp::now())
                            .map_err(|e| e.to_string())?,
                    };
                    let budget = budget.map_or(4000, |b| usize::try_from(b).unwrap_or(usize::MAX));
                    let p = analysis::brief(&mut conn, &settings, day, budget).await?;
                    Ok(json!({"text": p.markdown, "presentation": p}))
                }
                _ => Err("not an analysis call".into()),
            }
        }
        .await;
        match outcome {
            Ok(v) => data(v),
            Err(e) => refused(e),
        }
    }

    /// Excise the record a citation names, with its bytes.
    async fn excise(
        &self,
        caller: &Caller,
        reference: Option<&str>,
        material: Option<&str>,
        dry_run: bool,
        actor: &str,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let target = match (reference, material) {
            (Some(reference), None) => {
                let r = match query::parse_reference(reference) {
                    Ok(r) => r,
                    Err(e) => return query_refusal(reference, e),
                };
                match sinex_store::query::resolve(&mut conn, &r).await {
                    Ok(id) => sinex_store::excision::Target::Event(EventId(id)),
                    Err(e) => return refused(e.to_string()),
                }
            }
            (None, Some(m)) => match uuid::Uuid::parse_str(m) {
                Ok(m) => sinex_store::excision::Target::Material(sinex_model::MaterialId(m)),
                Err(e) => return refused(format!("material {m}: {e}")),
            },
            _ => return refused("excise needs exactly one of an event reference or --material"),
        };
        if dry_run {
            return match sinex_store::excision::plan(&mut conn, target).await {
                Ok(plan) => Reply::Ok {
                    result: json!({ "dry_run": true, "would_excise": crate::operate::describe(&plan) }),
                },
                Err(e) => refused(e.to_string()),
            };
        }
        match crate::operate::excise_with(&mut conn, &self.blobs, writer, target, actor).await {
            Ok(summary) => Reply::Ok {
                result: json!({ "excised": summary }),
            },
            Err(e) => refused(e),
        }
    }

    async fn replay(&self, caller: &Caller, o: crate::replay::Options, all: bool) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let compare = o.compare;
        match crate::replay::run(&mut conn, &self.blobs, writer, o).await {
            Ok(report) => Reply::Ok {
                result: report.to_json(compare, all),
            },
            Err(e) => refused(e),
        }
    }

    async fn recompute(
        &self,
        caller: &Caller,
        derivation: &str,
        compare: bool,
        range: Option<(sinex_model::Timestamp, sinex_model::Timestamp)>,
        examples: usize,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let conn = match self.acquire(caller).await {
            Ok(c) => c.detach(),
            Err(e) => return refused(e.to_string()),
        };
        let mut driver =
            match crate::derive::Driver::operating(conn, writer.clone(), derivation, &self.host)
                .await
            {
                Ok(d) => d,
                Err(e) => return refused(e),
            };
        let result = if compare {
            driver
                .compare(derivation, range, examples)
                .await
                .map(|cmp| json!({ "comparison": cmp.id, "summary": cmp.to_string() }))
        } else {
            driver
                .recompute_all(derivation, range)
                .await
                .map(|n| json!({ "derivation": derivation, "keys_marked": n }))
        };
        let ended = driver.end_runs().await;
        match (result, ended) {
            (Ok(result), Ok(())) => Reply::Ok { result },
            (Err(e), _) | (_, Err(e)) => refused(e),
        }
    }

    async fn crawl(
        &self,
        caller: &Caller,
        allow: Vec<String>,
        urls: Vec<String>,
        limit: Option<u64>,
        pause: u64,
    ) -> Reply {
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        match crate::crawl::run(
            &mut conn,
            &self.blobs,
            writer,
            &self.host,
            allow,
            urls,
            limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
            std::time::Duration::from_secs(pause),
        )
        .await
        {
            Ok(totals) => Reply::Ok {
                result: json!({ "crawl": totals.to_string() }),
            },
            Err(e) => refused(e),
        }
    }

    async fn status(&self, caller: &Caller) -> Result<Value, sqlx::Error> {
        let mut conn = self.acquire(caller).await?;
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

/// The metrics named by `ids`, or every shipped metric when none is named.
fn metric_defs(ids: &[String]) -> Result<Vec<&'static sinex_analysis::MetricDef>, String> {
    if ids.is_empty() {
        return Ok(sinex_analysis::catalog::METRICS.iter().collect());
    }
    ids.iter()
        .map(|id| {
            sinex_analysis::catalog::metric(id).ok_or_else(|| {
                let known: Vec<&str> = sinex_analysis::catalog::METRICS
                    .iter()
                    .map(|m| m.id)
                    .collect();
                format!("no metric {id}; known: {}", known.join(", "))
            })
        })
        .collect()
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

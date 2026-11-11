//! Executing requests: one function per command of the table, shared by every
//! surface. Reads use the read pool; writes go to the writer as batches, so
//! admission stays the only way anything is stored.
use crate::writer::WriterHandle;
use crate::{Error, Result};
use serde_json::{Value, json};
use sinex_model::event_types::telemetry::SinexTelemetry;
use sinex_model::lifecycle::Act;
use sinex_model::query::{self, Context, QueryError, Source};
use sinex_model::{EventId, EventPayload, OperationId, RunId};
use sinex_protocol::{Access, DATA_NOTICE, Reply, Request};
use sinex_store::query::{Answer, Page};
use sqlx::PgPool;

mod analysis;
mod backup;
mod forecast;
mod fusion;
mod kinds;
mod operations;
mod privacy;
mod queries;
mod recall;
mod signals;
mod sql;

pub(crate) use sql::SqlCursors;

/// The daemon's read side and its handle on the writer.
#[derive(Clone)]
pub(crate) struct Api {
    pub reads: PgPool,
    pub writer: Option<WriterHandle>,
    pub backup_producers: Option<(async_nats::Client, sinex_protocol::Subjects, Vec<String>)>,
    /// The content store.
    pub blobs: sinex_store::Blobs,
    pub zone: jiff::tz::TimeZone,
    pub day_start: jiff::civil::Time,
    pub derivations: sinex_derivations::Configuration,
    /// Classes the agent token is granted besides `open` (`guarded` only by
    /// the operator's explicit grant; never `sealed`).
    pub agent_grants: Vec<sinex_model::Class>,
    /// The sealed class's unlock session, shared by every call.
    pub unlock: std::sync::Arc<tokio::sync::Mutex<Option<Unlocked>>>,
    /// This machine's name, for library requests and scans.
    pub host: String,
    pub source_config: std::sync::Arc<crate::binding_runner::Config>,
    /// The bindings whose latest read attempt failed, kept by the binding runner.
    pub read_failures: crate::binding_runner::ReadFailures,
    /// The stages this daemon is reading now, so one is never read twice
    /// at once.
    pub running_stages: crate::stage::Running,
    /// The daemon's shutdown. A stage reading in the background stops at it,
    /// ends `stopped_early`, and `resume` reads it again later.
    pub stop: tokio::sync::watch::Receiver<bool>,
    pub library_roots: Vec<std::path::PathBuf>,
    /// Read-only SQL statements with pages still to read.
    pub sql_cursors: SqlCursors,
}

/// An unlock session: the operator's identity until it expires, and the data
/// keys it has unwrapped.
pub(crate) struct Unlocked {
    unlock: crate::material_access::sealing::Unlock,
    keys: std::collections::HashMap<String, sinex_sealing::Zeroizing<[u8; 32]>>,
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

/// An RFC 3339 time argument.
fn timestamp(s: &str) -> Result<sinex_model::Timestamp> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map(sinex_model::Timestamp)
        .map_err(|e| Error::bad_request(format!("{s}: not an RFC 3339 time: {e}")))
}

/// A result carrying captured text, with the data notice first.
fn data(result: Value) -> Reply {
    let mut out = json!({ "notice": DATA_NOTICE });
    if let (Some(o), Value::Object(r)) = (out.as_object_mut(), result) {
        o.extend(r);
    }
    Reply::Ok { result: out }
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

    pub(crate) fn context_at(&self, at: sinex_model::Timestamp) -> Result<Context> {
        let now = jiff::Timestamp::from_nanosecond(at.unix_ns())?;
        Ok(Context::new(now, self.zone.clone(), self.day_start))
    }

    pub(crate) fn context(&self) -> Context {
        Context::new(jiff::Timestamp::now(), self.zone.clone(), self.day_start)
    }

    pub(crate) async fn call(&self, request: Request, access: Access) -> Reply {
        let caller = self.caller(access).await;
        match self.answer(request, &caller).await {
            Ok(reply) => reply,
            Err(error) => Reply::failed(error),
        }
    }

    async fn answer(&self, request: Request, caller: &Caller) -> Result<Reply> {
        let reply = self.dispatch(request, caller).await?;
        // Sealed values are unsealed only for a caller holding the class.
        match reply {
            Reply::Ok { mut result } => {
                if caller.grants.split(',').any(|g| g == "sealed") {
                    self.unseal(&mut result).await?;
                }
                Ok(Reply::Ok { result })
            }
            other => Ok(other),
        }
    }

    /// Replace every sealed payload in `value` with its plaintext, unwrapping
    /// each data key once per session.
    async fn unseal(&self, value: &mut Value) -> Result<()> {
        let mut refs = Vec::new();
        crate::material_access::sealing::collect_sealed(value, &mut refs);
        if refs.is_empty() {
            return Ok(());
        }
        let mut session = self.unlock.lock().await;
        let Some(s) = session.as_mut() else {
            return Ok(());
        };
        s.unlock.check()?;
        let mut conn = self.reads.acquire().await?;
        for r in refs {
            if s.keys.contains_key(&r) {
                continue;
            }
            if let Some(wrapped) = sinex_store::sealed::wrapped_key(&mut conn, &r).await? {
                let key = s.unlock.unwrap(&wrapped)?;
                s.keys.insert(r, sinex_sealing::Zeroizing::new(key));
            }
        }
        s.unlock.check()?;
        crate::material_access::sealing::unseal_in(value, &s.keys);
        Ok(())
    }

    async fn dispatch(&self, request: Request, caller: &Caller) -> Result<Reply> {
        let actor = caller.actor;
        let method = request.method();
        Ok(match request {
            Request::Signal {
                action,
                name,
                rule,
                page,
                cursor,
            } => data(
                self.signal(caller, &action, name, rule, Page { size: page, cursor })
                    .await?,
            ),
            Request::Kinds { kind, page, cursor } => {
                self.kind_fields(caller, kind, page, cursor, false).await?
            }
            Request::Fields { kind, page, cursor } => {
                self.kind_fields(caller, kind, page, cursor, true).await?
            }
            Request::Backup {
                action,
                hold,
                output,
                page,
                cursor,
            } => {
                self.backup(caller, &action, hold, output, page, cursor)
                    .await?
            }
            Request::Status => Reply::Ok {
                result: self.status(caller).await?,
            },
            Request::SourcesDiscover => Reply::Ok {
                result: self.sources_discover().await?,
            },
            Request::Query {
                query,
                page,
                cursor,
            } => {
                self.query(caller, &query, Page { size: page, cursor })
                    .await?
            }
            Request::QueryComplete { query } => data(json!({"fields": query::complete(&query)})),
            Request::QueryList => {
                let mut conn = self.acquire(caller).await?;
                data(json!({"queries": queries::list(&mut conn).await?}))
            }
            request @ (Request::QuerySave { .. } | Request::Mark { .. }) => {
                data(self.query_record(request, caller).await?)
            }
            Request::Context {
                since,
                until,
                page,
                cursor,
            } => {
                self.context_view(caller, since, until, Page { size: page, cursor })
                    .await?
            }
            Request::Timeline {
                query,
                page,
                cursor,
            } => {
                self.query(
                    caller,
                    &format!("events {}", query.unwrap_or_default()),
                    Page { size: page, cursor },
                )
                .await?
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
                .await?
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
                        return Err(Error::bad_request(
                            "cwd applies only to recall last; use a session-field query for other filters",
                        ));
                    }
                    text.push_str(" where cwds ^ ");
                    text.push_str(&serde_json::to_string(&cwd).expect("strings serialize"));
                }
                self.recall(caller, &text, Page { size: page, cursor })
                    .await?
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
                .await?
            }
            Request::Sql { sql, page, cursor } => self.sql(caller, sql, page, cursor).await?,
            Request::Show { reference } => {
                let r = query::parse_reference(&reference)
                    .map_err(|error| error.failure(&reference))?;
                if let query::Reference::Window { from, to } = &r {
                    return self
                        .query(
                            caller,
                            &format!("recall sinex:window/{from}/{to}"),
                            Page::default(),
                        )
                        .await;
                }
                let mut conn = self.acquire(caller).await?;
                data(json!({ "event": sinex_store::query::show(&mut conn, &r).await? }))
            }
            Request::Archive {
                target,
                material,
                run,
                source,
                after,
                before,
                compare,
            } => {
                let scope = self
                    .lifecycle_scope(
                        caller,
                        operations::ScopeArgs {
                            target,
                            material,
                            run,
                            source,
                            after,
                            before,
                            operation: None,
                        },
                    )
                    .await?;
                self.lifecycle(caller, Act::Archive, scope, compare.unwrap_or(false))
                    .await?
            }
            Request::Restore {
                target,
                material,
                run,
                source,
                after,
                before,
                operation,
                compare,
            }
            | Request::Tombstone {
                target,
                material,
                run,
                source,
                after,
                before,
                operation,
                compare,
            } => {
                let act = if method == "restore" {
                    Act::Restore
                } else {
                    Act::Tombstone
                };
                let scope = self
                    .lifecycle_scope(
                        caller,
                        operations::ScopeArgs {
                            target,
                            material,
                            run,
                            source,
                            after,
                            before,
                            operation,
                        },
                    )
                    .await?;
                self.lifecycle(caller, act, scope, compare.unwrap_or(false))
                    .await?
            }
            Request::Undo { operation, compare } => {
                let id = uuid::Uuid::parse_str(&operation)
                    .map_err(|e| Error::bad_value(format!("operation {operation}: {e}")))?;
                let scope = sinex_model::lifecycle::Scope::Operation {
                    operation: OperationId(id),
                };
                self.lifecycle(caller, Act::Undo, scope, compare.unwrap_or(false))
                    .await?
            }
            Request::Resume { operation } => self.resume(caller, &operation).await?,
            Request::Replay {
                sealed,
                material,
                binding,
                event_type,
                from,
                until,
                compare,
                all,
                batch_events,
            } => {
                if batch_events == Some(0) {
                    return Err(Error::bad_request(
                        "--batch-events needs at least one reading per batch",
                    ));
                }
                let scope = crate::replay::Scope {
                    materials: material
                        .iter()
                        .map(|m| {
                            uuid::Uuid::parse_str(m)
                                .map_err(|e| Error::bad_value(format!("material {m}: {e}")))
                        })
                        .collect::<Result<_>>()?,
                    binding,
                    event_types: (!event_type.is_empty()).then_some(event_type),
                    from: from.as_deref().map(timestamp).transpose()?,
                    until: until.as_deref().map(timestamp).transpose()?,
                    batch_events,
                };
                self.replay(
                    caller,
                    crate::replay::Options {
                        scope,
                        compare: compare.unwrap_or(false),
                        resume: None,
                        actor: actor.to_string(),
                        unlock: None,
                    },
                    all.unwrap_or(false),
                    sealed.unwrap_or(false),
                )
                .await?
            }
            Request::Recompute {
                derivation,
                compare,
                switch,
                from,
                until,
                examples,
            } => {
                let from = from.as_deref().map(timestamp).transpose()?;
                let until = until.as_deref().map(timestamp).transpose()?;
                let range = (from.is_some() || until.is_some()).then(|| {
                    (
                        from.unwrap_or(sinex_model::Timestamp(time::OffsetDateTime::UNIX_EPOCH)),
                        until.unwrap_or_else(sinex_model::Timestamp::now),
                    )
                });
                self.recompute(
                    caller,
                    &derivation,
                    compare.unwrap_or(false),
                    switch,
                    range,
                    usize::try_from(examples.unwrap_or(5)).unwrap_or(usize::MAX),
                )
                .await?
            }
            Request::Comparisons { derivation, limit } => {
                let mut conn = self.acquire(caller).await?;
                let limit = i64::try_from(limit.unwrap_or(20)).unwrap_or(i64::MAX);
                let kept =
                    sinex_store::derivations::comparisons(&mut conn, derivation.as_deref(), limit)
                        .await?;
                let rows: Vec<Value> = kept
                    .into_iter()
                    .map(|c| {
                        json!({
                            "id": c.id,
                            "derivation": c.derivation,
                            "version": c.version,
                            "context": c.context,
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
            Request::Crawl {
                allow,
                url,
                limit,
                pause,
            } => {
                self.crawl(caller, allow, url, limit, pause.unwrap_or(2))
                    .await?
            }
            r @ (Request::ForecastIssue { .. }
            | Request::ForecastResolve { .. }
            | Request::ForecastReport) => data(self.forecast(r, caller).await?),
            Request::AnalysisCatalog
            | Request::AnalysisRun { .. }
            | Request::AnalysisPanel { .. }
            | Request::Gaps { .. }
            | Request::AnalysisWhy { .. }
            | Request::Brief { .. } => self.analysis(caller, request).await?,
            Request::Follow { .. } => {
                return Err(Error::bad_request(
                    "follow streams; use the API's follow stream",
                ));
            }
            Request::Stage {
                path,
                parser,
                account,
                export_date,
                follow,
                batch_events,
            } => {
                self.stage(
                    caller,
                    &path,
                    &parser,
                    account,
                    export_date,
                    follow.unwrap_or(false),
                    batch_events,
                )
                .await?
            }
            Request::StageList { active } => {
                let mut conn = self.acquire(caller).await?;
                let stages = sinex_store::stages::list(&mut conn, active.unwrap_or(false)).await?;
                Reply::Ok {
                    result: json!({ "stages": stages }),
                }
            }
            Request::StageShow { operation } => self.stage_show(caller, &operation).await?,
            r @ (Request::Same { .. }
            | Request::Distinct { .. }
            | Request::WithdrawDecision { .. }) => data(self.decide_pair(r, caller).await?),
            r @ (Request::PrivacyRules { .. }
            | Request::PrivacyRule { .. }
            | Request::PrivacyEnd { .. }
            | Request::PrivacyClass { .. }
            | Request::Unlock { .. }
            | Request::Lock
            | Request::PrivacyVerify) => self.privacy(r, caller).await?,
            r @ (Request::LibraryAdd { .. }
            | Request::LibraryItem { .. }
            | Request::LibraryCurate { .. }
            | Request::LibraryCollection { .. }
            | Request::LibraryRank { .. }
            | Request::LibraryOpen { .. }
            | Request::LibraryViewed { .. }
            | Request::LibraryPreview { .. }
            | Request::LibraryImport { .. }) => data(self.library(r, caller).await?),
        })
    }

    /// The media library's commands (`crate::library`). Reads see only what
    /// the caller may, through the same visibility as every other read.
    async fn library(&self, request: Request, caller: &Caller) -> Result<Value> {
        let mut conn = self.acquire(caller).await?;
        crate::library::dispatch(
            &mut conn,
            self.writer.as_ref(),
            &self.blobs,
            &self.host,
            &self.library_roots,
            sinex_model::time_settings::TimeSettings::from_parts(self.zone.clone(), self.day_start),
            request,
            caller.actor,
        )
        .await
    }

    async fn query(&self, caller: &Caller, text: &str, page: Page) -> Result<Reply> {
        if text
            .split_whitespace()
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("recall"))
        {
            return self.recall(caller, text, page).await;
        }
        // A refusal of the query text is placed in it.
        let placed = |error: QueryError| Error::from(error).in_query(text);
        let mut ctx = self.context();
        let mut conn = self.acquire(caller).await?;
        if let Some(name) = query::saved::reference_name(text) {
            let definitions = queries::definitions(&mut conn, &name).await?;
            let declarations = sinex_schema::kinds::all(&mut conn).await?;
            ctx.catalog = Some(query::QueryCatalog::new(declarations.into_values()));
            let bound = query::saved::bind_saved(text, &ctx, &definitions).map_err(placed)?;
            if matches!(
                bound.source,
                Source::Recall { .. } | Source::RecallLast { .. } | Source::RecallMark { .. }
            ) {
                drop(conn);
                return self.recall(caller, text, page).await;
            }
        }
        let result = if query::native::is_command(text) {
            let operation = query::native::parse(text, &ctx).map_err(placed)?;
            sinex_store::query::run_operation(&mut conn, text, &operation, &ctx, &page).await
        } else {
            sinex_store::query::run(&mut conn, text, None, &ctx, &page).await
        };
        match result {
            Ok(answer) => Ok(data(answer_json(answer))),
            Err(sinex_store::Error::Query(error)) => Err(placed(error)),
            Err(error) => Err(error.into()),
        }
    }

    /// The discovery dry run: what this host's sources would capture, read
    /// only from the inputs' metadata.
    async fn sources_discover(&self) -> Result<Value> {
        let context = self.source_config.context(&self.host);
        let (inputs, unimplemented) = tokio::task::spawn_blocking(move || {
            sinex_sources::discovery::status(&context)
                .map(|inputs| (inputs, sinex_sources::unimplemented::census(&context)))
        })
        .await?
        .map_err(Error::internal)?;
        Ok(json!({"inputs": inputs, "unimplemented": unimplemented}))
    }

    async fn status(&self, caller: &Caller) -> Result<Value> {
        let mut conn = self.acquire(caller).await?;
        let row = sinex_store::diagnostics::positions(&mut conn).await?;
        let open_backup_holds = sinex_store::backup::open_summary(&mut conn).await?;
        let telemetry =
            sinex_store::telemetry::latest(&mut conn, SinexTelemetry::EVENT_TYPE).await?;
        let upgrade_work = sinex_store::diagnostics::upgrade_work(&mut conn).await?;
        let refusals = sinex_store::diagnostics::ingress_refusals(&mut conn).await?;
        let producer_errors = sinex_store::diagnostics::producer_errors(&mut conn).await?;
        let selected = sinex_store::registry::selected_derivations(&mut conn).await?;
        let time_settings =
            sinex_model::time_settings::TimeSettings::from_parts(self.zone.clone(), self.day_start);
        let mut available: Vec<(String, u32, sinex_model::DerivationHistory)> =
            crate::analysis::keyed_with(&self.derivations)
                .iter()
                .map(|d| {
                    (
                        d.name().to_owned(),
                        d.version_for(&time_settings),
                        d.history_policy(),
                    )
                })
                .collect();
        available.extend(
            crate::derivation_runner::per_event_with(&self.derivations)?
                .iter()
                .map(|d| (d.name().to_owned(), d.version(), d.history_policy())),
        );
        let build = crate::build_identity::current()?;
        let names: Vec<_> = available.iter().map(|(name, _, _)| name.clone()).collect();
        let versions: Vec<_> = available
            .iter()
            .map(|(_, version, _)| i64::from(*version))
            .collect();
        let comparisons =
            sinex_store::registry::latest_comparisons(&mut conn, &names, &versions, build).await?;
        let producer_versions: Vec<_> = available.into_iter().map(|(name, version, history)| {
            let current = selected.iter().find(|(selected_name, _, _)| selected_name == &name);
            let compared = comparisons.iter().find(|(producer,..)| producer == &name);
            json!({"name": name, "selected": current.map(|(_,version,_)| version), "selected_build": current.map(|(_,_,build)|build), "available": version,
                "state": if current.is_some_and(|(_,selected_version,selected_build)| *selected_version == i64::from(version) && selected_build == build) { "selected" } else { "pending_history_switch" },
                "history": history, "build": build, "last_comparison": compared.map(|(_,id,made_at,keys,changed,complete)| json!({"id":id,"made_at":made_at,"keys":keys,"changed":changed,"complete":complete}))})
        }).collect();
        let rule_rows = sinex_store::signals::rule_rows(
            &mut conn,
            sinex_model::event_types::signal::RuleWritten::EVENT_TYPE,
            row.commit_seq,
        )
        .await?;
        // Ambiguity per fusion family: pairs awaiting a decision, decided
        // pairs, and fused records chosen among several candidates.
        let fusion_families: Vec<(String, String)> = sinex_derivations::fusion::families()
            .iter()
            .map(|f| (f.name.to_owned(), f.output.to_owned()))
            .collect();
        let fusion = sinex_store::diagnostics::fusion_ambiguity(
            &mut conn,
            &fusion_families,
            sinex_model::event_types::fused::ConsideredPair::EVENT_TYPE,
        )
        .await?;
        let signals = json!({"configured":self.source_config.signals.is_some(),"rules":rule_rows.into_iter().map(|(id,payload)|json!({"citation":format!("sinex:ev/{id}"),"rule":payload})).collect::<Vec<_>>()});
        drop(conn);
        let sources = match self
            .query(
                caller,
                "sources | sort by binding",
                Page {
                    size: Some(1000),
                    cursor: None,
                },
            )
            .await
        {
            Ok(Reply::Ok { result }) => result.get("rows").cloned().unwrap_or(Value::Null),
            Ok(Reply::StoppedEarly { failure, .. } | Reply::Failed { failure }) => {
                json!({ "unavailable": failure })
            }
            Err(error) => json!({ "unavailable": error.failure() }),
        };
        let context = self.source_config.context(&self.host);
        let discovery =
            tokio::task::spawn_blocking(move || sinex_sources::discovery::status(&context))
                .await?
                .map_err(Error::internal)?;
        let read_failures: Vec<_> = self
            .read_failures
            .lock()
            .unwrap()
            .iter()
            .map(|(binding, failure)| {
                json!({"binding": binding, "error": failure.error, "since": failure.since})
            })
            .collect();
        let exclusions: std::collections::BTreeMap<_, _> = self
            .source_config
            .exclude
            .iter()
            .filter_map(|(name, exclusion)| exclusion.as_ref().map(|exclusion| (name, exclusion)))
            .collect();
        Ok(json!({
            "commit_seq": row.commit_seq,
            "search_index": {
                "position": row.search_position,
                "behind": row.commit_seq - row.search_position.unwrap_or(0),
                "refused": row.search_refusals,
            },
            "schema_version": row.schema_version,
            // Upgrade tasks the daemon still runs after start; empty when done.
            "upgrade_work_after_start": upgrade_work,
            "failures_24h": row.failures_24h,
            // Inputs that cannot be read admit no records, so no record failure
            // counts them; each binding failing now is listed here instead.
            "read_failures": read_failures,
            "ingress_refusals_24h": refusals,
            "producer_errors": producer_errors,
            "writer": self.writer.is_some(),
            "backup_holds": { "open":open_backup_holds.0,"oldest":open_backup_holds.1 },
            "zone": self.context().zone_name(),
            "day_start": self.day_start.to_string(),
            "sources": sources,
            // The daemon's newest sample of each of its parts.
            "telemetry": telemetry,
            "producer_versions": producer_versions,
            "signals":signals,
            "fusion":fusion,
            "discovery": discovery,
            "exclusions": exclusions,
        }))
    }
}

fn answer_json(a: Answer) -> Value {
    serde_json::to_value(a).expect("answers serialize")
}

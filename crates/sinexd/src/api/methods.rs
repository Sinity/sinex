//! Executing requests: one function per command of the table, shared by every
//! surface. Reads use the read pool; writes go to the writer as batches, so
//! admission stays the only way anything is stored.
use crate::writer::WriterHandle;
use crate::{Error, Result};
use serde_json::{Value, json};
use sinex_model::{EventId, EventPayload, OperationId, RunId};
use sinex_protocol::{Access, DATA_NOTICE, Reply, Request};
use sinex_query::{self as query, Context, QueryError, Source};
use sinex_store::query::{Answer, Page};
use sqlx::PgPool;

mod analysis;
mod backup;
pub(crate) use backup::StartedBackup;
mod forecast;
mod fusion;
mod kinds;
mod operations;
mod privacy;
mod queries;
mod recall;
mod signals;
mod sql;
mod status;
pub(crate) use status::{HISTORY_WINDOW, status_fields};

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

type Conn = crate::request_deadline::ReadConnection;

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
    pub(crate) async fn acquire(&self, caller: &Caller) -> Result<Conn> {
        let mut conn = crate::request_deadline::acquire(&self.reads).await?;
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

    pub(crate) async fn call(
        &self,
        call: impl Into<sinex_protocol::Call>,
        access: Access,
    ) -> Reply {
        let call = call.into();
        let default = sinex_protocol::command(call.request.method())
            .expect("declared command")
            .default_deadline();
        let duration = call.deadline.duration(Some(default));
        let api = self.clone();
        match crate::request_deadline::run(duration, async move {
            let caller = api.caller(access).await;
            api.answer(call.request, &caller).await
        })
        .await
        {
            Ok(reply) => reply,
            Err(error) => Reply::failed(error),
        }
    }

    async fn answer(&self, request: Request, caller: &Caller) -> Result<Reply> {
        let sql_page = matches!(&request, Request::Sql { .. });
        let reply = self.dispatch(request, caller).await?;
        // Sealed values are unsealed only for a caller holding the class.
        match reply {
            Reply::Ok { mut result } => {
                if !sql_page && caller.grants.split(',').any(|g| g == "sealed") {
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
        let mut conn = crate::request_deadline::acquire(&self.reads).await?;
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
        match request {
            Request::Status => self.status(caller).await,
            Request::SourcesDiscover => self.sources_discover().await,
            request @ Request::Watch { .. } => self.watch_request(caller, request).await,
            request @ (Request::Kinds { .. } | Request::Fields { .. }) => {
                self.kind_fields(caller, request).await
            }
            request @ Request::Backup { .. } => self.backup(caller, request).await,
            request @ (Request::Query { .. }
            | Request::QueryComplete { .. }
            | Request::QueryList
            | Request::Save { .. }
            | Request::Mark { .. }
            | Request::Context { .. }
            | Request::Timeline { .. }
            | Request::Search { .. }
            | Request::Trace { .. }) => self.query_request(caller, request).await,
            request @ Request::Recall { .. } => self.recall_request(caller, request).await,
            Request::Sql {
                sql,
                page_size,
                cursor,
            } => self.sql(caller, sql, page_size, cursor).await,
            request @ Request::Show { .. } => self.show(caller, request).await,
            request @ (Request::Archive { .. }
            | Request::Restore { .. }
            | Request::Tombstone { .. }
            | Request::Undo { .. }) => self.lifecycle_request(caller, request).await,
            Request::Resume { operation } => self.resume(caller, &operation).await,
            request @ Request::Replay { .. } => self.replay_request(caller, request).await,
            request @ Request::Recompute { .. } => self.recompute_request(caller, request).await,
            request @ Request::Comparisons { .. } => self.comparisons(caller, request).await,
            Request::Crawl {
                allow,
                url,
                limit,
                pause,
            } => {
                self.crawl(caller, allow, url, limit, pause.unwrap_or(2))
                    .await
            }
            request @ (Request::ForecastIssue { .. }
            | Request::ForecastResolve { .. }
            | Request::ForecastReport) => self.forecast(request, caller).await.map(data),
            request @ (Request::AnalysisCatalog
            | Request::AnalysisRun { .. }
            | Request::AnalysisPanel { .. }
            | Request::Gaps { .. }
            | Request::AnalysisWhy { .. }
            | Request::Brief { .. }) => self.analysis(caller, request).await,
            Request::Follow { .. } => Err(Error::bad_request(
                "follow streams; use the API's follow stream",
            )),
            request @ Request::Stage { .. } => self.stage_request(caller, request).await,
            Request::StageList { active } => self.stage_list(caller, active.unwrap_or(false)).await,
            Request::StageShow { operation } => self.stage_show(caller, &operation).await,
            request @ (Request::Same { .. }
            | Request::Distinct { .. }
            | Request::WithdrawDecision { .. }) => {
                self.decide_pair(request, caller).await.map(data)
            }
            request @ (Request::PrivacyRules { .. }
            | Request::PrivacyRule { .. }
            | Request::PrivacyEnd { .. }
            | Request::PrivacyClass { .. }
            | Request::Unlock { .. }
            | Request::Lock
            | Request::PrivacyVerify) => self.privacy(request, caller).await,
            request @ (Request::LibraryAdd { .. }
            | Request::LibraryItem { .. }
            | Request::LibraryCurate { .. }
            | Request::LibraryCollection { .. }
            | Request::LibraryRank { .. }
            | Request::LibraryOpen { .. }
            | Request::LibraryViewed { .. }
            | Request::LibraryPreview { .. }
            | Request::LibraryImport { .. }) => self.library(request, caller).await.map(data),
        }
    }

    /// The media library's commands (`crate::library`). Reads see only what
    /// the caller may, through the same visibility as every other read.
    async fn library(&self, request: Request, caller: &Caller) -> Result<Value> {
        if caller.actor != "operator"
            && let Request::LibraryAdd { path, .. } | Request::LibraryImport { path } = &request
        {
            let items = crate::stage::items(std::slice::from_ref(path), &[])?;
            let roots = self.source_config.agent_stage_roots(&self.host);
            tokio::task::spawn_blocking(move || crate::stage::within_roots(&items, &roots))
                .await??;
        }
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
        let result = if query::template::is_command(text) {
            let operation = query::template::parse(text, &ctx).map_err(placed)?;
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
    async fn sources_discover(&self) -> Result<Reply> {
        let context = self.source_config.context(&self.host);
        let (inputs, unimplemented) = tokio::task::spawn_blocking(move || {
            sinex_sources::discovery::status(&context)
                .map(|inputs| (inputs, sinex_sources::unimplemented::census(&context)))
        })
        .await?
        .map_err(Error::internal)?;
        Ok(Reply::Ok {
            result: json!({"inputs": inputs, "unimplemented": unimplemented}),
        })
    }
}

fn answer_json(a: Answer) -> Value {
    serde_json::to_value(a).expect("answers serialize")
}

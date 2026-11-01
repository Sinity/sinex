//! Operations that change what is stored: the lifecycle (archive, restore,
//! tombstone, undo, resume), replay, recomputing a derivation, and the crawl.
use super::*;
use sinex_model::lifecycle::{Act, Scope};

/// A lifecycle request's scope options, as the command table names them.
pub(super) struct ScopeArgs {
    pub target: Option<String>,
    pub material: Vec<String>,
    pub run: Option<String>,
    pub source: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub operation: Option<String>,
}

fn uuid(what: &str, text: &str) -> Result<uuid::Uuid, String> {
    uuid::Uuid::parse_str(text).map_err(|e| format!("{what} {text}: {e}"))
}

impl Api {
    /// Archive, restore, tombstone or undo, through the writer in batches.
    pub(super) async fn lifecycle(
        &self,
        caller: &Caller,
        act: Act,
        scope: Result<Scope, Reply>,
        compare: bool,
    ) -> Reply {
        let scope = match scope {
            Ok(scope) => scope,
            Err(reply) => return reply,
        };
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        answer(crate::lifecycle::run(&mut conn, writer, act, caller.actor, scope, compare).await)
    }

    /// Continue an operation from its saved position: a lifecycle operation
    /// or a replay.
    pub(super) async fn resume(&self, caller: &Caller, operation: &str) -> Reply {
        let operation = match uuid("operation", operation) {
            Ok(id) => OperationId(id),
            Err(e) => return refused(e),
        };
        let Some(writer) = &self.writer else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let kind = match sinex_store::lifecycle::operation_kind(&mut conn, operation).await {
            Ok(kind) => kind,
            Err(e) => return refused(e.to_string()),
        };
        match kind.as_deref() {
            None => refused(format!("no operation {}", operation.0)),
            Some(sinex_store::stages::KIND) => {
                drop(conn);
                self.resume_stage(caller, operation).await
            }
            Some("replay") => {
                drop(conn);
                self.replay(
                    caller,
                    crate::replay::Options {
                        scope: crate::replay::Scope::default(),
                        compare: false,
                        resume: Some(operation),
                        actor: caller.actor.to_string(),
                        unlock: None,
                    },
                    false,
                    false,
                )
                .await
            }
            Some(_) => answer(crate::lifecycle::resume(&mut conn, writer, operation).await),
        }
    }

    /// The one scope a lifecycle request names: an event reference, the
    /// event rows of a query, materials, a run, a source and time range, or
    /// an earlier operation. None or several is an error, never everything.
    pub(super) async fn lifecycle_scope(
        &self,
        caller: &Caller,
        args: ScopeArgs,
    ) -> Result<Scope, Reply> {
        let given = [
            args.target.is_some(),
            !args.material.is_empty(),
            args.run.is_some(),
            args.source.is_some(),
            args.operation.is_some(),
        ]
        .into_iter()
        .filter(|g| *g)
        .count();
        if given != 1 {
            return Err(refused(
                "name exactly one scope: a reference or query, --material, --run, --source or --operation",
            ));
        }
        if args.source.is_none() && (args.after.is_some() || args.before.is_some()) {
            return Err(refused("--after and --before narrow --source"));
        }
        if !args.material.is_empty() {
            return args
                .material
                .iter()
                .map(|m| uuid("material", m).map(sinex_model::MaterialId))
                .collect::<Result<_, _>>()
                .map(|materials| Scope::Materials { materials })
                .map_err(refused);
        }
        if let Some(run) = &args.run {
            return uuid("run", run)
                .map(|run| Scope::Run { run: RunId(run) })
                .map_err(refused);
        }
        if let Some(operation) = &args.operation {
            return uuid("operation", operation)
                .map(|operation| Scope::Operation {
                    operation: OperationId(operation),
                })
                .map_err(refused);
        }
        if let Some(source) = args.source {
            return Ok(Scope::Source {
                source,
                after: args
                    .after
                    .as_deref()
                    .map(timestamp)
                    .transpose()
                    .map_err(refused)?,
                before: args
                    .before
                    .as_deref()
                    .map(timestamp)
                    .transpose()
                    .map_err(refused)?,
            });
        }
        let target = args.target.expect("one scope was given");
        let mut conn = self
            .acquire(caller)
            .await
            .map_err(|e| refused(e.to_string()))?;
        if let Ok(reference) = query::parse_reference(&target) {
            let id = sinex_store::query::resolve(&mut conn, &reference)
                .await
                .map_err(|e| refused(e.to_string()))?;
            return Ok(Scope::Events {
                events: vec![EventId(id)],
                query: None,
            });
        }
        // A query: every event row of its answer, page by page.
        let ctx = self.context();
        let mut page = Page::default();
        let mut events = Vec::new();
        loop {
            let answer = match sinex_store::query::run(&mut conn, &target, None, &ctx, &page).await
            {
                Ok(answer) => answer,
                Err(sinex_store::Error::Query(error)) => return Err(query_refusal(&target, error)),
                Err(e) => return Err(refused(e.to_string())),
            };
            if !matches!(answer.table.as_str(), "events" | "archive") {
                return Err(refused(format!(
                    "the query selects {} rows; a lifecycle scope takes event rows",
                    answer.table
                )));
            }
            for row in &answer.rows {
                let id = row
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| refused("the query's rows carry no event id"))?;
                events.push(EventId(uuid("event", id).map_err(refused)?));
            }
            match answer.page.cursor {
                Some(cursor) => page.cursor = Some(cursor),
                None => break,
            }
        }
        events.sort_by_key(|e| e.0);
        events.dedup();
        Ok(Scope::Events {
            events,
            query: Some(target),
        })
    }

    pub(super) async fn replay(
        &self,
        caller: &Caller,
        mut o: crate::replay::Options,
        all: bool,
        sealed: bool,
    ) -> Reply {
        if sealed {
            if caller.actor != "operator" || !caller.grants.split(',').any(|g| g == "sealed") {
                return refused(
                    "sealed replay requires the operator's current unlock and explicit --sealed grant",
                );
            }
            let unlock = self.unlock.lock().await;
            let Some(session) = unlock.as_ref() else {
                return refused("sealed replay needs an operator unlock");
            };
            if let Err(error) = session.unlock.check() {
                return refused(error);
            }
            o.unlock = Some(session.unlock.clone());
        }
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

    pub(super) async fn recompute(
        &self,
        caller: &Caller,
        derivation: &str,
        compare: bool,
        switch: Option<i64>,
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
        let mut driver = match crate::derivation_runner::DerivationRunner::for_one(
            conn,
            writer.clone(),
            derivation,
            &self.host,
            &self.derivations,
            sinex_model::time_settings::TimeSettings::from_parts(self.zone.clone(), self.day_start),
        )
        .await
        {
            Ok(d) => d.with_blobs(self.blobs.clone()),
            Err(e) => return refused(e),
        };
        let result = if let Some(comparison) = switch {
            if compare || range.is_some() {
                Err("switch requires a comparison ID and no compare or range options".to_owned())
            } else {
                driver.switch(derivation, comparison, caller.actor).await
                    .map(|count| json!({"derivation": derivation, "comparison": comparison, "keys_marked": count}))
            }
        } else if compare {
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

    pub(super) async fn crawl(
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
}

fn answer(result: Result<crate::lifecycle::Answer, String>) -> Reply {
    match result {
        Ok(crate::lifecycle::Answer::Done(result)) => Reply::Ok { result },
        Ok(crate::lifecycle::Answer::Refused(reason)) => refused(reason),
        Ok(crate::lifecycle::Answer::StoppedEarly(reason, result)) => {
            Reply::StoppedEarly { reason, result }
        }
        Err(e) => refused(e),
    }
}

impl Api {
    /// Stage `paths`, each with the parser named for it or recognised by
    /// detection, in the background; with `follow`, answer when it ends.
    pub(super) async fn stage(
        &self,
        caller: &Caller,
        paths: &[String],
        parsers: &[String],
        account: Option<String>,
        export_date: Option<String>,
        follow: bool,
    ) -> Reply {
        let items = match crate::stage::items(paths, parsers) {
            Ok(items) => items,
            Err(e) => return refused(e),
        };
        let request = crate::stage::Request {
            items,
            account,
            export_date,
            host: self.host.clone(),
            zone: self.zone.iana_name().map(str::to_owned),
        };
        self.start_stage(caller, OperationId::new(), request, None, follow)
            .await
    }

    /// Read a stage that did not end again, from its recorded plan, its
    /// saved progress and its bindings' cursors, and answer when it ends.
    async fn resume_stage(&self, caller: &Caller, operation: OperationId) -> Reply {
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        let stored = match sinex_store::stages::show(&mut conn, operation).await {
            Ok(Some(stored)) => stored,
            Ok(None) => return refused(format!("no stage operation {}", operation.0)),
            Err(e) => return refused(e.to_string()),
        };
        drop(conn);
        if let Some(result) = stored.result {
            use sinex_model::DbCheck;
            return refused(format!(
                "stage {} already ended {}",
                operation.0,
                result.as_db()
            ));
        }
        match crate::stage::Request::from_scope(&stored.scope) {
            Ok(request) => {
                self.start_stage(caller, operation, request, Some(stored.inventory), true)
                    .await
            }
            Err(e) => refused(e),
        }
    }

    /// Plan, claim the bindings, and read in a task of its own; `saved` is
    /// the progress of a stage being resumed.
    async fn start_stage(
        &self,
        caller: &Caller,
        operation: OperationId,
        request: crate::stage::Request,
        saved: Option<Value>,
        follow: bool,
    ) -> Reply {
        let Some(writer) = self.writer.clone() else {
            return refused("this daemon has no writer; writes go through `sinexd run`");
        };
        let planned = request.clone();
        let config = self.source_config.clone();
        let host = self.host.clone();
        let plan = tokio::task::spawn_blocking(move || {
            let plan = planned.plan()?;
            crate::stage::not_continuous(&plan, &crate::binding_runner::plan(&config, &host)?)?;
            Ok::<_, String>(plan)
        })
        .await;
        let plan = match plan {
            Ok(Ok(plan)) => plan,
            Ok(Err(e)) => return refused(e),
            Err(e) => return refused(e.to_string()),
        };
        let claim = match crate::stage::Claim::take(&self.running_stages, &plan) {
            Ok(claim) => claim,
            Err(e) => return refused(e),
        };
        let unlock = if caller.actor == "operator" {
            self.unlock
                .lock()
                .await
                .as_ref()
                .filter(|session| session.unlock.check().is_ok())
                .map(|session| session.unlock.clone())
        } else {
            None
        };
        let reads = self.reads.clone();
        let blobs = self.blobs.clone();
        let stop = self.stop.clone();
        let actor = caller.actor.to_owned();
        let (begun, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _claim = claim;
            let mut conn = reads.acquire().await.map_err(|e| e.to_string())?;
            let mut staging = crate::stage::Staging {
                read_conn: &mut conn,
                blobs: &blobs,
                writer: &writer,
                access: crate::material_access::Access {
                    recipient: None,
                    unlock: unlock.as_ref(),
                },
                stop,
            };
            match saved {
                Some(saved) => {
                    staging
                        .resume(operation, &request, plan, &saved, begun)
                        .await
                }
                None => staging.run(operation, &actor, &request, plan, begun).await,
            }
        });
        if !follow && let Ok(started) = started.await {
            return Reply::Ok { result: started };
        }
        match task.await {
            Ok(Ok(outcome)) if outcome.failures.is_empty() => Reply::Ok {
                result: outcome.report,
            },
            Ok(Ok(outcome)) => Reply::StoppedEarly {
                reason: format!(
                    "stage {} stopped early; `resume {}` reads it again once the cause is cleared: {}",
                    outcome.operation.0,
                    outcome.operation.0,
                    outcome.failures.join("; ")
                ),
                result: outcome.report,
            },
            Ok(Err(e)) => refused(e),
            Err(e) => refused(e.to_string()),
        }
    }

    async fn stage_show(&self, caller: &Caller, operation: &str) -> Reply {
        let operation = match uuid("operation", operation) {
            Ok(id) => OperationId(id),
            Err(e) => return refused(e),
        };
        let mut conn = match self.acquire(caller).await {
            Ok(c) => c,
            Err(e) => return refused(e.to_string()),
        };
        match sinex_store::stages::show(&mut conn, operation).await {
            Ok(Some(stage)) => Reply::Ok {
                result: json!(stage),
            },
            Ok(None) => refused(format!("no stage operation {}", operation.0)),
            Err(e) => refused(e.to_string()),
        }
    }
}

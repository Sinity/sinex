//! Operations that change what is stored: the lifecycle (archive, restore,
//! tombstone, undo, resume), replay, recomputing a derivation, and the crawl.
use super::*;
use crate::writer::WriterHandle;
use crate::{Error, Result};
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

fn uuid(what: &str, text: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(text).map_err(|e| Error::bad_value(format!("{what} {text}: {e}")))
}

impl Api {
    /// Archive, restore, tombstone or undo, through the writer in batches.
    pub(super) async fn lifecycle(
        &self,
        caller: &Caller,
        act: Act,
        scope: Scope,
        compare: bool,
    ) -> Result<Reply> {
        let writer = self.writer()?;
        let mut conn = self.acquire(caller).await?;
        Ok(
            crate::lifecycle::run(&mut conn, writer, act, caller.actor, scope, compare)
                .await?
                .reply(),
        )
    }

    /// The writer, which only `sinexd run` holds.
    pub(super) fn writer(&self) -> Result<&WriterHandle> {
        self.writer.as_ref().ok_or_else(|| {
            Error::not_configured("this daemon has no writer; writes go through `sinexd run`")
        })
    }

    /// Continue an operation from its saved position: a lifecycle operation
    /// or a replay.
    pub(super) async fn resume(&self, caller: &Caller, operation: &str) -> Result<Reply> {
        let operation = OperationId(uuid("operation", operation)?);
        let writer = self.writer()?;
        let mut conn = self.acquire(caller).await?;
        let kind = sinex_store::lifecycle::operation_kind(&mut conn, operation).await?;
        match kind.as_deref() {
            None => Err(Error::not_found(format!("no operation {}", operation.0))),
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
            Some(_) => Ok(crate::lifecycle::resume(&mut conn, writer, operation)
                .await?
                .reply()),
        }
    }

    /// The one scope a lifecycle request names: an event reference, the
    /// event rows of a query, materials, a run, a source and time range, or
    /// an earlier operation. None or several is an error, never everything.
    pub(super) async fn lifecycle_scope(&self, caller: &Caller, args: ScopeArgs) -> Result<Scope> {
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
            return Err(Error::bad_request(
                "name exactly one scope: a reference or query, --material, --run, --source or --operation",
            ));
        }
        if args.source.is_none() && (args.after.is_some() || args.before.is_some()) {
            return Err(Error::bad_request("--after and --before narrow --source"));
        }
        if !args.material.is_empty() {
            return args
                .material
                .iter()
                .map(|m| uuid("material", m).map(sinex_model::MaterialId))
                .collect::<Result<_>>()
                .map(|materials| Scope::Materials { materials });
        }
        if let Some(run) = &args.run {
            return Ok(Scope::Run {
                run: RunId(uuid("run", run)?),
            });
        }
        if let Some(operation) = &args.operation {
            return Ok(Scope::Operation {
                operation: OperationId(uuid("operation", operation)?),
            });
        }
        if let Some(source) = args.source {
            return Ok(Scope::Source {
                source,
                after: args.after.as_deref().map(timestamp).transpose()?,
                before: args.before.as_deref().map(timestamp).transpose()?,
            });
        }
        let target = args.target.expect("one scope was given");
        let mut conn = self.acquire(caller).await?;
        if let Ok(reference) = query::parse_reference(&target) {
            let id = sinex_store::query::resolve(&mut conn, &reference).await?;
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
                Err(sinex_store::Error::Query(error)) => return Err(error.failure(&target).into()),
                Err(e) => return Err(e.into()),
            };
            if !matches!(answer.table.as_str(), "events" | "archive") {
                return Err(Error::bad_request(format!(
                    "the query selects {} rows; a lifecycle scope takes event rows",
                    answer.table
                )));
            }
            for row in &answer.rows {
                let id = row
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::bad_request("the query's rows carry no event id"))?;
                events.push(EventId(uuid("event", id)?));
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
    ) -> Result<Reply> {
        if sealed {
            if caller.actor != "operator" || !caller.grants.split(',').any(|g| g == "sealed") {
                return Err(Error::refused(
                    "sealed replay requires the operator's current unlock and explicit --sealed grant",
                ));
            }
            let unlock = self.unlock.lock().await;
            let session = unlock
                .as_ref()
                .ok_or_else(|| Error::refused("sealed replay needs an operator unlock"))?;
            session.unlock.check()?;
            o.unlock = Some(session.unlock.clone());
        }
        let writer = self.writer()?;
        let mut conn = self.acquire(caller).await?;
        let compare = o.compare;
        let report = crate::replay::run(&mut conn, &self.blobs, writer, o).await?;
        Ok(Reply::Ok {
            result: report.to_json(compare, all),
        })
    }

    pub(super) async fn recompute(
        &self,
        caller: &Caller,
        derivation: &str,
        compare: bool,
        switch: Option<i64>,
        range: Option<(sinex_model::Timestamp, sinex_model::Timestamp)>,
        examples: usize,
    ) -> Result<Reply> {
        let writer = self.writer()?;
        let conn = self.acquire(caller).await?.detach();
        let mut driver = crate::derivation_runner::DerivationRunner::for_one(
            conn,
            writer.clone(),
            derivation,
            &self.host,
            &self.derivations,
            sinex_model::time_settings::TimeSettings::from_parts(self.zone.clone(), self.day_start),
        )
        .await?
        .with_blobs(self.blobs.clone());
        let result = if let Some(comparison) = switch {
            if compare || range.is_some() {
                Err(Error::bad_request(
                    "switch requires a comparison ID and no compare or range options",
                ))
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
        let result = result?;
        ended?;
        Ok(Reply::Ok { result })
    }

    pub(super) async fn crawl(
        &self,
        caller: &Caller,
        allow: Vec<String>,
        urls: Vec<String>,
        limit: Option<u64>,
        pause: u64,
    ) -> Result<Reply> {
        let writer = self.writer()?;
        let mut conn = self.acquire(caller).await?;
        let totals = crate::crawl::run(
            &mut conn,
            &self.blobs,
            writer,
            &self.host,
            allow,
            urls,
            limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
            std::time::Duration::from_secs(pause),
        )
        .await?;
        Ok(Reply::Ok {
            result: json!({ "crawl": totals.to_json() }),
        })
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
        batch_events: Option<u64>,
    ) -> Result<Reply> {
        let items = crate::stage::items(paths, parsers)?;
        if batch_events == Some(0) {
            return Err(Error::bad_request(
                "--batch-events needs at least one reading per batch",
            ));
        }
        let request = crate::stage::Request {
            items,
            account,
            export_date,
            host: self.host.clone(),
            zone: self.zone.iana_name().map(str::to_owned),
            batch_events,
        };
        self.start_stage(caller, OperationId::new(), request, None, follow)
            .await
    }

    /// Read a stage that did not end again, from its recorded plan, its
    /// saved progress and its bindings' cursors, and answer when it ends.
    async fn resume_stage(&self, caller: &Caller, operation: OperationId) -> Result<Reply> {
        let mut conn = self.acquire(caller).await?;
        let stored = sinex_store::stages::show(&mut conn, operation)
            .await?
            .ok_or_else(|| Error::not_found(format!("no stage operation {}", operation.0)))?;
        drop(conn);
        if stored.result == Some(sinex_model::lifecycle::OperationResult::Ok) {
            return Err(Error::conflict(format!(
                "stage {} already ended ok",
                operation.0
            )));
        }
        let request = crate::stage::Request::from_scope(&stored.scope)?;
        self.start_stage(caller, operation, request, Some(stored.inventory), true)
            .await
    }

    /// Plan, claim the bindings, and read in a task of its own; `saved` is
    /// the progress of a stage being resumed.
    async fn start_stage(
        &self,
        caller: &Caller,
        operation: OperationId,
        request: crate::stage::Request,
        saved: Option<sinex_model::stage::SavedProgress>,
        follow: bool,
    ) -> Result<Reply> {
        let writer = self.writer()?.clone();
        let planned = request.clone();
        let config = self.source_config.clone();
        let host = self.host.clone();
        // The operator's token stages any path the daemon can read; an
        // agent's only under the declared and configured roots.
        let operator = caller.actor == "operator";
        let plan = tokio::task::spawn_blocking(move || {
            if !operator {
                crate::stage::within_roots(&planned.items, &config.agent_stage_roots(&host))?;
            }
            let plan = planned.plan()?;
            crate::stage::not_continuous(&plan, &crate::binding_runner::plan(&config, &host)?)?;
            Ok::<_, Error>(plan)
        })
        .await??;
        let claim = crate::stage::Claim::take(&self.running_stages, &plan)?;
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
            let mut conn = reads.acquire().await?;
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
            return Ok(Reply::Ok {
                result: serde_json::to_value(started).expect("stage report serializes"),
            });
        }
        let outcome = task.await??;
        let result = serde_json::to_value(&outcome).expect("stage report serializes");
        if outcome.failures.is_empty() {
            return Ok(Reply::Ok { result });
        }
        let reason = format!(
            "stage {} stopped early; `resume {}` reads it again once the cause is cleared: {}",
            outcome.operation.0,
            outcome.operation.0,
            outcome.failures.join("; ")
        );
        Ok(Reply::StoppedEarly {
            failure: sinex_protocol::stopped_early(command_surface::Failure::new(
                command_surface::FailureKind::Io,
                reason,
            )),
            result,
        })
    }

    pub(super) async fn stage_show(&self, caller: &Caller, operation: &str) -> Result<Reply> {
        let operation = OperationId(uuid("operation", operation)?);
        let mut conn = self.acquire(caller).await?;
        let stage = sinex_store::stages::show(&mut conn, operation)
            .await?
            .ok_or_else(|| Error::not_found(format!("no stage operation {}", operation.0)))?;
        Ok(Reply::Ok {
            result: json!(stage),
        })
    }
}

//! Shared operation handlers. Product authentication supplies the caller;
//! the common table owns request metadata, schema validation and dispatch.
use super::*;
use command_surface::{Answer as SurfaceAnswer, Operation, OperationFuture, RequestContext};
use sinex_commands::Status;
use std::sync::Arc;

pub(crate) fn table(api: Api) -> command_surface::OperationTable {
    let mut table =
        command_surface::OperationTable::new(Arc::clone(sinex_commands::declarations()));
    table
        .bind("rows", RowsOperation(api.clone()))
        .expect("rows binds its declared request and reply");
    table
        .bind("status", StatusOperation(api))
        .expect("status binds its declared request and reply");
    table
}

struct StatusOperation(Api);

impl Operation for StatusOperation {
    type Request = Status;
    type Response = Value;

    fn required_access(
        &self,
        _: &Status,
        _: command_surface::Access,
        _: &command_surface::Caller,
    ) -> command_surface::Access {
        command_surface::Access::Read
    }

    fn execute(
        &self,
        _: Status,
        context: RequestContext,
    ) -> OperationFuture<'_, SurfaceAnswer<Value>> {
        Box::pin(async move {
            let Some(caller) = context.caller().identity::<Caller>() else {
                return SurfaceAnswer::error(command_surface::Failure::new(
                    command_surface::FailureKind::Refused,
                    "status needs an authenticated Sinex caller",
                ));
            };
            let api = self.0.clone();
            let caller = caller.clone();
            let remaining = context
                .read_deadline()
                .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()));
            // Database acquisition and statement cancellation use the same
            // accepted deadline as the common operation context.
            let reply =
                crate::request_deadline::run(remaining, async move { api.status(&caller).await })
                    .await;
            match reply {
                Ok(Reply::Ok { result }) => {
                    let mut answer = SurfaceAnswer::ok(result);
                    answer
                        .envelope
                        .notes
                        .push(sinex_protocol::DATA_NOTICE.into());
                    answer
                }
                Ok(Reply::Failed { failure } | Reply::StoppedEarly { failure, .. }) => {
                    SurfaceAnswer::error(failure)
                }
                Err(error) => SurfaceAnswer::error(error.failure()),
            }
        })
    }
}

struct RowsOperation(Api);

impl Operation for RowsOperation {
    type Request = sinex_protocol::rows::Rows;
    type Response = sinex_protocol::rows::RowsReply;

    fn required_access(
        &self,
        request: &Self::Request,
        _: command_surface::Access,
        _: &command_surface::Caller,
    ) -> command_surface::Access {
        if request
            .steps
            .iter()
            .any(|step| matches!(step, sinex_protocol::rows::Step::Save(_)))
        {
            crate::api::surface_access(
                sinex_protocol::command("save")
                    .expect("save declared")
                    .access,
            )
        } else {
            command_surface::Access::Read
        }
    }

    fn execute(
        &self,
        request: Self::Request,
        context: RequestContext,
    ) -> OperationFuture<'_, SurfaceAnswer<Self::Response>> {
        Box::pin(async move {
            let Some(caller) = context.caller().identity::<Caller>().cloned() else {
                return SurfaceAnswer::error(
                    Error::refused("rows need an authenticated Sinex caller").failure(),
                );
            };
            let api = self.0.clone();
            let metadata = context.clone();
            let remaining = context
                .read_deadline()
                .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()));
            match crate::request_deadline::run(remaining, async move {
                api.rows(request, &caller, &metadata).await
            })
            .await
            {
                Ok(answer) => answer,
                Err(error) => SurfaceAnswer::error(error.failure()),
            }
        })
    }
}

impl Api {
    async fn rows(
        &self,
        request: sinex_protocol::rows::Rows,
        caller: &Caller,
        metadata: &RequestContext,
    ) -> Result<SurfaceAnswer<sinex_protocol::rows::RowsReply>> {
        use sinex_protocol::rows::{
            ReadView, Rows, RowsReply, Step, StepOutcome, StepReply, StepReport,
        };
        request.validate()?;
        let mut context = self.context();
        let mut conn = self.acquire(caller).await?;
        let writes = request
            .steps
            .iter()
            .any(|step| matches!(step, Step::Save(_)));
        let reads = request.steps.is_empty()
            || request
                .steps
                .iter()
                .any(|step| matches!(step, Step::Read(_)));
        if writes && !caller.grants.split(',').any(|grant| grant == "guarded") {
            return Err(Error::refused("query authoring requires the guarded grant"));
        }
        let writer = if writes {
            Some(self.writer.as_ref().ok_or_else(Error::no_writer)?)
        } else {
            None
        };
        let _turn = if let Some(writer) = writer {
            Some(
                writer
                    .lock_request(metadata.caller().scope(), &metadata.request_id)
                    .await,
            )
        } else {
            None
        };
        let accepted = if let Some(writer) = writer {
            Some(
                writer
                    .begin_request(
                        metadata.caller().scope().into(),
                        metadata.request_id.clone(),
                        caller.actor.into(),
                        json!({"operation":metadata.request().operation,"arguments":metadata.request().arguments}),
                    )
                    .await?,
            )
        } else {
            None
        };
        let question = if let Some(accepted) = &accepted {
            if reads {
                let question = if let Some(question) = &accepted.question {
                    question.clone()
                } else {
                    let question = sinex_store::query::Question::accept(
                        &mut conn,
                        request.selection.as_deref().expect("save has a question"),
                        &context,
                    )
                    .await?;
                    sinex_store::requests::accept_question(&mut conn, accepted.operation, question)
                        .await?
                };
                context = question.context()?;
                Some(question)
            } else {
                None
            }
        } else if let Some(operation) = request.accepted {
            let question =
                sinex_store::requests::question(&mut conn, operation, metadata.caller().scope())
                    .await?;
            if request.selection.as_deref() != Some(question.text.as_str()) {
                return Err(Error::bad_request(
                    "accepted continuation belongs to another question",
                ));
            }
            context = question.context()?;
            Some(question)
        } else {
            None
        };
        let selected_reference = request
            .selection
            .as_deref()
            .and_then(|text| query::parse_reference(text).ok());
        let named = request.references.clone().or_else(|| {
            selected_reference
                .as_ref()
                .filter(|reference| !matches!(reference, query::Reference::Window { .. }))
                .map(|_| {
                    vec![
                        request
                            .selection
                            .clone()
                            .expect("parsed selection reference"),
                    ]
                })
        });
        let records;
        let mut references = Vec::new();
        let population;
        let position;
        if !reads {
            records = Vec::new();
            population = None;
            position = sinex_store::query::current_seq(&mut conn).await?;
        } else if let Some(named) = named {
            use sqlx::Connection;
            let mut snapshot = conn.begin().await?;
            sinex_store::query::read_one_snapshot(&mut snapshot).await?;
            position = sinex_store::query::current_seq(&mut snapshot).await?;
            let mut ids = Vec::with_capacity(named.len());
            for text in named {
                let reference =
                    query::parse_reference(&text).map_err(|error| error.failure(&text))?;
                let id = sinex_store::query::resolve(&mut snapshot, &reference).await?;
                references.push(format!("sinex:ev/{id}"));
                ids.push(id);
            }
            records = sinex_store::query::read_events(&mut snapshot, &ids, Some(position)).await?;
            snapshot.commit().await?;
            population = None;
        } else {
            let text = request.selection.as_deref().expect("validated selection");
            let window_query;
            let text = if matches!(selected_reference, Some(query::Reference::Window { .. })) {
                window_query = format!("recall {text}");
                window_query.as_str()
            } else {
                text
            };
            let page = Page {
                size: request.page_size,
                cursor: metadata.cursor().map(str::to_owned),
            };
            let selected = if let Some(question) = &question {
                question.read(&mut conn, &page).await
            } else {
                sinex_store::query::read_page(&mut conn, text, &context, &page).await
            }
            .map_err(|error| match error {
                sinex_store::Error::Query(error) => Error::from(error).in_query(text),
                error => Error::from(error),
            });
            let selected = match selected {
                Ok(selected) => selected,
                Err(error) => {
                    if let Some(accepted) = &accepted {
                        writer
                            .expect("write request")
                            .finish_request(accepted.operation, true)
                            .await?;
                        return retained_write_reply(&request, accepted, error.failure());
                    }
                    return Err(error);
                }
            };
            let page = selected.answer;
            position = page.resolved.commit_seq;
            context.now = page.resolved.now.parse()?;
            for id in selected.event_ids.into_iter().flatten() {
                references.push(format!("sinex:ev/{id}"));
            }
            records = page.rows.clone();
            population = Some(page);
        }
        let count = population
            .as_ref()
            .map_or(records.len() as u64, |page| page.total as u64);
        let shown = records.len() as u64;
        let mut reports = Vec::new();
        let mut next = Vec::new();
        let mut failed = false;
        for (index, step) in request.steps.iter().enumerate() {
            let Step::Read(read) = step else {
                let Step::Save(save) = step else {
                    unreachable!("declared step")
                };
                let recorded = accepted.as_ref().and_then(|accepted| accepted.step(index));
                let outcome = if let Some(recorded) = recorded {
                    let recorded: sinex_store::requests::CommittedStep =
                        serde_json::from_value(recorded.clone())?;
                    StepOutcome::Done {
                        reply: StepReply::Written(recorded.reply),
                    }
                } else if failed {
                    StepOutcome::NotRun
                } else {
                    let operation = accepted.as_ref().expect("write request accepted").operation;
                    match self
                        .query_record_step(
                            Request::Save {
                                name: save.name.clone(),
                                query: request.selection.clone().expect("save has a question"),
                                parameter: save.parameter.clone(),
                            },
                            caller,
                            operation,
                            index,
                            metadata,
                        )
                        .await
                    {
                        Ok(reply) => {
                            if request.steps[index + 1..]
                                .iter()
                                .any(|step| matches!(step, Step::Read(_)))
                            {
                                crate::request_deadline::handoff_read(&mut conn).await?;
                            }
                            StepOutcome::Done {
                                reply: StepReply::Written(reply),
                            }
                        }
                        Err(error) => {
                            failed = true;
                            StepOutcome::Failed {
                                error: error.failure(),
                            }
                        }
                    }
                };
                reports.push(StepReport {
                    verb: "save".into(),
                    outcome,
                });
                continue;
            };
            let outcome = if failed {
                StepOutcome::NotRun
            } else {
                let result: Result<StepReply> = async {
                    let at = read
                        .at
                        .as_ref()
                        .map(|at| at.parse::<i64>())
                        .transpose()
                        .map_err(|_| {
                            Error::bad_request("read --at needs an exact commit position")
                        })?
                        .unwrap_or(position);
                    match read.view {
                        ReadView::Full => {
                            if references.len() == records.len() {
                                let ids = references
                                    .iter()
                                    .map(|reference| {
                                        reference
                                            .strip_prefix("sinex:ev/")
                                            .ok_or_else(|| {
                                                Error::internal(
                                                    "selected event reference is malformed",
                                                )
                                            })?
                                            .parse::<uuid::Uuid>()
                                            .map_err(|error| Error::internal(error.to_string()))
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                Ok(StepReply::Records(
                                    sinex_store::query::read_events(&mut conn, &ids, Some(at))
                                        .await?,
                                ))
                            } else {
                                Ok(StepReply::Records(records.clone()))
                            }
                        }
                        ReadView::Trace => {
                            if references.len() != records.len() {
                                return Err(Error::bad_request(
                                    "trace needs event references, not grouped rows",
                                ));
                            }
                            let mut pages = Vec::new();
                            for reference in &references {
                                let text = format!("trace {reference}");
                                let page = sinex_store::query::read_at(
                                    &mut conn,
                                    &text,
                                    &context,
                                    &Page {
                                        size: read.page_size,
                                        cursor: read.cursor.clone(),
                                    },
                                    at,
                                )
                                .await?;
                                if let Some(cursor) = &page.page.cursor {
                                    let mut continued = read.clone();
                                    continued.cursor = Some(cursor.clone());
                                    continued.at = Some(at.to_string());
                                    let call = Rows {
                                        references: Some(vec![reference.clone()]),
                                        steps: vec![Step::Read(continued)],
                                        ..Rows::default()
                                    };
                                    next.push(read_continuation(metadata, call, None)?);
                                }
                                pages.push(page);
                            }
                            Ok(StepReply::Trace(pages))
                        }
                    }
                }
                .await;
                match result {
                    Ok(reply) => StepOutcome::Done { reply },
                    Err(error) => {
                        failed = true;
                        StepOutcome::Failed {
                            error: error.failure(),
                        }
                    }
                }
            };
            reports.push(StepReport {
                verb: "read".into(),
                outcome,
            });
        }
        if let Some(accepted) = &accepted {
            writer
                .expect("write request")
                .finish_request(accepted.operation, failed)
                .await?;
        }
        if let Some(cursor) = population
            .as_ref()
            .and_then(|page| page.page.cursor.as_ref())
        {
            let mut call = request.clone();
            // A page continues only the read, never an earlier write in the line.
            if question.is_some() {
                call.accepted = accepted
                    .as_ref()
                    .map(|accepted| accepted.operation)
                    .or(request.accepted);
            }
            call.steps = request
                .steps
                .iter()
                .filter(|step| matches!(step, Step::Read(_)))
                .cloned()
                .collect();
            next.push(read_continuation(metadata, call, Some(cursor.clone()))?);
        }
        let read_as = population.as_ref().map(|page| page.read_as.clone());
        let mut reply = RowsReply {
            population,
            references,
            steps: reports,
        };
        if caller.grants.split(',').any(|grant| grant == "sealed") {
            let mut values = Vec::new();
            if let Some(page) = &mut reply.population {
                values.extend(page.rows.iter_mut());
            }
            for report in &mut reply.steps {
                if let StepOutcome::Done { reply } = &mut report.outcome {
                    match reply {
                        StepReply::Records(records) => values.extend(records.iter_mut()),
                        StepReply::Written(_) => {}
                        StepReply::Trace(pages) => {
                            for page in pages {
                                values.extend(page.rows.iter_mut());
                            }
                        }
                    }
                }
            }
            self.unseal_values(values).await?;
        }
        let mut answer = if shown == 0 && !writes {
            SurfaceAnswer::empty(reply)
        } else {
            SurfaceAnswer::ok(reply)
        };
        answer
            .envelope
            .notes
            .push(sinex_protocol::DATA_NOTICE.into());
        if question
            .as_ref()
            .is_some_and(sinex_store::query::Question::reads_current_metadata)
        {
            answer.envelope.notes.push("Selected subjects and their order are retained; mutable unit values are read from their current state.".into());
        }
        answer.envelope.read_as = read_as;
        answer.envelope.count = Some(command_surface::Count::exact(count));
        answer.envelope.shown = Some(shown);
        answer.envelope.next = next
            .into_iter()
            .map(|request| command_surface::Next {
                label: "next".into(),
                request,
            })
            .collect();
        Ok(answer)
    }
}

/// A failed read on retry still reports every write that already committed.
fn retained_write_reply(
    request: &sinex_protocol::rows::Rows,
    accepted: &sinex_store::requests::Accepted,
    error: command_surface::Failure,
) -> Result<SurfaceAnswer<sinex_protocol::rows::RowsReply>> {
    use sinex_protocol::rows::{RowsReply, Step, StepOutcome, StepReply, StepReport};
    let first_read = request
        .steps
        .iter()
        .position(|step| matches!(step, Step::Read(_)));
    let mut reports = Vec::with_capacity(request.steps.len());
    for (index, step) in request.steps.iter().enumerate() {
        let (verb, outcome) = match step {
            Step::Read(_) if Some(index) == first_read => (
                "read",
                StepOutcome::Failed {
                    error: error.clone(),
                },
            ),
            Step::Read(_) => ("read", StepOutcome::NotRun),
            Step::Save(_) => {
                let outcome = if let Some(recorded) = accepted.step(index) {
                    let recorded: sinex_store::requests::CommittedStep =
                        serde_json::from_value(recorded.clone())?;
                    StepOutcome::Done {
                        reply: StepReply::Written(recorded.reply),
                    }
                } else {
                    StepOutcome::NotRun
                };
                ("save", outcome)
            }
        };
        reports.push(StepReport {
            verb: verb.into(),
            outcome,
        });
    }
    let mut answer = SurfaceAnswer::ok(RowsReply {
        population: None,
        references: Vec::new(),
        steps: reports,
    });
    answer
        .envelope
        .notes
        .push(sinex_protocol::DATA_NOTICE.into());
    Ok(answer)
}

fn read_continuation(
    metadata: &RequestContext,
    rows: sinex_protocol::rows::Rows,
    cursor: Option<String>,
) -> Result<command_surface::RunnableRequest> {
    let mut request = metadata.request().clone();
    request.operation = "rows".into();
    request.arguments = serde_json::to_value(rows)?;
    request.cursor = cursor;
    let identity = blake3::hash(&serde_json::to_vec(&(
        metadata.caller().scope(),
        metadata.caller().authorship(),
        &request.operation,
        &request.arguments,
        &request.cursor,
    ))?);
    request.request_id = format!("read:{}", identity.to_hex());
    Ok(request)
}

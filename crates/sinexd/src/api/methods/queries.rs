//! Query authoring and the small adapters that share the query engine.
use super::*;
use crate::{Error, Result};
use sinex_model::Timestamp;
use sinex_model::event_types::query::{Mark, QueryRecord, Saved};
use sinex_model::query_definition::SavedQuery;
use sinex_query::saved::parameter;
use std::collections::BTreeMap;

pub(super) async fn definitions(
    conn: &mut sqlx::PgConnection,
    requested: &str,
) -> Result<BTreeMap<String, SavedQuery>> {
    Ok(sinex_store::query::saved_definitions(conn, requested).await?)
}

impl Api {
    pub(super) async fn query_request(&self, caller: &Caller, request: Request) -> Result<Reply> {
        match request {
            Request::Query {
                query,
                page_size,
                cursor,
            } => {
                self.query(
                    caller,
                    &query,
                    Page {
                        size: page_size,
                        cursor,
                    },
                )
                .await
            }
            Request::Context {
                since,
                until,
                page_size,
                cursor,
            } => {
                self.context_view(
                    caller,
                    since,
                    until,
                    Page {
                        size: page_size,
                        cursor,
                    },
                )
                .await
            }
            Request::Timeline {
                query,
                page_size,
                cursor,
            } => {
                self.query(
                    caller,
                    &format!("events {}", query.unwrap_or_default()),
                    Page {
                        size: page_size,
                        cursor,
                    },
                )
                .await
            }
            Request::Search {
                words,
                page_size,
                cursor,
            } => {
                self.query(
                    caller,
                    &format!("events where {words}"),
                    Page {
                        size: page_size,
                        cursor,
                    },
                )
                .await
            }
            Request::Trace {
                reference,
                page_size,
                cursor,
            } => {
                self.query(
                    caller,
                    &format!("trace {reference}"),
                    Page {
                        size: page_size,
                        cursor,
                    },
                )
                .await
            }
            Request::QueryComplete { query } => {
                Ok(data(json!({"fields": query::complete(&query)})))
            }
            Request::QueryList => {
                let mut conn = self.acquire(caller).await?;
                Ok(data(
                    json!({"queries": sinex_store::query::saved_records(&mut conn).await?}),
                ))
            }
            request @ Request::Mark { .. } => self.mark(request, caller).await.map(data),
            _ => unreachable!("query request"),
        }
    }

    pub(super) async fn show(&self, caller: &Caller, request: Request) -> Result<Reply> {
        let Request::Show { reference } = request else {
            unreachable!("show request");
        };
        let reference =
            query::parse_reference(&reference).map_err(|error| error.failure(&reference))?;
        if let query::Reference::Window { from, to } = &reference {
            return self
                .query(
                    caller,
                    &format!("recall sinex:window/{from}/{to}"),
                    Page::default(),
                )
                .await;
        }
        let mut conn = self.acquire(caller).await?;
        Ok(data(
            json!({ "event": sinex_store::query::show(&mut conn, &reference).await? }),
        ))
    }

    async fn query_authoring_connection(&self, caller: &Caller) -> Result<Conn> {
        if !caller.grants.split(',').any(|grant| grant == "guarded") {
            return Err(Error::refused("query authoring requires the guarded grant"));
        }
        self.acquire(caller).await
    }

    async fn mark(&self, request: Request, caller: &Caller) -> Result<Value> {
        let Request::Mark {
            text,
            at,
            cwd,
            command,
            app,
            title,
            command_record,
            focus_record,
        } = request
        else {
            unreachable!("mark request");
        };
        let mut conn = self.query_authoring_connection(caller).await?;
        let now = Timestamp::now();
        let record = QueryRecord::Mark(Mark {
            command_record,
            focus_record,
            host: self.host.clone(),
            at: at.as_deref().map(timestamp).transpose()?.unwrap_or(now),
            text,
            cwd,
            command,
            app,
            title,
            by: caller.actor.into(),
            written_at: now,
        });
        self.admit_query_record(&mut conn, caller, record, now, None)
            .await
    }

    pub(super) async fn save_question(
        &self,
        query: &str,
        save: &sinex_protocol::rows::Save,
        caller: &Caller,
        operation: sinex_model::OperationId,
        index: usize,
        metadata: &command_surface::RequestContext,
    ) -> Result<Value> {
        let mut conn = self.query_authoring_connection(caller).await?;
        if let Some(saved) = sinex_store::requests::step(&mut conn, operation, index).await? {
            let saved: sinex_store::requests::CommittedStep = serde_json::from_value(saved)?;
            return Ok(saved.reply);
        }
        let now = Timestamp::now();
        let parameters = save
            .parameter
            .iter()
            .map(|value| parameter(value))
            .collect::<Result<Vec<_>, _>>()?;
        let mut definitions = if let Some(reference) = query::saved::reference_name(query) {
            definitions(&mut conn, &reference).await?
        } else {
            BTreeMap::new()
        };
        let context = self.context();
        let definition = SavedQuery {
            name: save.name.clone(),
            query: query.into(),
            template: query::template::parse(query, &context)
                .map_err(|error| error.failure(query))?,
            parameters: parameters.clone(),
        };
        definitions.insert(save.name.clone(), definition.clone());
        definitions.insert(
            format!("{}@{}", save.name, caller.actor),
            definition.clone(),
        );
        query::saved::validate_with(&definition, &context, &definitions)
            .map_err(|error| error.failure(query))?;
        let record = QueryRecord::Saved(Saved {
            template: definition.template,
            name: save.name.clone(),
            query: query.into(),
            parameters,
            by: caller.actor.into(),
            written_at: now,
        });
        self.admit_query_record(
            &mut conn,
            caller,
            record,
            now,
            Some((operation, index, metadata)),
        )
        .await
    }

    async fn admit_query_record(
        &self,
        conn: &mut sqlx::PgConnection,
        caller: &Caller,
        record: QueryRecord,
        now: Timestamp,
        step: Option<(
            sinex_model::OperationId,
            usize,
            &command_surface::RequestContext,
        )>,
    ) -> Result<Value> {
        let writer = self.writer.as_ref().ok_or_else(Error::no_writer)?;
        let acquired =
            sinex_sources::sources::query::request_material(record, caller.actor, &self.host, now);
        let material = acquired
            .material
            .as_ref()
            .map(|material| material.id)
            .ok_or_else(|| Error::internal("query acquisition has no material"))?;
        let source = std::sync::Arc::new(sinex_sources::sources::query::request_source());
        let binding = format!("query.request:{}@{}", caller.actor, self.host);
        let done = if let Some((operation, index, metadata)) = step {
            metadata.finish_read_phase()?;
            crate::submit::request_step(
                conn,
                writer,
                &self.blobs,
                source,
                &binding,
                &self.host,
                acquired,
                crate::writer::RequestStep {
                    operation,
                    index,
                    response: std::sync::Arc::new(move |outcome| authored_reply(material, outcome)),
                },
            )
            .await?
        } else {
            crate::submit::request(
                conn,
                writer,
                &self.blobs,
                source,
                &binding,
                &self.host,
                acquired,
            )
            .await?
        };
        if let Some(reply) = done.reply {
            return Ok(reply);
        }
        authored_reply(material, &done.outcome)
    }

    pub(super) async fn context_view(
        &self,
        caller: &Caller,
        since: Option<String>,
        until: Option<String>,
        page: Page,
    ) -> Result<Reply> {
        let mut conn = self.acquire(caller).await?;
        let ctx = super::recall::context_for_page(&mut conn, &self.context(), &page).await?;
        drop(conn);
        let end = match until {
            Some(s) => query::time::instant(&ctx, &s)
                .ok_or_else(|| Error::bad_value("context until requires an absolute timestamp"))?,
            None => ctx.now,
        };
        let since = since.unwrap_or_else(|| "2h".into());
        let start = if let Some(seconds) = query::time::duration(&since) {
            end.checked_sub(jiff::SignedDuration::from_secs_f64(seconds))?
        } else {
            query::time::instant(&ctx, &since).ok_or_else(|| {
                Error::bad_value("context since requires a duration or absolute timestamp")
            })?
        };
        if start >= end {
            return Err(Error::bad_value("context end must follow its start"));
        }
        // The absolute window and its continuation share this clock.
        self.recall_in(
            caller,
            &format!("recall sinex:window/{start}/{end}"),
            page,
            ctx,
        )
        .await
    }
}

/// Admission supplies the exact identity even if a later write archives it.
fn authored_reply(
    material: sinex_model::MaterialId,
    outcome: &sinex_store::Outcome,
) -> Result<Value> {
    if outcome.admitted != 1 || outcome.failures != 0 {
        return Err(Error::internal(
            "query request was not admitted as one event",
        ));
    }
    let [id] = outcome.admitted_ids.as_slice() else {
        return Err(Error::internal(
            "query admission did not return its one event identity",
        ));
    };
    Ok(json!({"event":id,"citation":format!("sinex:record/{material}/0"),"material":material}))
}

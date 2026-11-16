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
            request @ (Request::Save { .. } | Request::Mark { .. }) => {
                self.query_record(request, caller).await.map(data)
            }
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

    pub(super) async fn query_record(&self, request: Request, caller: &Caller) -> Result<Value> {
        if !caller.grants.split(',').any(|g| g == "guarded") {
            return Err(Error::refused("query authoring requires the guarded grant"));
        }
        let mut conn = self.acquire(caller).await?;
        let now = Timestamp::now();
        let record = match request {
            Request::Mark {
                text,
                at,
                cwd,
                command,
                app,
                title,
                command_record,
                focus_record,
            } => QueryRecord::Mark(Mark {
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
            }),
            Request::Save {
                name,
                query,
                parameter: declarations,
            } => {
                let parameters = declarations
                    .iter()
                    .map(|s| parameter(s))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut definitions = if let Some(reference) = query::saved::reference_name(&query)
                {
                    definitions(&mut conn, &reference).await?
                } else {
                    BTreeMap::new()
                };
                let definition = SavedQuery {
                    name: name.clone(),
                    query: query.clone(),
                    template: query::template::parse(&query, &self.context())
                        .map_err(|error| error.failure(&query))?,
                    parameters: parameters.clone(),
                };
                definitions.insert(name.clone(), definition.clone());
                definitions.insert(format!("{name}@{}", caller.actor), definition.clone());
                query::saved::validate_with(&definition, &self.context(), &definitions)
                    .map_err(|error| error.failure(&query))?;
                QueryRecord::Saved(Saved {
                    template: definition.template,
                    name,
                    query,
                    parameters,
                    by: caller.actor.into(),
                    written_at: now,
                })
            }
            _ => return Err(Error::bad_request("not a query authoring command")),
        };
        let writer = self.writer.as_ref().ok_or_else(Error::no_writer)?;
        let acquired =
            sinex_sources::sources::query::request_material(record, caller.actor, &self.host, now);
        let done = crate::submit::request(
            &mut conn,
            writer,
            &self.blobs,
            std::sync::Arc::new(sinex_sources::sources::query::request_source()),
            &format!("query.request:{}@{}", caller.actor, self.host),
            &self.host,
            acquired,
        )
        .await?;
        if done.outcome.admitted != 1 || done.outcome.failures != 0 {
            return Err(Error::internal(
                "query request was not admitted as one event",
            ));
        }
        let material = done
            .material
            .ok_or_else(|| Error::internal("query admission has no material"))?;
        let id = sinex_store::live::at(
            &mut conn,
            sinex_model::RecordLocation {
                material,
                record_offset: 0,
            },
            0,
        )
        .await?
        .ok_or_else(|| Error::conflict("query record was withdrawn before its response"))?;
        Ok(json!({"event":id,"citation":format!("sinex:record/{material}/0"),"material":material}))
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

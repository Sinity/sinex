//! Query authoring and the small adapters that share the ordinary query engine.
use super::*;
use crate::{Error, Result};
use sinex_model::Timestamp;
use sinex_model::event_types::query::{Mark, QueryRecord, Saved};
use sinex_model::query::saved::{SavedQuery, parameter};
use std::collections::BTreeMap;

pub(super) async fn list(conn: &mut sqlx::PgConnection) -> Result<Vec<Saved>> {
    Ok(sinex_store::query::saved_records(conn).await?)
}

pub(super) async fn definitions(
    conn: &mut sqlx::PgConnection,
    requested: &str,
) -> Result<BTreeMap<String, SavedQuery>> {
    Ok(sinex_store::query::saved_definitions(conn, requested).await?)
}

impl Api {
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
            Request::QuerySave {
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
                    template: query::native::parse(&query, &self.context())
                        .map_err(|error| error.failure(&query))?,
                    parameters: parameters.clone(),
                };
                definitions.insert(name.clone(), definition.clone());
                definitions.insert(format!("{name}@{}", caller.actor), definition.clone());
                definition
                    .validate_with(&self.context(), &definitions)
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
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| Error::not_configured("query authoring requires sinexd run"))?;
        let acquired =
            sinex_sources::sources::query::request_material(record, caller.actor, &self.host, now);
        let done = crate::intake::request(
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
                anchor: 0,
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

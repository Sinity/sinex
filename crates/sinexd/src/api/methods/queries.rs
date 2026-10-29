//! Query authoring and the small adapters that share the ordinary query engine.
use super::*;
use sinex_model::Timestamp;
use sinex_model::event_types::query::{Mark, QueryRecord, Saved};
use sinex_model::query::saved::{SavedQuery, parameter};
use std::collections::BTreeMap;

fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}

pub(super) async fn list(conn: &mut sqlx::PgConnection) -> Result<Vec<Saved>, String> {
    sinex_store::query::saved_records(conn).await.map_err(error)
}

pub(super) async fn definitions(
    conn: &mut sqlx::PgConnection,
    requested: &str,
) -> Result<BTreeMap<String, SavedQuery>, String> {
    sinex_store::query::saved_definitions(conn,requested).await.map_err(error)
}

impl Api {
    pub(super) async fn query_record(
        &self,
        request: Request,
        caller: &Caller,
    ) -> Result<Value, String> {
        if !caller.grants.split(',').any(|g| g == "guarded") {
            return Err("query authoring requires the guarded grant".into());
        }
        let mut conn = self.acquire(caller).await.map_err(error)?;
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
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(error)?;
                let mut definitions = if let Some(reference) = query::saved::reference_name(&query)
                {
                    definitions(&mut conn, &reference).await?
                } else {
                    BTreeMap::new()
                };
                let definition = SavedQuery {
                    name: name.clone(),
                    query: query.clone(),
                    parameters: parameters.clone(),
                };
                definitions.insert(name.clone(), definition.clone());
                definitions.insert(format!("{name}@{}", caller.actor), definition.clone());
                definition
                    .validate_with(&self.context(), &definitions)
                    .map_err(error)?;
                QueryRecord::Saved(Saved {
                    name,
                    query,
                    parameters,
                    by: caller.actor.into(),
                    written_at: now,
                })
            }
            _ => return Err("not a query authoring command".into()),
        };
        let writer = self
            .writer
            .as_ref()
            .ok_or("query authoring requires sinexd run")?;
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
            return Err("query request was not admitted as one event".into());
        }
        let material = done.material.ok_or("query admission has no material")?;
        let id = sinex_store::live::at(
            &mut conn,
            sinex_model::RecordLocation {
                material,
                anchor: 0,
            },
            0,
        )
        .await
        .map_err(error)?
        .ok_or("query record was withdrawn before its response")?;
        Ok(json!({"event":id,"citation":format!("sinex:record/{material}/0"),"material":material}))
    }

    pub(super) async fn context_view(
        &self,
        caller: &Caller,
        since: Option<String>,
        until: Option<String>,
        page: Page,
    ) -> Reply {
        let question=json!({"surface":"context","since":since,"until":until});
        let mut conn=match self.acquire(caller).await {Ok(c)=>c,Err(e)=>return refused(e.to_string())};
        let ctx=match super::recall::context_for_page(&mut conn,&self.context(),&page,&question,&caller.grants).await {
            Ok(ctx)=>ctx,Err(e)=>return refused(e),
        };
        drop(conn);
        let end = match until {
            Some(s) => match query::time::instant(&ctx, &s) {
                Some(t) => t,
                None => return refused("context until requires an absolute timestamp"),
            },
            None => ctx.now,
        };
        let since = since.unwrap_or_else(|| "2h".into());
        let start = if let Some(seconds) = query::time::duration(&since) {
            match end.checked_sub(jiff::SignedDuration::from_secs_f64(seconds)) {
                Ok(t) => t,
                Err(e) => return refused(error(e)),
            }
        } else {
            match query::time::instant(&ctx, &since) {
                Some(t) => t,
                None => return refused("context since requires a duration or absolute timestamp"),
            }
        };
        if start >= end {
            return refused("context end must follow its start");
        }
        self.recall_context(caller,&format!("recall sinex:window/{start}/{end}"),page,question).await
    }
}

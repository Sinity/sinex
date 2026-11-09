use super::*;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sinex_model::query::cursor::{OrderPosition, compare_positions};
use sqlx::Connection;

#[derive(Serialize, Deserialize)]
struct Continuation {
    kind: Option<String>,
    fields: bool,
    declarations: Vec<(sinex_model::KindRef, String)>,
}

impl Api {
    pub(super) async fn kind_fields(
        &self,
        caller: &Caller,
        kind: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
        fields: bool,
    ) -> Result<Reply> {
        Ok(Reply::Ok {
            result: self
                .kind_fields_page(caller, kind, page, cursor, fields)
                .await?,
        })
    }

    async fn kind_fields_page(
        &self,
        caller: &Caller,
        kind: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
        fields: bool,
    ) -> Result<Value> {
        let size = usize::try_from(page.unwrap_or(50))
            .map_err(|_| Error::bad_request("page size is out of range"))?;
        if size == 0 {
            return Err(Error::bad_request("page size must be positive"));
        }
        let mut conn = self.acquire(caller).await?;
        let mut tx = conn.begin().await?;
        sinex_store::query::read_one_snapshot(&mut tx).await?;
        let ctx = self.context();
        let seal = sinex_store::query::continuation::seal_key(&mut tx).await?;
        let question = sinex_store::query::continuation::identity(
            "declarations",
            &json!([kind, fields]),
            &caller.grants,
        )?;
        let prior = cursor
            .as_deref()
            .map(|raw| {
                sinex_store::query::continuation::open(&seal, raw, question, &ctx)
                    .map_err(Error::from)
            })
            .transpose()?;
        let saved: Option<Continuation> = prior
            .as_ref()
            .map(|prior| {
                serde_json::from_str(
                    prior
                        .context
                        .versions
                        .get("sinex.declarations")
                        .ok_or_else(|| Error::bad_request("invalid declaration cursor"))?,
                )
                .map_err(Error::from)
            })
            .transpose()?;
        let declarations = match &saved {
            Some(saved) => {
                if saved.kind != kind || saved.fields != fields {
                    return Err(Error::bad_request(
                        "declaration cursor belongs to another request",
                    ));
                }
                let refs: Vec<_> = saved.declarations.iter().map(|d| d.0.clone()).collect();
                let found = sinex_schema::kinds::load(&mut tx, &refs).await?;
                if saved
                    .declarations
                    .iter()
                    .any(|(r, h)| found.get(r).is_none_or(|d| d.definition_hash() != *h))
                {
                    return Err(Error::bad_request("declaration snapshot is unavailable"));
                }
                found
            }
            None => {
                let mut found = sinex_schema::kinds::all(&mut tx).await?;
                if let Some(name) = &kind {
                    found.retain(|r, _| &r.name == name);
                }
                found
            }
        };
        let mut rows = vec![];
        for declaration in declarations.values() {
            if fields {
                for field in sinex_model::query::QueryCatalog::new([declaration.clone()])
                    .fields
                    .into_values()
                {
                    rows.push(json!({"kind":declaration.kind,"field":field}));
                }
            } else {
                rows.push(json!({"declaration":declaration,"hash":declaration.definition_hash()}));
            }
        }
        let identity = |row: &Value| -> Result<String> {
            let reference = if fields {
                &row["kind"]
            } else {
                &row["declaration"]["kind"]
            };
            let name = reference["name"]
                .as_str()
                .ok_or_else(|| Error::bad_request("missing kind name"))?;
            let version = reference["version"]
                .as_i64()
                .ok_or_else(|| Error::bad_request("missing kind version"))?;
            Ok(format!(
                "{name}@{version:020}/{}",
                if fields {
                    row["field"]["path"].to_string()
                } else {
                    String::new()
                }
            ))
        };
        let mut rows = rows
            .into_iter()
            .map(|row| identity(&row).map(|id| (id, row)))
            .collect::<Result<Vec<_>, _>>()?;
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let total = rows.len();
        let mut chosen = Vec::new();
        let mut last = None;
        let mut more = false;
        for (id, row) in rows {
            let here = OrderPosition {
                values: vec![],
                row_identity: id,
            };
            if let Some(prior) = &prior
                && !compare_positions(&here, &prior.after, &[])
                    .map_err(sinex_model::query::QueryError::from)?
                    .is_gt()
            {
                continue;
            }
            if chosen.len() == size {
                more = true;
                break;
            }
            chosen.push(row);
            last = Some(here);
        }
        let next = if more {
            let metadata = Continuation {
                kind,
                fields,
                declarations: declarations
                    .values()
                    .map(|d| (d.kind.clone(), d.definition_hash()))
                    .collect(),
            };
            Some(sinex_store::query::continuation::encode(
                &seal,
                question,
                &ctx,
                std::collections::BTreeMap::new(),
                std::collections::BTreeMap::from([(
                    "sinex.declarations".into(),
                    serde_json::to_string(&metadata)?,
                )]),
                last.ok_or_else(|| Error::bad_request("declaration page did not advance"))?,
            )?)
        } else {
            None
        };
        let result = json!({if fields {"fields"}else{"kinds"}:chosen,"total":total,"cursor":next});
        tx.commit().await?;
        Ok(result)
    }
}

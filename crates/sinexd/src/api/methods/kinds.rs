use super::*;
use serde::{Deserialize, Serialize};
use sinex_model::query::cursor as encoding;
use sqlx::Connection;

#[derive(Serialize, Deserialize)]
struct Continuation {
    kind: Option<String>,
    fields: bool,
    declarations: Vec<(sinex_model::KindRef, String)>,
    offset: usize,
}

impl Api {
    pub(super) async fn kind_fields(
        &self,
        caller: &Caller,
        kind: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
        fields: bool,
    ) -> Reply {
        match self
            .kind_fields_page(caller, kind, page, cursor, fields)
            .await
        {
            Ok(result) => Reply::Ok { result },
            Err(reason) => refused(reason),
        }
    }

    async fn kind_fields_page(
        &self,
        caller: &Caller,
        kind: Option<String>,
        page: Option<u64>,
        cursor: Option<String>,
        fields: bool,
    ) -> Result<Value, String> {
        let size = usize::try_from(page.unwrap_or(50)).map_err(|_| "page size is out of range")?;
        if size == 0 {
            return Err("page size must be positive".into());
        }
        let mut conn = self.acquire(caller).await.map_err(|e| e.to_string())?;
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
        sinex_store::query::read_one_snapshot(&mut tx)
            .await
            .map_err(|e| e.to_string())?;
        let saved = cursor
            .map(|raw| {
                encoding::decode::<Continuation>(&raw).map_err(|_| "invalid declaration cursor")
            })
            .transpose()?;
        let declarations = match &saved {
            Some(saved) => {
                if saved.kind != kind || saved.fields != fields {
                    return Err("declaration cursor belongs to another request".into());
                }
                let refs: Vec<_> = saved.declarations.iter().map(|d| d.0.clone()).collect();
                let found = sinex_schema::kinds::load(&mut tx, &refs)
                    .await
                    .map_err(|e| e.to_string())?;
                if saved
                    .declarations
                    .iter()
                    .any(|(r, h)| found.get(r).is_none_or(|d| d.definition_hash() != *h))
                {
                    return Err("declaration snapshot is unavailable".into());
                }
                found
            }
            None => {
                let mut found = sinex_schema::kinds::all(&mut tx)
                    .await
                    .map_err(|e| e.to_string())?;
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
        let offset = saved.as_ref().map_or(0, |c| c.offset);
        if offset > rows.len() {
            return Err("declaration cursor is out of range".into());
        }
        let end = offset.saturating_add(size).min(rows.len());
        let next = if end < rows.len() {
            let saved = Continuation {
                kind,
                fields,
                offset: end,
                declarations: declarations
                    .values()
                    .map(|d| (d.kind.clone(), d.definition_hash()))
                    .collect(),
            };
            Some(encoding::encode(&saved).map_err(|e| e.to_string())?)
        } else {
            None
        };
        let result = json!({if fields {"fields"} else {"kinds"}:rows[offset..end],"total":rows.len(),"cursor":next});
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(result)
    }
}

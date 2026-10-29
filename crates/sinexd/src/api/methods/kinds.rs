use super::*;
use serde::{Deserialize, Serialize};
use sinex_model::query::cursor::{OrderPosition,compare_positions};
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
        let ctx=self.context();
        let seal=sinex_store::query::continuation::seal_key(&mut tx).await.map_err(|e|e.to_string())?;
        let question=sinex_store::query::continuation::identity("declarations",&json!([kind,fields]),&caller.grants).map_err(|e|e.to_string())?;
        let prior=cursor.as_deref().map(|raw|sinex_store::query::continuation::open(&seal,raw,question,&ctx).map_err(|e|e.to_string())).transpose()?;
        let saved:Option<Continuation>=prior.as_ref().map(|prior|{
            serde_json::from_str(prior.context.versions.get("sinex.declarations").ok_or("invalid declaration cursor")?).map_err(|e|e.to_string())
        }).transpose()?;
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
        let identity=|row:&Value|->Result<String,String>{
            let reference=if fields {&row["kind"]}else{&row["declaration"]["kind"]};
            let name=reference["name"].as_str().ok_or("missing kind name")?;
            let version=reference["version"].as_i64().ok_or("missing kind version")?;
            Ok(format!("{name}@{version:020}/{}",if fields {row["field"]["path"].to_string()}else{String::new()}))
        };
        let mut rows=rows.into_iter().map(|row|identity(&row).map(|id|(id,row))).collect::<Result<Vec<_>,_>>()?;
        rows.sort_by(|a,b|a.0.cmp(&b.0));
        let total=rows.len();let mut chosen=Vec::new();let mut last=None;let mut more=false;
        for (id,row) in rows {
            let here=OrderPosition{values:vec![],row_identity:id};
            if let Some(prior)=&prior && !compare_positions(&here,&prior.after,&[]).map_err(|e|e.to_string())?.is_gt(){continue;}
            if chosen.len()==size {more=true;break;}
            chosen.push(row);last=Some(here);
        }
        let next=if more {
            let metadata=Continuation{kind,fields,declarations:declarations.values().map(|d|(d.kind.clone(),d.definition_hash())).collect()};
            Some(sinex_store::query::continuation::encode(&seal,question,&ctx,std::collections::BTreeMap::new(),
                std::collections::BTreeMap::from([("sinex.declarations".into(),serde_json::to_string(&metadata).map_err(|e|e.to_string())?)]),
                last.ok_or("declaration page did not advance")?).map_err(|e|e.to_string())?)
        }else{None};
        let result=json!({if fields {"fields"}else{"kinds"}:chosen,"total":total,"cursor":next});
        tx.commit().await.map_err(|e| e.to_string())?;
        Ok(result)
    }
}

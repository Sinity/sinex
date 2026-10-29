//! Running queries: the language compiled to SQL, provenance walks, recall,
//! `show`, and read-only SQL. Each page uses one read-only repeatable snapshot.
//! Continuations keep the original question, clock and event position; a
//! changed visible ordered answer requires a fresh query. Rows use the table's
//! field names; times are UTC.
mod compile;
mod cursor;
pub mod continuation;
mod fields;
mod follow;
mod recall;
mod selected;
pub mod signals;
mod saved;
mod products;
pub use products::{current_grants, key_dirty, live_payload,event_payload};
pub use saved::{saved_records, saved_definitions};
pub use selected::{SelectedAnswer, resolve_recall, selected_query, selected_query_in_snapshot, check_selected_question, selected_page};
mod source_views;
pub(crate) mod sql;
mod walk;

pub mod read_sql;

use crate::{Error, Result};
use compile::Compiled;
use compile::calendar::compile as compile_query;
pub use compile::calendar::initialize as initialize_calendar;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sinex_model::query::{
    AsOf, Context, Expr, FieldRef, Operand, Query, Source, Stage, Table, read_as,
};
use sql::SqlFragment;
use sqlx::{Connection, PgConnection};

pub use cursor::context_for_page;
pub use follow::{follow_position, follow_query};
pub use walk::{resolve, show};

/// Which page of an answer to return.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Page {
    /// Rows per page; 50 when not given. The page is not the answer: every
    /// row is reachable through the cursor.
    pub size: Option<u64>,
    /// The cursor from the previous page.
    pub cursor: Option<String>,
}

pub const DEFAULT_PAGE: u64 = 50;

/// One page of an answer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Answer {
    pub table: String,
    pub query: String,
    pub read_as: String,
    pub resolved: Resolved,
    pub columns: Vec<String>,
    pub rows: Vec<Json>,
    /// Rows in the whole answer.
    pub total: i64,
    pub page: PageInfo,
    /// What a recall found per source, and which sources had no data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recall: Option<Json>,
}

/// What the query used: `now`, the zone, the day start and the commit
/// position it read at.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Resolved {
    pub now: String,
    pub zone: String,
    pub day_start: String,
    pub commit_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PageInfo {
    pub size: u64,
    /// `None` on the last page.
    pub cursor: Option<String>,
}

/// The newest commit position.
pub async fn current_seq(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
            .fetch_one(&mut *conn)
            .await?,
    )
}

/// Let this session see the classes in `grants` (comma-separated) through the
/// visibility views.
pub async fn set_grants(conn: &mut PgConnection, grants: &str) -> sqlx::Result<()> {
    sqlx::query_scalar!("SELECT set_config('sinex.grants', $1, false)", grants)
        .fetch_one(&mut *conn)
        .await?;
    Ok(())
}

/// Make the transaction just begun on `conn` read-only, with every statement
/// seeing the same snapshot.
pub async fn read_one_snapshot(conn: &mut PgConnection) -> Result<()> {
    sqlx::query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Parse and bind one question against this transaction's declarations. A
/// supplied query is a programmatic query (including follow's commit filter).
pub async fn run(
    conn: &mut PgConnection,
    text: &str,
    query: Option<&Query>,
    ctx: &Context,
    page: &Page,
) -> Result<Answer> {
    run_inner(conn, text, query, ctx, page, None, false, false)
        .await
        .map(|selected| selected.answer)
}

#[allow(clippy::too_many_arguments)]
async fn run_inner(
    conn: &mut PgConnection,
    text: &str,
    query: Option<&Query>,
    ctx: &Context,
    page: &Page,
    retained: Option<&sinex_model::event_types::analysis::QuerySelection>,
    capture_selection: bool,
    in_snapshot: bool,
) -> Result<selected::SelectedPage> {
    if !in_snapshot {initialize_calendar(conn).await?;}
    let mut tx = conn.begin().await?;
    if !in_snapshot { read_one_snapshot(&mut tx).await?; }
    sqlx::query!("SET LOCAL TimeZone = 'UTC'")
        .execute(&mut *tx)
        .await?;
    let size = page.size.unwrap_or(DEFAULT_PAGE).max(1);
    if size > i64::MAX as u64 {
        return Err(Error::Invalid("page size is out of range".into()));
    }
    let mut ctx=ctx.clone();
    if ctx.catalog.is_none() {ctx.catalog=Some(sinex_model::query::QueryCatalog::new(sinex_schema::kinds::all(&mut tx).await?.into_values()));}
    let grants=current_grants(&mut tx).await?;
    let question_id=continuation::identity("query",&continuation::query_identity(text,&ctx)?,&grants)?;
    let seal=continuation::seal_key(&mut tx).await?;
    let pinned=page.cursor.as_deref().map(|raw|cursor::Cursor::decode(&seal,raw,question_id,&ctx)).transpose()?;
    if let Some(pinned)=&pinned {ctx.now=pinned.ctx.now;}
    let declarations = match &pinned {
        Some(cursor) => {
            let refs: Vec<_> = cursor.declarations.iter().map(|d| d.0.clone()).collect();
            let declarations = sinex_schema::kinds::load(&mut tx, &refs).await?;
            if cursor.declarations.iter().any(|(r, h)| {
                declarations
                    .get(r)
                    .is_none_or(|d| d.definition_hash() != *h)
            }) {
                return Err(Error::Invalid(
                    "query declaration snapshot is unavailable".into(),
                ));
            }
            declarations
        }
        None if retained.is_some() => {
            let expected: Vec<(sinex_model::KindRef, String)> =
                serde_json::from_value(retained.expect("retained context").declarations.clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            let refs: Vec<_> = expected
                .iter()
                .map(|(reference, _)| reference.clone())
                .collect();
            let declarations = sinex_schema::kinds::load(&mut tx, &refs).await?;
            if expected.iter().any(|(reference, hash)| {
                declarations
                    .get(reference)
                    .is_none_or(|declaration| declaration.definition_hash() != *hash)
            }) {
                return Err(Error::Invalid(
                    "query declaration snapshot is unavailable".into(),
                ));
            }
            declarations
        }
        None => sinex_schema::kinds::all(&mut tx).await?,
    };
    ctx.catalog = Some(sinex_model::query::QueryCatalog::new(
        declarations.into_values(),
    ));
    let programmatic=query.and_then(|q|{
        let parsed=sinex_model::query::bind(text,&ctx).ok();
        (parsed.as_ref()!=Some(q)).then(||format!("{q:?}"))
    });
    let mut saved_context = None;
    let mut bound = match query {
        Some(query) => query.clone(),
        None => if let Some(name) = sinex_model::query::saved::reference_name(text) {
            let definitions = if let Some(snapshot) = retained.and_then(|retained| retained.selected.get("_saved")) {
                serde_json::from_value(snapshot.clone()).map_err(|error| Error::Invalid(error.to_string()))?
            } else { saved::saved_definitions(&mut tx,&name).await? };
            let bound = sinex_model::query::saved::bind_saved(text,&ctx,&definitions)?;
            saved_context = Some(definitions);
            bound
        } else {sinex_model::query::bind(text,&ctx)?},
    };
    ctx.catalog
        .as_ref()
        .expect("loaded catalog")
        .bind(&mut bound, &ctx)
        .map_err(Error::Query)?;
    let query = &bound;
    let scope = sqlx::query!(
        r#"SELECT current_user::text AS "user!", coalesce(current_setting('sinex.grants', true), '') AS "grants!""#,
    )
    .fetch_one(&mut *tx)
    .await?;
    let scope = (scope.user, scope.grants);
    let question = cursor::question(text,programmatic,saved_context.as_ref(),&ctx,&scope)?;
    let current = current_seq(&mut tx).await?;
    let prior=pinned;
    let mut seq=match &prior {
        Some(cursor)=>{cursor.check_question(&question)?;if cursor.seq>current {return Err(Error::Invalid("query cursor is ahead of the archive".into()));}cursor.seq},
        None=>current,
    };
    if page.cursor.is_none() {
        for stage in &query.stages {
            match stage {
                Stage::AsOf(AsOf::Seq(s)) => seq = *s,
                Stage::AsOf(AsOf::Time(t)) => seq = seq_at(&mut tx, *t).await?,
                _ => {}
            }
        }
    }
    if page.cursor.is_none()
        && let Some(position) = retained.and_then(|retained| retained.read_as)
    {
        seq = position;
    }
    if seq < 0 || seq > current {
        return Err(Error::Invalid(
            "as-of position is outside the known archive".into(),
        ));
    }
    if let Some(reference)=query.similar_reference()? {crate::vectors::validate_reference(&mut tx,reference,seq).await?;}
    if matches!(query.source, Source::Walk { .. })
        && (query.filter.is_some() || !query.stages.is_empty())
    {
        return Err(Error::Invalid(
            "provenance walks do not support query stages; use a separate event query".into(),
        ));
    }
    let availability=availability(&mut tx,query).await?;
    let mut read = ReadPage {
        question,
        ctx,
        seq,
        size,
        prior,
        seal,
        question_id,
        availability,
    };
    let table = match &query.source {
        Source::Table { alias: Some(a), .. } => a.name.to_string(),
        Source::Table { table, .. } => table.name().to_string(),
        Source::Walk { walk, .. } => format!("{walk:?}").to_lowercase(),
        Source::Recall { .. } | Source::RecallLast { .. } | Source::RecallMark { .. } => {
            "recall".to_string()
        }
    };
    let mut captured = None;
    let mut selected_parents = Vec::new();
    let mut selected_class = sinex_model::Class::Open;
    let mut answer = match &query.source {
        Source::Walk { walk, reference } => {
            let rows = walk::walk(&mut tx, *walk, reference).await?;
            read.availability["graph"]=serde_json::json!(cursor::digest(&serde_json::json!(rows))?);
            read.check_availability()?;
            let total = rows.len() as i64;
            let columns = rows
                .first()
                .and_then(Json::as_object)
                .map(|o| o.keys().cloned().collect())
                .unwrap_or_default();
            let reading = read_as(query);
            use continuation::{Position,Direction,compare_positions};
            let position=|row:&Json|Position {values:vec![row["depth"].clone(),if matches!(query.source,Source::Walk{walk:sinex_model::query::Walk::Trace,..}){Json::Null}else{row["when"].clone()}],row_identity:row["id"].as_str().unwrap_or("").to_owned()};
            let directions=[Direction::Ascending,Direction::Ascending];
            let mut chosen=Vec::new();let mut more=false;
            for row in rows {
                if let Some(prior)=&read.prior && !compare_positions(&position(&row),&prior.after,&directions).map_err(|e|Error::Invalid(e.to_string()))?.is_gt(){continue;}
                if chosen.len() as u64==read.size {more=true;break;}chosen.push(row);
            }
            let last=chosen.last().map(|row|(position(row),vec!["numeric".into(),"timestamp with time zone".into()]));
            Answer{table,query:text.into(),read_as:reading,resolved:read.resolved(),columns,total,page:read.info(total,more,last)?,rows:chosen,recall:None}
        }
        Source::Table { .. } => {
            read.check_availability()?;
            let rendered = render_page(
                compile_query(&mut tx,&read.ctx,read.seq,query).await?,
                &read,
            )?;
            page_of(&mut tx, rendered, table, text, query, &read, None).await?
        }
        Source::Recall { .. } | Source::RecallLast { .. } | Source::RecallMark { .. } => {
            read.check_availability()?;
            let selection = match retained {
                Some(retained) => recall::retained(&mut tx, retained, &read.ctx, read.seq).await?,
                None => recall::select(&mut tx, &query.source, &read.ctx, read.seq).await?,
            };
            let as_events = Query {
                source: Source::Table {
                    table: Table::Events,
                    alias: None,
                },
                filter: Some(Expr::And(
                    Box::new(selection.filter()),
                    Box::new(recall::input_filter()),
                )),
                stages: query.stages.clone(),
                ranked: query.ranked,
            };
            let rendered = render_page(
                compile_query(&mut tx,&read.ctx,read.seq,&as_events).await?,
                &read,
            )?;
            let account = selection
                .account(&mut tx, read.seq, &as_events, &read.ctx)
                .await?;
            if capture_selection {
                let (context, parents, class) = selected::capture(
                    &mut tx, text, query, &read.ctx, read.seq, &scope.1, &selection, &as_events, &account,
                )
                .await?;
                let mut context = context;
                if let Some(definitions) = &saved_context { context.selected["_saved"] = serde_json::to_value(definitions).map_err(|error| Error::Invalid(error.to_string()))?; }
                captured = Some(context);
                selected_parents = parents;
                selected_class = class;
            }
            page_of(&mut tx, rendered, table, text, query, &read, Some(account)).await?
        }
    };
    answer.read_as = read_as(query);
    tx.commit().await?;
    Ok(selected::SelectedPage {
        answer,
        selection: captured,
        parents: selected_parents,
        class: selected_class,
    })
}

/// Current policies can make an otherwise reconstructable read unavailable.
/// Mutable metadata has one atomic write epoch; event rows use their commit
/// position and only privacy/excision changes invalidate their continuations.
async fn availability(conn:&mut PgConnection,query:&Query)->Result<Json> {
    let mut policy:Json=sqlx::query_scalar("SELECT jsonb_build_object('policy_epoch',privacy.require_active_epoch(),'privacy_outputs',coalesce((SELECT value FROM core.counters WHERE name='privacy_outputs'),0),'rules',coalesce((SELECT max(seq) FROM privacy.rules),0),'floors',coalesce((SELECT max(seq) FROM privacy.source_classes),0),'excised',coalesce((SELECT value FROM core.counters WHERE name='excised'),0))").fetch_one(&mut *conn).await?;
    if matches!(query.source,Source::Table{table,..} if !table.is_events()) {
        let metadata:Json=sqlx::query_scalar("SELECT jsonb_build_object('epoch',(SELECT value FROM core.counters WHERE name='query_metadata'),'head',(SELECT value FROM core.counters WHERE name='commit_seq'))").fetch_one(conn).await?;
        policy["metadata"]=metadata;
    }
    Ok(policy)
}

/// One authenticated question and immutable read position.
struct ReadPage {
    question:String,question_id:query_language::identity::QuestionId,
    seal:query_language::cursor::CursorSeal,ctx:Context,seq:i64,size:u64,prior:Option<cursor::Cursor>,availability:Json,
}
impl ReadPage {
    fn check_availability(&self)->Result<()> {
        if self.prior.as_ref().is_some_and(|prior|prior.availability!=self.availability) {return Err(Error::StaleCursor);}Ok(())
    }
    fn resolved(&self)->Resolved {Resolved{now:self.ctx.now.to_string(),zone:self.ctx.zone_name(),day_start:self.ctx.day_start.to_string(),commit_seq:self.seq}}
    fn info(&self,total:i64,more:bool,last:Option<(query_language::cursor::OrderPosition,Vec<String>)>)->Result<PageInfo> {
        let cursor=if more {
            let (position,types)=last.ok_or_else(||Error::Invalid("query page did not advance".into()))?;
            Some(cursor::Cursor::encode(&self.seal,self.question_id,&self.question,&self.ctx,self.seq,total,types,position,self.availability.clone())?)
        }else{None};
        Ok(PageInfo{size:self.size,cursor})
    }
}
struct Rendered {
    total:Option<(String,Vec<sql::Param>)>,rows:(String,Vec<sql::Param>),columns:Vec<String>,
}
fn typed_boundary(value:&Json,ty:&str)->Result<SqlFragment> {
    let cast=match ty {
        "smallint"|"integer"|"bigint"|"numeric"|"real"|"double precision"|"boolean"|"uuid"|"timestamp with time zone"|"timestamp without time zone"|"date"|"text"|"character varying"=>ty,
        _=>return Err(Error::Invalid(format!("{ty} cannot be used as a scalar page key"))),
    };
    let mut f=SqlFragment::text("(");f.param(sea_query::Value::String(Some(value.to_string()))).push("::jsonb #>> '{}')::").push(cast);
    Ok(f)
}
fn render_page(compiled:Compiled,read:&ReadPage)->Result<Rendered> {
    let object=json_object(&compiled.columns);
    let values=compiled.keys.iter().map(|(name,_)|format!("q.\"{name}\"")).collect::<Vec<_>>();
    let order=compiled.keys.iter().map(|(name,desc)|format!("q.\"{name}\" {} NULLS LAST",if *desc {"DESC"}else{"ASC"})).collect::<Vec<_>>().join(", ");
    let total=read.prior.is_none().then(||compiled.count.render());
    let mut rows=SqlFragment::text(format!("SELECT jsonb_build_object('row',({object}),'keys',jsonb_build_array({}),'types',q.\"_key_types\",'id',q.\"_row_identity\") FROM (",values.join(", ")));
    rows.append(compiled.answer).push(") q");
    if let Some(prior)=&read.prior {
        if prior.after.values.len()!=compiled.keys.len() || prior.key_types.len()!=compiled.keys.len() {return Err(Error::StaleCursor);}
        let mut disjunction=Vec::new();
        let mut equals=Vec::new();
        for (i,((name,desc),value)) in compiled.keys.iter().zip(&prior.after.values).enumerate() {
            let key=format!("q.\"{name}\"");
            if !value.is_null() {
                let mut next=SqlFragment::text(format!("({key} IS NULL OR {key} {} ",if *desc {"<"}else{">"}));
                next.append(typed_boundary(value,&prior.key_types[i])?).push(")");
                let mut chain=equals.clone();chain.push(next);disjunction.push(SqlFragment::join(chain," AND "));
            }
            let mut equal=SqlFragment::text(format!("{key} IS NOT DISTINCT FROM "));
            equal.append(typed_boundary(value,&prior.key_types[i])?);equals.push(equal);
        }
        rows.push(" WHERE (").append(if disjunction.is_empty(){SqlFragment::text("false")}else{SqlFragment::join(disjunction," OR ")}).push(")");
    }
    rows.push(format!(" ORDER BY {order} LIMIT ")).param(sea_query::Value::BigInt(Some(i64::try_from(read.size.checked_add(1).ok_or_else(||Error::Invalid("page size overflow".into()))?).map_err(|_|Error::Invalid("page size overflow".into()))?)));
    Ok(Rendered{total,rows:rows.render(),columns:compiled.columns})
}
async fn page_of(tx:&mut PgConnection,rendered:Rendered,table:String,text:&str,query:&Query,read:&ReadPage,recall:Option<Json>)->Result<Answer> {
    let total=match (rendered.total,&read.prior) {
        (Some((sql,values)),_)=>sql::bind_scalar(sqlx::query_scalar::<_,i64>(&sql),values).fetch_one(&mut *tx).await?,
        (None,Some(cursor))=>cursor.total,_=>unreachable!("first page counts the result"),
    };
    let (sql,values)=rendered.rows;
    let mut selected:Vec<Json>=sql::bind_scalar(sqlx::query_scalar(&sql),values).fetch_all(&mut *tx).await?;
    let more=selected.len() as u64>read.size;if more {selected.pop();}
    let last=selected.last().map(|row|->Result<_>{
        let values=row["keys"].as_array().ok_or_else(||Error::Invalid("missing page keys".into()))?.clone();
        if values.iter().any(|v|v.is_array()||v.is_object()) {return Err(Error::Invalid("page keys must be scalar".into()));}
        let types=serde_json::from_value(row["types"].clone()).map_err(|e|Error::Invalid(e.to_string()))?;
        let identity=row["id"].as_str().ok_or_else(||Error::Invalid("missing row identity".into()))?.into();
        Ok((query_language::cursor::OrderPosition{values,row_identity:identity},types))
    }).transpose()?;
    let page=read.info(total,more,last)?;
    Ok(Answer{table,query:text.into(),read_as:read_as(query),resolved:read.resolved(),columns:rendered.columns,rows:selected.into_iter().map(|mut row|row["row"].take()).collect(),total,page,recall})
}

/// `jsonb_build_object` over the columns, in order, in chunks the function's
/// argument limit allows.
fn json_object(columns: &[String]) -> String {
    if columns.is_empty() {
        return "'{}'::jsonb".to_string();
    }
    let parts: Vec<String> = columns
        .chunks(50)
        .map(|chunk| {
            let pairs: Vec<String> = chunk
                .iter()
                .map(|c| {
                    format!(
                        "'{}', q.\"{}\"",
                        c.replace('\'', "''"),
                        c.replace('"', "\"\"")
                    )
                })
                .collect();
            format!("jsonb_build_object({})", pairs.join(", "))
        })
        .collect();
    parts.join(" || ")
}

/// The commit position Sinex had reached at a time.
async fn seq_at(conn: &mut PgConnection, t: jiff::Timestamp) -> Result<i64> {
    let odt = time::OffsetDateTime::from_unix_timestamp_nanos(t.as_nanosecond())
        .map_err(|e| Error::Invalid(format!("time out of range: {e}")))?;
    Ok(sqlx::query_scalar!(
        r#"SELECT coalesce(max(commit_seq), 0) AS "seq!" FROM (
               SELECT commit_seq FROM core.events WHERE ts_persisted <= $1
               UNION ALL
               SELECT commit_seq FROM audit.archive_changes WHERE recorded_at <= $1
           ) positions"#,
        odt
    )
    .fetch_one(&mut *conn)
    .await?)
}

#[cfg(test)]
pub(crate) mod tests;

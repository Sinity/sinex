//! Complete query-backed derivation inputs through the ordinary query compiler.
use super::*;
use serde_json::json;
use sinex_model::event_types::analysis::QuerySelection;
use sinex_model::{Class, EventId, Timestamp};
use std::collections::BTreeSet;
pub use crate::privacy::estimability::{SelectorCoverage,SelectorUnavailable};

/// Guard an all-current candidate scope before a reverse payload selector.
/// Call in the same pinned snapshot as the subsequent ordinary query. The
/// caller's connection grants are authoritative; no request field widens them.
pub async fn selector_coverage_in_snapshot(
    conn:&mut PgConnection,
    read:crate::ReadMark,
    through:i64,
    kinds:&[sinex_model::KindRef],
    requirements:Option<&[sinex_model::InputRequirement]>,
)->Result<SelectorCoverage>{
    crate::privacy::estimability::selector_coverage(conn,read,through,kinds,requirements,512,crate::PAGE_BYTES).await
}

pub(super) struct SelectedPage {
    pub answer: Answer,
    pub selection: Option<QuerySelection>,
    pub parents: Vec<EventId>,
    pub class: Class,
}

pub struct SelectedAnswer {
    pub answer: Answer,
    pub parents: Vec<EventId>,
    pub class: Class,
    pub signature: String,
}

/// Resolve a relative/session/mark selector once, before its request is admitted.
pub async fn resolve_recall(
    conn: &mut PgConnection,
    text: &str,
    ctx: &Context,
) -> Result<QuerySelection> {
    run_inner(
        conn,
        text,
        None,
        ctx,
        &Page {
            size: Some(1),
            cursor: None,
        },
        None,
        true,
        false,
    )
    .await?
    .selection
    .ok_or_else(|| Error::Invalid("a selected context requires a recall question".into()))
}

/// Read all rows of a retained selector. Query paging bounds each database read;
/// the complete stored product has no presentation limit.
pub async fn selected_query(
    conn: &mut PgConnection,
    selection: &QuerySelection,
) -> Result<SelectedAnswer> {
    complete(conn,selection,false).await
}

pub async fn selected_query_in_snapshot(conn: &mut PgConnection,selection: &QuerySelection) -> Result<SelectedAnswer> {
    complete(conn,selection,true).await
}

async fn complete(conn: &mut PgConnection,selection: &QuerySelection,in_snapshot:bool) -> Result<SelectedAnswer> {
    let previous: String =
        sqlx::query_scalar("SELECT coalesce(current_setting('sinex.grants', true), '')")
            .fetch_one(&mut *conn)
            .await?;
    set_grants(conn, &selection.grants.join(",")).await?;
    let result = async {
        let now = jiff::Timestamp::from_nanosecond(selection.now.unix_ns())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let zone = jiff::tz::TimeZone::get(&selection.zone)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let day_start = selection
            .day_start
            .parse()
            .map_err(|error: jiff::Error| Error::Invalid(error.to_string()))?;
        let ctx = Context::new(now, zone, day_start);
        let mut page = Page {
            size: Some(5_000),
            cursor: None,
        };
        let mut first: Option<SelectedAnswer> = None;
        loop {
            let next = run_inner(
                conn,
                &selection.query,
                None,
                &ctx,
                &page,
                Some(selection),
                first.is_none(),
                in_snapshot,
            )
            .await?;
            let cursor = next.answer.page.cursor.clone();
            if let Some(first) = &mut first {
                if first.answer.resolved != next.answer.resolved
                    || first.answer.recall != next.answer.recall
                {
                    return Err(Error::StaleCursor);
                }
                first.answer.rows.extend(next.answer.rows);
            } else {
                let signature = next.selection.as_ref().and_then(|selection| selection.selected["_fingerprint"].as_str())
                    .ok_or_else(|| Error::Invalid("selected query fingerprint is unavailable".into()))?.to_owned();
                first = Some(SelectedAnswer {
                    answer: next.answer,
                    parents: next.parents,
                    class: next.class,
                    signature,
                });
            }
            match cursor {
                Some(cursor) => page.cursor = Some(cursor),
                None => break,
            }
        }
        let mut selected = first.expect("a query returns its first page, including empty answers");
        selected.answer.page.cursor = None;
        selected.answer.page.size = u64::try_from(selected.answer.rows.len()).map_err(|_| {
            Error::Invalid("selected answer row count is outside the archive range".into())
        })?;
        Ok(selected)
    }
    .await;
    let restored = set_grants(conn, &previous).await;
    restored?;
    result
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn capture(
    conn: &mut PgConnection,
    text: &str,
    query: &Query,
    ctx: &Context,
    seq: i64,
    grants: &str,
    selection: &recall::Selection,
    rows: &Query,
    account: &Json,
) -> Result<(QuerySelection, Vec<EventId>, Class)> {
    let selector = walk::recall_rows(rows);
    let (sql, values) = {
        let compiled = super::compile_query(conn,ctx,seq,&selector).await?;
        let mut sql =
            SqlFragment::text(format!("SELECT {} FROM (", json_object(&compiled.columns)));
        sql.append(compiled.answer).push(") q");
        sql.render()
    };
    let rows: Vec<Json> = sql::bind_scalar(sqlx::query_scalar(&sql), values)
        .fetch_all(&mut *conn)
        .await?;
    let mut parents = BTreeSet::new();
    for row in &rows {
        if let Some(fields) = row.as_object() {
            for (name, value) in fields {
                if name == "id"
                    || name == "near"
                    || name.starts_with("near_")
                    || name == "during"
                    || name.starts_with("during_")
                    || name == "containing"
                    || name.starts_with("containing_")
                {
                    if let Some(id) = value
                        .as_str()
                        .and_then(|value| value.parse::<uuid::Uuid>().ok())
                    {
                        parents.insert(id);
                    }
                }
            }
        }
    }
    parents.extend(selection.anchor_ids());
    let ids: Vec<_> = parents.into_iter().collect();
    let classes: Vec<String> = sqlx::query_scalar("SELECT class::text FROM core.visible_events WHERE id = ANY($1) AND commit_seq <= $2 AND NOT audit.is_archived_at(id, $2)")
        .bind(&ids).bind(seq).fetch_all(&mut *conn).await?;
    if classes.len() != ids.len() {
        return Err(Error::Invalid(
            "selected query provenance changed while being read".into(),
        ));
    }
    let class = classes.into_iter().try_fold(Class::Open,|class,stored| {
        serde_json::from_value::<Class>(json!(stored)).map(|next| class.max(next))
    }).map_err(|error| Error::Invalid(error.to_string()))?;
    let explicit = query
        .stages
        .iter()
        .any(|stage| matches!(stage, Stage::AsOf(_)));
    let mut grants: Vec<String> = grants
        .split(',')
        .filter(|grant| !grant.is_empty())
        .map(str::to_owned)
        .collect();
    grants.sort();
    grants.dedup();
    let mut retained = selection.retained(&query.source)?;
    retained["_query"] = json!(format!("{query:?}"));
    retained["_fingerprint"] = json!(blake3::hash(serde_json::to_vec(&json!([rows,account])).map_err(|error| Error::Invalid(error.to_string()))?.as_slice()).to_hex().to_string());
    let selection = QuerySelection {
        query: text.trim().into(),
        now: Timestamp::from_unix_ns(ctx.now.as_nanosecond())
            .map_err(|error| Error::Invalid(error.to_string()))?,
        read_as: explicit.then_some(seq),
        zone: ctx.zone_name(),
        day_start: ctx.day_start.to_string(),
        grants,
        selected: retained,
        declarations: serde_json::to_value(
            ctx.catalog
                .as_ref()
                .map(|catalog| &catalog.declarations)
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))?,
    };
    Ok((selection, ids.into_iter().map(EventId).collect(), class))
}

/// A pack cursor cannot be used as a page of a different bound question.
pub async fn check_selected_question(
    conn: &mut PgConnection,
    text: &str,
    selection: &QuerySelection,
) -> Result<()> {
    let expected: Vec<(sinex_model::KindRef,String)> = serde_json::from_value(selection.declarations.clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let references:Vec<_> = expected.iter().map(|(reference,_)| reference.clone()).collect();
    let declarations = sinex_schema::kinds::load(conn,&references).await?;
    if expected.iter().any(|(reference,hash)| declarations.get(reference).is_none_or(|declaration| declaration.definition_hash()!=*hash)) {
        return Err(Error::Invalid("query declaration snapshot is unavailable".into()));
    }
    let now = jiff::Timestamp::from_nanosecond(selection.now.unix_ns()).map_err(|error| Error::Invalid(error.to_string()))?;
    let zone = jiff::tz::TimeZone::get(&selection.zone).map_err(|error| Error::Invalid(error.to_string()))?;
    let day_start = selection.day_start.parse().map_err(|error:jiff::Error| Error::Invalid(error.to_string()))?;
    let mut ctx = Context::new(now,zone,day_start);
    ctx.catalog = Some(sinex_model::query::QueryCatalog::new(declarations.into_values()));
    let mut query = if sinex_model::query::saved::reference_name(text).is_some() {
        let definitions = serde_json::from_value(selection.selected["_saved"].clone()).map_err(|error| Error::Invalid(error.to_string()))?;
        sinex_model::query::saved::bind_saved(text,&ctx,&definitions)?
    } else {sinex_model::query::bind(text,&ctx)?};
    ctx.catalog.as_ref().expect("retained query catalog").bind(&mut query,&ctx)?;
    if selection.selected["_query"].as_str()!=Some(format!("{query:?}").as_str()) {
        return Err(Error::Invalid("recall cursor belongs to a different bound question".into()));
    }
    Ok(())
}

/// Historical previews use the same selected rows and exact provenance without
/// admitting a new live output over inputs that may already be archived.
pub async fn selected_page(
    conn: &mut PgConnection,
    text: &str,
    ctx: &Context,
    page: &Page,
    selection: Option<&QuerySelection>,
) -> Result<SelectedAnswer> {
    let selected = run_inner(conn,text,None,ctx,page,selection,true,false).await?;
    let signature = selected.selection.as_ref().and_then(|selection| selection.selected["_fingerprint"].as_str())
        .ok_or_else(|| Error::Invalid("selected query fingerprint is unavailable".into()))?.to_owned();
    Ok(SelectedAnswer {answer:selected.answer,parents:selected.parents,class:selected.class,signature})
}

/// The result of a current computational selector after negative-membership coverage.
pub enum CurrentSelection {
    Available(Box<SelectedAnswer>),
    Unavailable(SelectorUnavailable),
}

/// Read indexed computational candidates through the ordinary bound compiler.
/// The caller supplies a trusted owner projection scope. Retained public
/// questions continue using their separately saved grants.
pub async fn referenced_query_in_snapshot(
    conn:&mut PgConnection,text:&str,ctx:&Context,
    scope:&crate::model_calls::references::ReferenceScope,targets:&[EventId],
)->Result<CurrentSelection>{
    use crate::model_calls::references::{self,ReferenceResult};
    let mut after=None;
    let mut aggregate:Option<SelectedAnswer>=None;
    let mut parents=BTreeSet::new();
    loop {
        let page=match references::candidate_page(conn,scope,targets,after,512,crate::PAGE_BYTES).await? {
            ReferenceResult::Unavailable(reason)=>return Ok(CurrentSelection::Unavailable(reason)),
            ReferenceResult::Known(page)=>page,
        };
        let selected=match complete_current(conn,text,ctx,std::slice::from_ref(&scope.reference().owner_kind),Some(&page.ids)).await? {
            CurrentSelection::Unavailable(reason)=>return Ok(CurrentSelection::Unavailable(reason)),
            CurrentSelection::Available(selected)=>*selected,
        };
        parents.extend(selected.parents.iter().copied());
        if let Some(aggregate)=&mut aggregate {
            if aggregate.answer.resolved!=selected.answer.resolved{return Err(Error::StaleCursor)}
            aggregate.answer.rows.extend(selected.answer.rows);
            aggregate.class=aggregate.class.max(selected.class);
        }else{aggregate=Some(selected)}
        match page.next {Some(next)=>after=Some(next),None=>break}
    }
    let mut aggregate=aggregate.expect("candidate readers return an empty first page");
    // Membership is a set. Transfer budgets never affect its fingerprint or
    // create a database parameter list proportional to the full candidate set.
    aggregate.answer.rows.sort_by(|a,b|a["id"].as_str().cmp(&b["id"].as_str()));
    aggregate.parents=parents.into_iter().collect();
    aggregate.answer.total=i64::try_from(aggregate.answer.rows.len()).map_err(|_|Error::Invalid("selected row count is outside the archive range".into()))?;
    aggregate.answer.page.size=u64::try_from(aggregate.answer.rows.len()).map_err(|_|Error::Invalid("selected row count is outside the archive range".into()))?;
    let mut bound_ctx=ctx.clone();
    bound_ctx.catalog=Some(sinex_model::query::QueryCatalog::new(sinex_schema::kinds::all(conn).await?.into_values()));
    let bound=sinex_model::query::bind(text,&bound_ctx)?;
    let mark=crate::ReadMark::now(conn).await?;
    aggregate.signature=blake3::hash(&serde_json::to_vec(&json!([format!("{bound:?}"),bound_ctx.catalog.as_ref().expect("loaded catalog").declarations,aggregate.answer.rows,mark.policy_epoch]))
        .map_err(|error|Error::Invalid(error.to_string()))?).to_hex().to_string();
    Ok(CurrentSelection::Available(Box::new(aggregate)))
}

async fn complete_current(conn:&mut PgConnection,text:&str,ctx:&Context,kinds:&[sinex_model::KindRef],candidates:Option<&[EventId]>)->Result<CurrentSelection>{
    let mark=crate::ReadMark::now(conn).await?;
    let through=current_seq(conn).await?;
    let mut ctx=ctx.clone();
    ctx.catalog=Some(sinex_model::query::QueryCatalog::new(sinex_schema::kinds::all(conn).await?.into_values()));
    let mut query=sinex_model::query::bind(text,&ctx)?;
    // Computational membership is an event set, never a presentation projection.
    if !matches!(query.source,Source::Table{table:Table::Events,..})
        ||query.stages.iter().any(|stage|!matches!(stage,Stage::Where(_)|Stage::Sort(_))) {
        return Err(Error::Invalid("computational selector must return complete event rows".into()));
    }
    if let Some(ids)=candidates {
        let predicate=Expr::Compare {
            lhs:Operand::Field(FieldRef::Field(Table::Events.field("id").expect("events have IDs"))),
            op:sinex_model::query::Op::Eq,
            rhs:sinex_model::query::Value::List(ids.iter().map(|id|sinex_model::query::Value::Id(id.0)).collect()),
        };
        query.filter=Some(match query.filter.take(){Some(filter)=>Expr::And(Box::new(filter),Box::new(predicate)),None=>predicate});
    }
    let mut page=Page{size:Some(512),cursor:None};
    let mut answer:Option<Answer>=None;
    let mut parents=BTreeSet::new();
    let mut class=Class::Open;
    loop {
        let next=run_inner(conn,text,Some(&query),&ctx,&page,None,false,true).await?.answer;
        if next.resolved.commit_seq!=through{return Err(Error::StaleCursor)}
        for row in &next.rows {
            let id=row.get("id").and_then(Json::as_str).and_then(|id|id.parse::<uuid::Uuid>().ok())
                .ok_or_else(||Error::Invalid("computational selector omitted an exact event ID".into()))?;
            let kind=row.get("event_type").and_then(Json::as_str);
            let version=row.get("kind_version").and_then(Json::as_u64);
            if !kinds.iter().any(|expected|Some(expected.name.as_str())==kind&&Some(expected.version)==version){
                return Err(Error::Invalid("computational selector returned an undeclared input kind".into()));
            }
            parents.insert(EventId(id));
        }
        let cursor=next.page.cursor.clone();
        match &mut answer {
            Some(answer)=>{
                if answer.resolved!=next.resolved{return Err(Error::StaleCursor)}
                answer.rows.extend(next.rows);
            },
            None=>answer=Some(next),
        }
        match cursor {Some(cursor)=>page.cursor=Some(cursor),None=>break}
    }
    let mut answer=answer.expect("ordinary queries return an empty first page");
    let ids:Vec<_>=parents.iter().map(|id|id.0).collect();
    for page in ids.chunks(512){
        let classes:Vec<(uuid::Uuid,String)>=sqlx::query_as("SELECT id,class::text FROM core.visible_events WHERE id=ANY($1) AND commit_seq<=$2 AND NOT audit.is_archived_at(id,$2)")
            .bind(page).bind(through).fetch_all(&mut *conn).await?;
        if classes.len()!=page.len(){return Err(Error::StaleCursor)}
        for (_,value) in classes {class=class.max(serde_json::from_value::<Class>(json!(value)).map_err(|error|Error::Invalid(error.to_string()))?);}
    }
    answer.page.cursor=None;
    answer.page.size=u64::try_from(answer.rows.len()).map_err(|_|Error::Invalid("selected row count is outside the archive range".into()))?;
    let signature=blake3::hash(&serde_json::to_vec(&json!([format!("{query:?}"),ctx.catalog.as_ref().expect("loaded catalog").declarations,answer.rows,mark.policy_epoch]))
        .map_err(|error|Error::Invalid(error.to_string()))?).to_hex().to_string();
    Ok(CurrentSelection::Available(Box::new(SelectedAnswer{answer,parents:parents.into_iter().collect(),class,signature})))
}

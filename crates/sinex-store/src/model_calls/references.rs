//! Owner references route identities and invalidation. They never supply a
//! computational payload or turn a source fact into a derived parent.
use crate::{Result,Error,ReadMark,DirtyKeys};
use crate::privacy::estimability::{self,SelectorCoverage,SelectorUnavailable};
use sinex_model::{EventId,KindRef,ReferenceInput,Validation};
use serde_json::{Value,json};
use sqlx::PgConnection;
use uuid::Uuid;

const OWNER:&str="model.request";
const VERSION:u64=2;
const PAGE:i64=512;

#[derive(Clone,Debug)]
pub enum ReferenceResult<T>{Known(T),Unavailable(SelectorUnavailable)}
#[derive(Clone,Debug)]
pub struct ReferenceScope {reference:ReferenceInput,read:ReadMark,through:i64,grants:String}
impl ReferenceScope {
    pub fn reference(&self)->&ReferenceInput{&self.reference}
    pub fn key(&self,target:EventId)->Value{key(&self.reference,target)}
}
#[derive(Clone,Copy,Debug)]
pub struct ReferenceCursor {pub position:i64,pub event:EventId}
#[derive(Clone,Debug)]
pub struct ReferencePage {pub ids:Vec<EventId>,pub next:Option<ReferenceCursor>}
fn key(reference:&ReferenceInput,target:EventId)->Value{let mut value=serde_json::Map::new();value.insert(reference.key_field.clone(),json!(target));Value::Object(value)}
fn supported(reference:&ReferenceInput)->bool{
    reference.owner_kind.name==OWNER&&reference.owner_kind.version==VERSION
        &&estimability::valid(&reference.selector)
        &&reference.target_kind.database_version().is_ok()&&!reference.key_field.is_empty()
        &&["/targets","/request/operation","/recorded_response"].iter().all(|required|
            reference.selector.iter().filter(|r|r.kind==reference.owner_kind).flat_map(|r|&r.paths)
                .any(|p|p.as_str()==*required||p.is_empty()||required.starts_with(&format!("{p}/"))))
}
/// Cache only within the caller's existing read snapshot. An older live
/// request format cannot prove absence from the current request format.
pub async fn scope(conn:&mut PgConnection,reference:&ReferenceInput,read:ReadMark,through:i64)->Result<ReferenceResult<ReferenceScope>>{
    if !supported(reference){return Ok(ReferenceResult::Unavailable(SelectorUnavailable::UndeclaredSelector))}
    let unknown:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.events e WHERE e.event_type='model.request' AND e.kind_version<>2 AND e.commit_seq<=$1 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id))").bind(through).fetch_one(&mut *conn).await?;
    if unknown{return Ok(ReferenceResult::Unavailable(SelectorUnavailable::UndeclaredSelector))}
    match estimability::selector_coverage(conn,read,through,std::slice::from_ref(&reference.owner_kind),Some(&reference.selector),PAGE,crate::PAGE_BYTES).await?{
        SelectorCoverage::Complete{read,through,grants}=>Ok(ReferenceResult::Known(ReferenceScope{reference:reference.clone(),read,through,grants})),
        SelectorCoverage::Unavailable{reason}=>Ok(ReferenceResult::Unavailable(reason)),
    }
}
/// The private targets expression supplies candidate identities only. The
/// ordinary projected query and ExactInputs still decide usable membership.
pub async fn candidate_page(conn:&mut PgConnection,scope:&ReferenceScope,targets:&[EventId],after:Option<ReferenceCursor>,count:i64,bytes:i64)->Result<ReferenceResult<ReferencePage>>{
    if count<=0||bytes<=0{return Err(Error::Invalid("reference page budgets must be positive".into()))}
    let isolation:String=sqlx::query_scalar("SELECT current_setting('transaction_isolation')").fetch_one(&mut *conn).await?;
    if !matches!(isolation.as_str(),"repeatable read"|"serializable"){return Ok(ReferenceResult::Unavailable(SelectorUnavailable::SnapshotRequired))}
    let policy=crate::privacy::projection::epoch(conn).await?;
    if !policy.ready()||scope.read.policy_epoch!=Some(policy.active)||crate::ReadMark::now(conn).await?!=scope.read
        ||crate::query::current_seq(conn).await?!=scope.through||crate::query::current_grants(conn).await?!=scope.grants{
        return Ok(ReferenceResult::Unavailable(SelectorUnavailable::ReadScopeChanged))
    }
    let targets:Vec<_>=targets.iter().map(ToString::to_string).collect();
    let position=after.map_or(0,|c|c.position);let event=after.map(|c|c.event.0);
    // UUID/bigint binary fields plus row framing fit in 64 wire bytes.
    let count=count.min((bytes/64).max(1));
    let rows:Vec<(Uuid,i64)>=sqlx::query_as("SELECT e.id,e.commit_seq FROM core.events e WHERE e.event_type='model.request' AND e.kind_version=2 AND e.validation='validated' AND e.payload->'targets' ?| $1::text[] AND e.commit_seq<=$2 AND (e.commit_seq>$3 OR (e.commit_seq=$3 AND $4::uuid IS NOT NULL AND e.id>$4)) AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) ORDER BY e.commit_seq,e.id LIMIT $5")
        .bind(targets).bind(scope.through).bind(position).bind(event).bind(count).fetch_all(conn).await?;
    let next=rows.last().map(|(id,position)|ReferenceCursor{position:*position,event:EventId(*id)});
    Ok(ReferenceResult::Known(ReferencePage{ids:rows.into_iter().map(|r|EventId(r.0)).collect(),next}))
}

#[derive(sqlx::FromRow)]
struct OwnerRow {
    id:Uuid,commit_seq:i64,kind_version:i64,validation:String,
    targets:Option<Value>,policy:Option<Value>,response:Option<Value>,
    epoch:Option<i64>,state:Option<String>,payload:Option<Value>,trace:Option<Value>,visible:bool,
}
const ROWS:&str="SELECT e.id,e.commit_seq,e.kind_version,e.validation,e.payload->'targets' AS targets,e.payload->'policy' AS policy,e.payload->'recorded_response' AS response,o.epoch,o.state,o.payload,o.trace,coalesce(core.class_visible(o.class),false) AS visible FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id";
fn bounded(sql:String,bytes_parameter:usize)->String {
    format!("WITH candidates AS MATERIALIZED ({sql}) SELECT id,commit_seq,kind_version,validation,targets,policy,response,epoch,state,payload,trace,visible FROM(SELECT sized.*,sum(size) OVER(ORDER BY commit_seq,id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)-size AS before FROM(SELECT candidates.*,coalesce(octet_length(targets::text),0)::bigint+coalesce(octet_length(policy::text),0)::bigint+coalesce(octet_length(response::text),0)::bigint+coalesce(octet_length(payload::text),0)::bigint+coalesce(octet_length(trace::text),0)::bigint+octet_length(validation)::bigint+coalesce(octet_length(state),0)::bigint+160 AS size FROM candidates) sized) limited WHERE before<${bytes_parameter}::bigint ORDER BY commit_seq,id")
}
fn known(row:&OwnerRow,reference:&ReferenceInput,epoch:i64)->Option<Vec<EventId>>{
    if row.kind_version!=VERSION as i64||row.validation!="validated"||row.epoch!=Some(epoch)||row.state.as_deref()!=Some("complete")||!row.visible{return None}
    let payload=row.payload.as_ref()?;if sinex_sealing::is_sealed(payload){return None}
    let trace:Vec<crate::privacy::AffectedPath>=serde_json::from_value(row.trace.clone()?).ok()?;
    let kind=KindRef{name:OWNER.into(),version:VERSION};
    if estimability::selector_reason(&kind,payload,Validation::Validated,&trace,&reference.selector).is_some(){return None}
    let targets=serde_json::from_value::<Vec<EventId>>(row.targets.clone()?).ok()?;
    serde_json::from_value::<EventId>(row.policy.clone()?).ok()?;
    serde_json::from_value::<Option<EventId>>(row.response.clone()?).ok()?;
    Some(targets)
}

/// Batches use the existing dirty table and the existing writer transaction.
/// New facts call this after their desired-epoch projection has been stored.
pub(crate) async fn mark_changed(conn:&mut PgConnection,changed:&[Uuid],seq:i64,keys:&dyn DirtyKeys)->Result<usize>{
    mark_changed_deferring(conn,changed,seq,keys,&[]).await
}
/// A replacement archives its predecessor before its new projection exists.
/// Defer only those known in-flight rows; admission checks them after project.
pub(crate) async fn mark_changed_deferring(conn:&mut PgConnection,changed:&[Uuid],seq:i64,keys:&dyn DirtyKeys,deferred:&[Uuid])->Result<usize>{
    let consumers=keys.reference_consumers();if consumers.is_empty()||changed.is_empty(){return Ok(0)}
    let kinds:Vec<(String,i64,i64)>=sqlx::query_as("SELECT event_type,kind_version,max(commit_seq) FROM core.events WHERE id=ANY($1) GROUP BY event_type,kind_version").bind(changed).fetch_all(&mut *conn).await?;
    if !consumers.iter().any(|(_,reference)|kinds.iter().any(|(kind,_,_)|kind==OWNER||kind=="model.policy"||kind=="model.response"||kind==&reference.target_kind.name)){return Ok(0)}
    let seq=seq.max(kinds.iter().map(|row|row.2).max().unwrap_or(0));
    // This private owner computation has the same class scope as the daemon
    // reference selector. LOCAL grants cannot escape an aborted writer tx.
    let previous=crate::query::current_grants(conn).await?;
    sqlx::query_scalar::<_,String>("SELECT set_config('sinex.grants','guarded,sealed',true)").fetch_one(&mut *conn).await?;
    let result=mark_changed_owner(conn,changed,seq,consumers,deferred,&kinds).await;
    let restored=sqlx::query_scalar::<_,String>("SELECT set_config('sinex.grants',$1,true)").bind(previous).fetch_one(&mut *conn).await;
    restored?;result
}
async fn mark_changed_owner(conn:&mut PgConnection,changed:&[Uuid],seq:i64,consumers:Vec<(String,ReferenceInput)>,deferred:&[Uuid],kinds:&[(String,i64,i64)])->Result<usize>{
    let epoch=crate::privacy::projection::epoch(conn).await?.desired;
    let changed_text:Vec<_>=changed.iter().map(ToString::to_string).collect();let mut marked=0;
    for (name,reference) in consumers {
        if !kinds.iter().any(|(kind,_,_)|kind==OWNER||kind=="model.policy"||kind=="model.response"||kind==&reference.target_kind.name){continue}
        let mut global=!supported(&reference);
        // Only potentially unavailable owner projections need full trace
        // inspection. Unchanged validated owners retain their typed refs.
        let mut after=(0,None::<Uuid>);
        while !global {
            let sql=format!("{ROWS} WHERE e.event_type='model.request' AND NOT (e.id=ANY($6) AND o.event_id IS NULL) AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) AND (e.kind_version<>2 OR e.validation<>'validated' OR o.epoch IS DISTINCT FROM $1 OR o.state IS DISTINCT FROM 'complete' OR NOT coalesce(core.class_visible(o.class),false) OR o.changed) AND (e.commit_seq>$2 OR (e.commit_seq=$2 AND $3::uuid IS NOT NULL AND e.id>$3)) ORDER BY e.commit_seq,e.id LIMIT $4");
            let sql=bounded(sql,5);
            let rows=sqlx::query_as::<_,OwnerRow>(&sql).bind(epoch).bind(after.0).bind(after.1).bind(PAGE).bind(crate::PAGE_BYTES).bind(deferred).fetch_all(&mut *conn).await?;
            let Some(last)=rows.last() else{break};after=(last.commit_seq,Some(last.id));
            if rows.iter().any(|row|known(row,&reference,epoch).is_none()){global=true;}
        }
        let mut after=(0,None::<Uuid>);
        while !global {
            let sql=format!("{ROWS} WHERE e.event_type='model.request' AND NOT (e.id=ANY($7) AND o.event_id IS NULL) AND (e.id=ANY($1) OR (e.kind_version=2 AND e.validation='validated' AND (e.payload->'targets' ?| $2::text[] OR e.payload->>'policy'=ANY($2::text[]) OR e.payload->>'recorded_response'=ANY($2::text[])))) AND (e.commit_seq>$3 OR (e.commit_seq=$3 AND $4::uuid IS NOT NULL AND e.id>$4)) ORDER BY e.commit_seq,e.id LIMIT $5");
            let sql=bounded(sql,6);
            let rows=sqlx::query_as::<_,OwnerRow>(&sql).bind(changed).bind(&changed_text).bind(after.0).bind(after.1).bind(PAGE).bind(crate::PAGE_BYTES).bind(deferred).fetch_all(&mut *conn).await?;
            let Some(last)=rows.last() else{break};after=(last.commit_seq,Some(last.id));let mut targets=std::collections::BTreeSet::new();
            for row in rows {match known(&row,&reference,epoch){Some(ids)=>targets.extend(ids),None=>{global=true;break}}}
            if !global{marked+=mark_targets(conn,&name,&reference,targets.into_iter().collect(),seq).await?;}
        }
        if global {marked+=mark_all(conn,&name,&reference,seq).await?;}
    }
    Ok(marked)
}

async fn mark_targets(conn:&mut PgConnection,name:&str,reference:&ReferenceInput,targets:Vec<EventId>,seq:i64)->Result<usize>{
    let mut marked=0;
    for page in targets.chunks(PAGE as usize){
        let ids:Vec<_>=page.iter().map(|id|id.0).collect();
        let rows:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM core.events WHERE id=ANY($1) AND event_type=$2 AND kind_version=$3")
            .bind(ids).bind(&reference.target_kind.name).bind(reference.target_kind.database_version().map_err(Error::Invalid)?).fetch_all(&mut *conn).await?;
        let mut dirty:Vec<_>=rows.into_iter().map(|id|(name.to_owned(),key(reference,EventId(id)))).collect();
        // An erased target may still have an old owned key to retire.
        let offered:Vec<_>=page.iter().map(|id|key(reference,*id)).collect();
        let old:Vec<Value>=sqlx::query_scalar("SELECT DISTINCT key FROM derivation.outputs WHERE derivation=$1 AND key=ANY($2::jsonb[])").bind(name).bind(offered).fetch_all(&mut *conn).await?;
        dirty.extend(old.into_iter().map(|key|(name.to_owned(),key)));
        marked+=crate::admission::mark_dirty(conn,dirty,seq).await?;
    }
    Ok(marked)
}
async fn mark_all(conn:&mut PgConnection,name:&str,reference:&ReferenceInput,seq:i64)->Result<usize>{
    let version=reference.target_kind.database_version().map_err(Error::Invalid)?;
    let mut after:Option<Uuid>=None;let mut marked=0;
    loop {
        let ids:Vec<Uuid>=sqlx::query_scalar("SELECT e.id FROM core.events e WHERE e.event_type=$1 AND e.kind_version=$2 AND ($3::uuid IS NULL OR e.id>$3) AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) ORDER BY e.id LIMIT $4")
            .bind(&reference.target_kind.name).bind(version).bind(after).bind(PAGE).fetch_all(&mut *conn).await?;
        let Some(last)=ids.last() else{break};after=Some(*last);
        marked+=crate::admission::mark_dirty(conn,ids.into_iter().map(|id|(name.to_owned(),key(reference,EventId(id)))).collect(),seq).await?;
    }
    let mut after=None;
    loop {
        let old=crate::privacy::refresh::output_keys_after(conn,name,after.as_ref(),PAGE,crate::PAGE_BYTES).await?;
        let Some(last)=old.last() else{break};after=Some(last.clone());
        marked+=crate::admission::mark_dirty(conn,old.into_iter().map(|key|(name.to_owned(),key)).collect(),seq).await?;
    }
    Ok(marked)
}

#[cfg(test)] mod tests;

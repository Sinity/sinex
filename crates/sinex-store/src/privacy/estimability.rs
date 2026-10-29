//! Required-input checks consume the trace from the single rule execution.
use super::{AffectedPath,projection};
use crate::{Error,Result};
use sinex_model::{Event,InputRequirement,InputUnavailableReason,KindRef,UnavailableInput};
use sqlx::PgConnection;

fn components(pointer:&str)->Option<Vec<String>> {
    if pointer.is_empty(){return Some(vec![])}
    if !pointer.starts_with('/'){return None}
    pointer[1..].split('/').map(|component|{
        let mut out=String::new();let mut chars=component.chars();
        while let Some(c)=chars.next(){if c=='~'{out.push(match chars.next()?{'0'=>'~','1'=>'/',_=>return None})}else{out.push(c)}}
        Some(out)
    }).collect()
}
fn overlaps(required:&str,affected:&str)->bool {
    let (Some(required),Some(affected))=(components(required),components(affected)) else{return true};
    required.iter().zip(&affected).all(|(r,a)|r=="*"||r==a)
}
pub(crate) fn valid(requirements:&[InputRequirement])->bool {
    requirements.iter().all(|r|r.kind.database_version().is_ok()
        &&r.paths.iter().all(|p|components(p).is_some())
        &&r.when.iter().all(|(p,_)|components(p).is_some_and(|parts|!parts.iter().any(|part|part=="*"))))
}
/// Classification retains estimability. A transforming action is evidence
/// even when it produced the same visible value as the original.
pub fn reason(event:&Event,trace:&[AffectedPath],requirements:Option<&[InputRequirement]>)->Option<InputUnavailableReason>{
    reason_fields(&event.event_type,event.kind_version,&event.payload,trace,requirements)
}
fn reason_fields(kind_name:&str,kind_version:u64,payload:&serde_json::Value,trace:&[AffectedPath],requirements:Option<&[InputRequirement]>)->Option<InputUnavailableReason>{
    let affected:Vec<_>=trace.iter().filter(|p|p.action.transforms()).collect();
    if affected.is_empty(){return None}
    let Some(requirements)=requirements.filter(|r|valid(r)) else{return Some(InputUnavailableReason::UndeclaredAffectedInput)};
    let same:Vec<_>=requirements.iter().filter(|r|r.kind.name==kind_name&&r.kind.version==kind_version).collect();
    if same.is_empty(){return Some(InputUnavailableReason::UndeclaredAffectedInput)}
    if same.iter().flat_map(|r|&r.when).any(|(p,_)|affected.iter().any(|a|overlaps(p,&a.path))){
        return Some(InputUnavailableReason::RequiredPathTransformed)
    }
    let applicable:Vec<_>=same.into_iter().filter(|r|r.when.iter().all(|(p,v)|payload.pointer(p)==Some(v))).collect();
    if applicable.is_empty(){return Some(InputUnavailableReason::UndeclaredAffectedInput)}
    applicable.iter().flat_map(|r|&r.paths).any(|p|affected.iter().any(|a|overlaps(p,&a.path)))
        .then_some(InputUnavailableReason::RequiredPathTransformed)
}
/// The caller holds the input-read snapshot. Missing/stale projections never
/// become an empty trace, and the public issue omits affected paths and values.
pub async fn unavailable(conn:&mut PgConnection,events:&[Event],requirements:Option<&[InputRequirement]>)->Result<Vec<UnavailableInput>> {
    let state=projection::epoch(conn).await?;
    if !state.ready(){return Err(Error::Invalid("privacy_projection_stale".into()))}
    let mut issues=Vec::new();
    for page in events.chunks(512){
        let ids:Vec<_>=page.iter().map(|e|e.id.0).collect();
        let rows:Vec<(uuid::Uuid,i64,String,serde_json::Value)>=sqlx::query_as("SELECT event_id,epoch,state,trace FROM privacy.event_outputs WHERE event_id=ANY($1)")
            .bind(ids).fetch_all(&mut *conn).await?;
        for event in page {
            let reason=if sinex_sealing::is_sealed(&event.payload){Some(InputUnavailableReason::ProjectionUnavailable)}else{match rows.iter().find(|r|r.0==event.id.0){
                Some((_,epoch,status,trace)) if *epoch==state.active&&status=="complete"=>match serde_json::from_value::<Vec<AffectedPath>>(trace.clone()) {
                    Ok(trace)=>reason(event,&trace,requirements),Err(_)=>Some(InputUnavailableReason::ProjectionUnavailable)
                },
                _=>Some(InputUnavailableReason::ProjectionUnavailable),
            }};
            if let Some(reason)=reason{issues.push(UnavailableInput{event:event.id,kind:KindRef{name:event.event_type.clone(),version:event.kind_version},reason})}
        }
    }
    Ok(issues)
}
/// A build's computational footprint is immutable like its producer version.
pub async fn register(conn:&mut PgConnection,name:&str,version:u32,build:&str,requirements:Option<&[InputRequirement]>)->Result<()> {
    if requirements.is_some_and(|r|!valid(r)){return Err(Error::Invalid("invalid_derivation_input_requirements".into()))}
    let requirements=serde_json::to_value(requirements).map_err(|e|Error::Invalid(e.to_string()))?;
    let saved:serde_json::Value=sqlx::query_scalar("INSERT INTO privacy.derivation_contracts(derivation,version,build,requirements) VALUES($1,$2,$3,$4) ON CONFLICT(derivation,version,build) DO UPDATE SET requirements=privacy.derivation_contracts.requirements RETURNING requirements")
        .bind(name).bind(i64::from(version)).bind(build).bind(&requirements).fetch_one(conn).await?;
    if saved!=requirements{return Err(Error::Invalid("derivation_input_contract_changed_for_build".into()))}
    Ok(())
}

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum SelectorUnavailable {
    SnapshotRequired,
    ReadScopeChanged,
    UndeclaredSelector,
    ProjectionUnavailable,
    Withheld,
    UnvalidatedSelector,
    RequiredPathMissing,
    RequiredPathTransformed,
}
impl SelectorUnavailable {
    pub fn code(&self)->&'static str {match self {
        Self::SnapshotRequired=>"selector_snapshot_required",
        Self::ReadScopeChanged=>"selector_read_scope_changed",
        Self::UndeclaredSelector=>"selector_footprint_unavailable",
        Self::ProjectionUnavailable=>"selector_projection_unavailable",
        Self::Withheld=>"selector_withheld",
        Self::UnvalidatedSelector=>"selector_validation_unavailable",
        Self::RequiredPathMissing=>"selector_required_path_missing",
        Self::RequiredPathTransformed=>"selector_required_path_transformed",
    }}
}

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum SelectorCoverage {
    Complete {read:crate::ReadMark,through:i64,grants:String},
    Unavailable {reason:SelectorUnavailable},
}

fn present(value:&serde_json::Value,path:&[String])->bool {
    let Some((first,rest))=path.split_first() else{return true};
    if first=="*" {
        match value {
            serde_json::Value::Array(values)=>values.iter().all(|value|present(value,rest)),
            serde_json::Value::Object(values)=>values.values().all(|value|present(value,rest)),
            _=>false,
        }
    }else{match value {
        serde_json::Value::Object(values)=>values.get(first).is_some_and(|value|present(value,rest)),
        serde_json::Value::Array(values)=>(first=="0"||(!first.starts_with('0')&&first.bytes().all(|c|c.is_ascii_digit())))
            &&first.parse::<usize>().ok().and_then(|i|values.get(i)).is_some_and(|value|present(value,rest)),
        _=>false,
    }}
}

pub(crate) fn selector_reason(kind:&KindRef,payload:&serde_json::Value,validation:sinex_model::Validation,trace:&[AffectedPath],requirements:&[InputRequirement])->Option<SelectorUnavailable>{
    match reason_fields(&kind.name,kind.version,payload,trace,Some(requirements)) {
        Some(InputUnavailableReason::RequiredPathTransformed)=>return Some(SelectorUnavailable::RequiredPathTransformed),
        Some(_)=>return Some(SelectorUnavailable::UndeclaredSelector),None=>{},
    }
    let same:Vec<_>=requirements.iter().filter(|r|r.kind.name==kind.name&&r.kind.version==kind.version).collect();
    if same.is_empty(){return Some(SelectorUnavailable::UndeclaredSelector)}
    if same.iter().flat_map(|r|&r.when).any(|(p,_)|!present(payload,&components(p).expect("validated pointer"))){
        return Some(SelectorUnavailable::RequiredPathMissing)
    }
    let applicable:Vec<_>=same.into_iter().filter(|r|r.when.iter().all(|(p,v)|payload.pointer(p)==Some(v))).collect();
    if applicable.is_empty(){return Some(SelectorUnavailable::UndeclaredSelector)}
    if validation!=sinex_model::Validation::Validated&&applicable.iter().any(|r|!r.paths.is_empty()||!r.when.is_empty()){
        return Some(SelectorUnavailable::UnvalidatedSelector)
    }
    applicable.iter().flat_map(|r|&r.paths).any(|p|!present(payload,&components(p).expect("validated pointer")))
        .then_some(SelectorUnavailable::RequiredPathMissing)
}

#[derive(sqlx::FromRow)]
struct SelectorRow {
    id:uuid::Uuid,event_type:String,kind_version:i64,commit_seq:i64,validation:String,
    epoch:Option<i64>,state:Option<String>,payload:Option<serde_json::Value>,trace:Option<serde_json::Value>,visible:bool,
}

/// Establish negative-membership coverage before applying payload predicates.
/// The caller pins its read mark, head and grants in one existing snapshot.
/// Bytes count PostgreSQL's JSON text wire representation, variable metadata
/// text and fixed field overhead, rather than compressed on-disk JSONB size.
/// An oversized first row travels alone; pages never limit candidate coverage.
pub async fn selector_coverage(conn:&mut PgConnection,read:crate::ReadMark,through:i64,kinds:&[KindRef],requirements:Option<&[InputRequirement]>,count:i64,bytes:i64)->Result<SelectorCoverage>{
    let unavailable=|reason|SelectorCoverage::Unavailable{reason};
    if count<=0||bytes<=0{return Err(Error::Invalid("selector page budgets must be positive".into()))}
    let isolation:String=sqlx::query_scalar("SELECT current_setting('transaction_isolation')").fetch_one(&mut *conn).await?;
    if !matches!(isolation.as_str(),"repeatable read"|"serializable"){return Ok(unavailable(SelectorUnavailable::SnapshotRequired))}
    let policy=projection::epoch(conn).await?;
    if !policy.ready()||read.policy_epoch!=Some(policy.active){return Ok(unavailable(SelectorUnavailable::ReadScopeChanged))}
    if crate::ReadMark::now(conn).await?!=read||crate::query::current_seq(conn).await?!=through||through<0{
        return Ok(unavailable(SelectorUnavailable::ReadScopeChanged))
    }
    let Some(requirements)=requirements.filter(|r|valid(r)) else{return Ok(unavailable(SelectorUnavailable::UndeclaredSelector))};
    let mut kinds=kinds.to_vec();kinds.sort();kinds.dedup();
    if kinds.is_empty()||kinds.iter().any(|kind|kind.database_version().is_err()||!requirements.iter().any(|r|r.kind==*kind)){
        return Ok(unavailable(SelectorUnavailable::UndeclaredSelector))
    }
    let names:Vec<_>=kinds.iter().map(|k|k.name.clone()).collect();
    let versions:Vec<_>=kinds.iter().map(|k|k.database_version().expect("validated version")).collect();
    let known:i64=sqlx::query_scalar("SELECT count(*) FROM registry.event_types k JOIN unnest($1::text[],$2::bigint[]) scope(name,version) ON scope.name=k.name AND scope.version=k.version")
        .bind(&names).bind(&versions).fetch_one(&mut *conn).await?;
    if known as usize!=kinds.len(){return Ok(unavailable(SelectorUnavailable::UndeclaredSelector))}
    let grants=crate::query::current_grants(conn).await?;
    let mut after=0;let mut after_id:Option<uuid::Uuid>=None;
    loop {
        let rows:Vec<SelectorRow>=sqlx::query_as(r#"WITH candidates AS MATERIALIZED (
          SELECT e.id,e.event_type,e.kind_version,e.commit_seq,e.validation,o.epoch,o.state,o.payload,o.trace,
            coalesce(core.class_visible(o.class),false) AS visible
          FROM core.events e JOIN unnest($1::text[],$2::bigint[]) scope(name,version) ON scope.name=e.event_type AND scope.version=e.kind_version
          LEFT JOIN privacy.event_outputs o ON o.event_id=e.id
          WHERE e.commit_seq<=$3 AND (e.commit_seq>$4 OR (e.commit_seq=$4 AND $5::uuid IS NOT NULL AND e.id>$5))
            AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
          ORDER BY e.commit_seq,e.id LIMIT $6)
          SELECT id,event_type,kind_version,commit_seq,validation,epoch,state,payload,trace,visible
          FROM(SELECT sized.*,sum(size) OVER(ORDER BY commit_seq,id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)-size AS before
            FROM(SELECT candidates.*,coalesce(octet_length(payload::text),0)::bigint+coalesce(octet_length(trace::text),0)::bigint
              +octet_length(event_type)::bigint+octet_length(validation)::bigint+coalesce(octet_length(state),0)::bigint+128 AS size FROM candidates) sized) bounded
          WHERE before<$7::bigint ORDER BY commit_seq,id"#)
            .bind(&names).bind(&versions).bind(through).bind(after).bind(after_id).bind(count).bind(bytes).fetch_all(&mut *conn).await?;
        if rows.is_empty(){return Ok(SelectorCoverage::Complete{read,through,grants})}
        for row in rows {
            after=row.commit_seq;after_id=Some(row.id);
            if row.epoch!=Some(policy.active)||row.state.as_deref()!=Some("complete"){
                return Ok(unavailable(SelectorUnavailable::ProjectionUnavailable))
            }
            if !row.visible{return Ok(unavailable(SelectorUnavailable::Withheld))}
            let (Some(payload),Some(trace))=(row.payload,row.trace) else{return Ok(unavailable(SelectorUnavailable::ProjectionUnavailable))};
            if sinex_sealing::is_sealed(&payload){return Ok(unavailable(SelectorUnavailable::ProjectionUnavailable))}
            let Ok(trace)=serde_json::from_value::<Vec<AffectedPath>>(trace) else{return Ok(unavailable(SelectorUnavailable::ProjectionUnavailable))};
            let kind=KindRef{name:row.event_type,version:row.kind_version as u64};
            let validation=if row.validation=="validated"{sinex_model::Validation::Validated}else{sinex_model::Validation::Unvalidated};
            if let Some(reason)=selector_reason(&kind,&payload,validation,&trace,requirements){return Ok(unavailable(reason))}
        }
    }
}

#[cfg(test)] mod tests;

//! A single policy epoch covers payloads, searchable text and vector exposure.
//! Original payloads remain in private core.events; the marker records which
//! readings were admitted after original preservation became an invariant.
use crate::{Error,Result,privacy};
use serde_json::{Value,json};
use sinex_model::{DbCheck,Event,EventId,KindRef,Origin,ReadingOrder,Class,EquivalenceKey};
use sqlx::{Connection,PgConnection};
use uuid::Uuid;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct Epoch {pub desired:i64,pub active:i64}
impl Epoch {pub fn ready(self)->bool{self.desired==self.active}}
/// Read inside the caller's snapshot; proof issuance must require ready().
pub async fn epoch(conn:&mut PgConnection)->Result<Epoch>{
    let (desired,active)=sqlx::query_as("SELECT desired_epoch,active_epoch FROM privacy.policy_state WHERE id").fetch_one(conn).await?;
    Ok(Epoch{desired,active})
}
/// Hold through the admission transaction, after exact recorded-frame retry.
pub async fn lock_epoch(conn:&mut PgConnection)->Result<Epoch>{
    let (desired,active)=sqlx::query_as("SELECT desired_epoch,active_epoch FROM privacy.policy_state WHERE id FOR SHARE").fetch_one(conn).await?;
    Ok(Epoch{desired,active})
}
#[derive(sqlx::FromRow)]
struct Input {id:Uuid,commit_seq:i64,source:String,event_type:String,kind_version:i64,payload:Value,class:String,equivalence_key:Option<String>,parents:Option<Vec<Uuid>>,original:bool,custody:Option<bool>,basis_epoch:Option<i64>,legacy_changed:bool,search:String}
const INPUT:&str=r#"SELECT e.id,e.commit_seq,e.source,e.event_type,e.kind_version,e.payload,coalesce(o.baseline_class,e.class) AS class,
 coalesce(o.equivalence_key,e.equivalence_key) AS equivalence_key,e.parents,coalesce(o.recoverable,false) AS original,o.recoverable AS custody,o.basis_epoch,
 (o.event_id IS NOT NULL AND NOT o.recoverable) OR EXISTS(SELECT 1 FROM privacy.rules r WHERE r.source=e.source AND (r.event_type IS NULL OR r.event_type=e.event_type))
 OR EXISTS(SELECT 1 FROM privacy.source_classes f WHERE f.source=e.source)
 OR EXISTS(SELECT 1 FROM core.runs r WHERE r.id=e.run_id AND r.privacy_rules_seq>0) AS legacy_changed,
 k.search FROM core.events e LEFT JOIN privacy.original_events o ON o.event_id=e.id
 JOIN registry.event_types k ON k.name=e.event_type AND k.version=e.kind_version"#;

/// The clone contains only fields RuleSet reads. Original metadata is never
/// replaced by this selector-only value or written back to core.events.
fn transform(input:&Input,rules:&privacy::RuleSet)->Result<(Value,Class,Option<String>,bool,&'static str)>{
    if !input.original&&input.legacy_changed{return Ok((json!({"unavailable":"original_payload_not_retained"}),privacy::parse_class(&input.class)?,None,true,"unavailable_original"))}
    let mut event=Event::submitted(KindRef{name:input.event_type.clone(),version:input.kind_version as u64},input.payload.clone(),input.source.clone(),
        Origin::Derived{parents:input.parents.as_ref().map(|ids|ids.iter().copied().map(EventId).collect()).unwrap_or_default()},ReadingOrder(0),[0;32],privacy::parse_class(&input.class)?);
    event.equivalence_key=input.equivalence_key.clone().map(EquivalenceKey);
    rules.apply(&mut event)?;
    let changed=event.payload!=input.payload;
    Ok((event.payload,event.class,event.equivalence_key.map(|key|key.0),changed,"complete"))
}
async fn project(conn:&mut PgConnection,input:&Input,rules:&privacy::RuleSet,epoch:i64)->Result<()> {
    if input.custody.is_none() {
        // This decision is made once, while retained history is available.
        // Rule deletion later cannot turn an unknown original into custody.
        sqlx::query("INSERT INTO privacy.original_events(event_id,baseline_class,equivalence_key,recoverable) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
          .bind(input.id).bind(&input.class).bind(&input.equivalence_key).bind(!input.legacy_changed).execute(&mut *conn).await?;
    }
    let (mut payload,mut class,mut key,mut changed,mut state)=transform(input,rules)?;
    if let Some(parents)=&input.parents {
        let outputs:Vec<(Uuid,String,bool,String)>=sqlx::query_as("SELECT event_id,class,changed,state FROM privacy.event_outputs WHERE event_id=ANY($1) AND epoch=$2").bind(parents).bind(epoch).fetch_all(&mut *conn).await?;
        let pending=outputs.len()!=parents.iter().copied().collect::<std::collections::HashSet<_>>().len()||outputs.iter().any(|row|row.3=="pending_dependency");
        let unavailable=outputs.iter().any(|row|row.2||row.3!="complete");
        for (_,parent,_,_) in outputs{class=class.max(privacy::parse_class(&parent)?);}
        if pending {payload=json!({"unavailable":"privacy_dependency_pending"});key=None;changed=true;state="pending_dependency";}
        else if unavailable && input.basis_epoch!=Some(epoch) {payload=json!({"unavailable":"privacy_dependency_changed"});key=None;changed=true;state="unavailable_dependency";}
    }
    sqlx::query("INSERT INTO privacy.event_outputs(event_id,epoch,payload,class,equivalence_key,changed,state) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(event_id) DO UPDATE SET epoch=EXCLUDED.epoch,payload=EXCLUDED.payload,class=EXCLUDED.class,equivalence_key=EXCLUDED.equivalence_key,changed=EXCLUDED.changed,state=EXCLUDED.state")
        .bind(input.id).bind(epoch).bind(&payload).bind(privacy::class_name(class)).bind(key).bind(changed).bind(state).execute(&mut *conn).await?;
    sqlx::query("DELETE FROM core.search_text WHERE event_id=$1").bind(input.id).execute(&mut *conn).await?;
    if state=="complete"&&class!=Class::Sealed&&input.search!="none" {
        let text=sinex_model::query::text::payload_text(&payload);
        if !text.is_empty(){sqlx::query("INSERT INTO core.search_text(event_id,text,trigram_indexed) VALUES($1,$2,$3)").bind(input.id).bind(text).bind(input.search=="words_and_trigram").execute(&mut *conn).await?;}
    }
    Ok(())
}
/// Called only for actually admitted rows after keep-known merging. Suppressed
/// duplicates never gain an original-preservation marker retroactively.
pub(crate) async fn admitted(conn:&mut PgConnection,ids:&[Uuid],rules:&privacy::RuleSet)->Result<()> {
    sqlx::query("INSERT INTO privacy.original_events(event_id,baseline_class,equivalence_key,recoverable) SELECT id,class,equivalence_key,true FROM core.events WHERE id=ANY($1) ON CONFLICT DO NOTHING").bind(ids).execute(&mut *conn).await?;
    let state=epoch(conn).await?;
    let sql=format!("{INPUT} WHERE e.id=ANY($1) ORDER BY e.commit_seq,e.id");
    let inputs=sqlx::query_as::<_,Input>(&sql).bind(ids).fetch_all(&mut *conn).await?;
    for input in inputs{project(conn,&input,rules,state.desired).await?;}
    Ok(())
}
/// Only a daemon-derived batch with a current read mark may certify that its
/// output used transformed inputs. Suppression alone is insufficient: the
/// complete offered payload/hash/kind/source/parents must equal the retained
/// original before a repeated identity can receive the new basis epoch.
pub(crate) async fn certify_basis(conn:&mut PgConnection,batch:&sinex_model::Batch,events:&[Event],resolved:&[Option<Uuid>],rules:&privacy::RuleSet)->Result<Vec<Uuid>> {
    let sinex_model::BatchKind::Derived{scope,derivation}=&batch.kind else{return Ok(vec![])};
    let scope:crate::DerivedScope=serde_json::from_value(scope.clone()).map_err(|error|Error::Invalid(format!("derived batch scope: {error}")))?;
    let read=match scope{crate::DerivedScope::Keys{read,..}|crate::DerivedScope::Position{read,..}=>read,_=>return Ok(vec![])};
    let Some(basis)=read.policy_epoch else{return Ok(vec![])};
    let policy=epoch(conn).await?;
    if !policy.ready()||policy.active!=basis{return Err(Error::Invalid("derivation_policy_epoch_changed".into()))}
    let mut restored=Vec::new();let mut restored_types=std::collections::BTreeSet::new();
    for (event,resolved)in events.iter().zip(resolved){
        let Some(id)=resolved else{continue};
        let sinex_model::Origin::Derived{parents}=&event.origin else{continue};
        let parents:Vec<_>=parents.iter().map(|id|id.0).collect();
        let proof=sqlx::query("UPDATE privacy.original_events o SET basis_epoch=$2 FROM core.events e WHERE o.event_id=e.id AND o.recoverable AND o.basis_epoch IS DISTINCT FROM $2 AND e.id=$1 AND e.payload=$3 AND e.record_hash=$4 AND e.parents=$5 AND e.source=$6 AND e.event_type=$7 AND e.kind_version=$8 AND e.record_state=$9 AND e.equivalence_key IS NOT DISTINCT FROM $10")
          .bind(id).bind(basis).bind(&event.payload).bind(event.record_hash.as_slice()).bind(&parents).bind(&event.source).bind(&event.event_type).bind(event.kind_version as i64).bind(event.record_state.as_db()).bind(event.equivalence_key.as_ref().map(|key|key.0.as_str())).execute(&mut *conn).await?;
        if proof.rows_affected()>0 {
            let sql=format!("{INPUT} WHERE e.id=$1");let input=sqlx::query_as::<_,Input>(&sql).bind(id).fetch_one(&mut *conn).await?;
            project(conn,&input,rules,basis).await?;
            if *id!=event.id.0 {restored.push(*id);restored_types.insert(event.event_type.clone());}
        }
    }
    if !restored_types.is_empty() {
        sqlx::query("UPDATE core.counters SET value=value+1 WHERE name='privacy_outputs'").execute(&mut *conn).await?;
        let types:Vec<_>=restored_types.into_iter().collect();
        sqlx::query("DELETE FROM derivation.positions p USING registry.derivations d WHERE p.derivation=d.name AND d.kind='per_event' AND d.inputs && $1::text[] AND d.name<>$2").bind(types).bind(derivation).execute(&mut *conn).await?;
    }
    Ok(restored)
}
/// Each bounded page commits separately while admission continues. Readers
/// stay unavailable until the short final activation covers the current head.
pub async fn ensure_current(conn:&mut PgConnection)->Result<()> {
    loop {if build_page(conn).await?{return Ok(())}}
}
async fn build_page(conn:&mut PgConnection)->Result<bool> {
    let mut tx=conn.begin().await?;
    let policy=lock_epoch(&mut tx).await?;
    sqlx::query("INSERT INTO privacy.output_builds(epoch,after_seq) VALUES($1,0) ON CONFLICT DO NOTHING").bind(policy.desired).execute(&mut *tx).await?;
    let after:i64=sqlx::query_scalar("SELECT after_seq FROM privacy.output_builds WHERE epoch=$1 FOR UPDATE").bind(policy.desired).fetch_one(&mut *tx).await?;
    let head:i64=sqlx::query_scalar("SELECT value FROM core.counters WHERE name='commit_seq'").fetch_one(&mut *tx).await?;
    if policy.ready()&&after>=head {tx.commit().await?;return Ok(true)}
    let hash_key=privacy::hash_key(&mut tx).await?;
    let mut cache=privacy::Cache::new(hash_key);
    let rules=cache.load(&mut tx).await?.clone();
    let sql=format!("{INPUT} JOIN (SELECT id FROM (SELECT id,commit_seq,sum(pg_column_size(e)::bigint) OVER(ORDER BY commit_seq)-pg_column_size(e)::bigint AS before FROM (SELECT * FROM core.events WHERE commit_seq>$1 AND commit_seq<=$3 ORDER BY commit_seq LIMIT 1000) e) bounded WHERE before<$2) page ON page.id=e.id ORDER BY e.commit_seq");
    let inputs=sqlx::query_as::<_,Input>(&sql).bind(after).bind(crate::PAGE_BYTES).bind(head).fetch_all(&mut *tx).await?;
    let covered=inputs.last().map_or(head,|row|row.commit_seq);
    // The same lock order is used by admission/search and excision.
    sqlx::query("LOCK TABLE core.search_text IN SHARE ROW EXCLUSIVE MODE").execute(&mut *tx).await?;
    for input in inputs{project(&mut tx,&input,&rules,policy.desired).await?;}
    sqlx::query("UPDATE privacy.output_builds SET after_seq=$2 WHERE epoch=$1").bind(policy.desired).bind(covered).execute(&mut *tx).await?;
    tx.commit().await?;
    if covered<head{return Ok(false)}
    let mut activation=conn.begin().await?;
    // Exclusive only across the scalar head comparison and activation CAS.
    let current:(i64,i64)=sqlx::query_as("SELECT desired_epoch,active_epoch FROM privacy.policy_state WHERE id FOR UPDATE").fetch_one(&mut *activation).await?;
    let current_head:i64=sqlx::query_scalar("SELECT value FROM core.counters WHERE name='commit_seq'").fetch_one(&mut *activation).await?;
    let activated=current.0==policy.desired&&covered>=current_head;
    if activated {
        sqlx::query("UPDATE privacy.policy_state SET active_epoch=$1 WHERE id AND desired_epoch=$1").bind(policy.desired).execute(&mut *activation).await?;
        sqlx::query("DELETE FROM privacy.output_builds WHERE epoch<>$1").bind(policy.desired).execute(&mut *activation).await?;
    }
    activation.commit().await?;
    Ok(activated)
}
/// Search consumers must read the policy projection, never original payloads.
pub(crate) async fn transformed(conn:&mut PgConnection,events:&[crate::commit_log::Committed])->Result<Vec<crate::commit_log::Committed>> {
    let mut out=Vec::with_capacity(events.len());
    let state=lock_epoch(conn).await?;if !state.ready(){return Err(Error::Invalid("privacy_projection_stale".into()))}
    for event in events {
        let row:Option<(Value,String)>=sqlx::query_as("SELECT payload,class FROM privacy.event_outputs WHERE event_id=$1 AND epoch=$2 AND state='complete'").bind(event.id).bind(state.active).fetch_optional(&mut *conn).await?;
        if let Some((payload,class))=row{let mut event=event.clone();event.payload=payload;event.class=class;out.push(event)}
    }
    Ok(out)
}
#[cfg(test)] mod tests;

/// A derivation that reads source bytes cannot certify transformed input when
/// this reading's configured output differs. Caller grants are a separate gate.
pub async fn raw_event_allowed(conn:&mut PgConnection,event:Uuid,basis:i64)->Result<bool>{
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM privacy.event_outputs o JOIN privacy.policy_state p ON p.id WHERE o.event_id=$1 AND o.epoch=$2 AND p.desired_epoch=$2 AND p.active_epoch=$2 AND o.state='complete' AND NOT o.changed)").bind(event).bind(basis).fetch_one(conn).await?)
}

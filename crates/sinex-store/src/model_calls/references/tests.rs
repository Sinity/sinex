use super::*;
use crate::{AdmissionContext,NoDerivations};
use crate::model_calls::tests::{captured,input,policy,requested,batch};
use sinex_model::{ArchiveRequest,OperationId,InputRequirement,Event,Class,Origin};
use sinex_model::event_types::model_call::{RecordedResponse,Charge};
use sinex_model::model_calls::{Action,Capabilities,OutputHash};

fn descriptor()->ReferenceInput{
    let owner=KindRef{name:OWNER.into(),version:VERSION};
    ReferenceInput{owner_kind:owner.clone(),target_kind:KindRef{name:"media.segment".into(),version:2},key_field:"segment".into(),
        selector:vec![InputRequirement::new(owner,["/targets","/request/operation","/recorded_response"])]}
}
struct References;
impl DirtyKeys for References {
    fn keys(&self,_:&crate::KeyInput<'_>)->Vec<(String,Value)>{vec![]}
    fn reference_consumers(&self)->Vec<(String,ReferenceInput)>{vec![("fixture.media".into(),descriptor())]}
}
async fn submit(conn:&mut AdmissionContext,batch:&sinex_model::Batch,keys:&dyn DirtyKeys){
    let mut tx=conn.begin().await.unwrap();tx.admit(batch,keys).await.unwrap();tx.commit().await.unwrap();
    crate::privacy::projection::ensure_current(conn).await.unwrap();
}
async fn world(db:&devkit::pg::TestDatabase)->(AdmissionContext,Vec<Event>,Event){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();crate::query::set_grants(&mut conn,"guarded").await.unwrap();
    let targets=vec![input("first target"),input("second target")];let policy=captured(&policy("1",35,10));
    submit(&mut conn,&batch(vec![targets[0].clone(),targets[1].clone(),policy.clone()]),&NoDerivations).await;
    (conn,targets,policy)
}
async fn dirty(conn:&mut PgConnection)->std::collections::BTreeSet<EventId>{
    let keys:Vec<Value>=sqlx::query_scalar("SELECT key FROM derivation.dirty WHERE derivation='fixture.media'").fetch_all(conn).await.unwrap();
    keys.into_iter().map(|key|serde_json::from_value(key["segment"].clone()).unwrap()).collect()
}
async fn clear(conn:&mut PgConnection){sqlx::query("DELETE FROM derivation.dirty WHERE derivation='fixture.media'").execute(conn).await.unwrap();}

#[devkit::test]
async fn owner_reference_candidates_are_bounded_id_only_and_bound_to_current_scope(db:devkit::pg::TestDatabase){
    let (mut conn,targets,policy)=world(&db).await;
    let requests=vec![captured(&requested(policy.id,&targets[0])),captured(&requested(policy.id,&targets[1]))];
    submit(&mut conn,&batch(requests.clone()),&References).await;
    let mut tx=conn.begin().await.unwrap();crate::query::read_one_snapshot(&mut tx).await.unwrap();
    let read=ReadMark::now(&mut tx).await.unwrap();let through=crate::query::current_seq(&mut tx).await.unwrap();
    let ReferenceResult::Known(scope)=scope(&mut tx,&descriptor(),read,through).await.unwrap() else{panic!("all typed references are visible")};
    assert_eq!(scope.reference(),&descriptor());assert_eq!(scope.key(targets[0].id),json!({"segment":targets[0].id}));
    let ReferenceResult::Known(first)=candidate_page(&mut tx,&scope,&[targets[0].id],None,1,1).await.unwrap() else{panic!("known scope")};
    assert_eq!(first.ids,[requests[0].id]);
    let ReferenceResult::Known(end)=candidate_page(&mut tx,&scope,&[targets[0].id],first.next,1,1).await.unwrap() else{panic!("known scope")};assert!(end.ids.is_empty());assert!(end.next.is_none());
    crate::query::set_grants(&mut tx,"").await.unwrap();
    assert!(matches!(candidate_page(&mut tx,&scope,&[targets[0].id],None,1,1).await.unwrap(),ReferenceResult::Unavailable(SelectorUnavailable::ReadScopeChanged)));
    tx.rollback().await.unwrap();
    let parents:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE id=ANY($1) AND parents IS NOT NULL").bind(requests.iter().map(|r|r.id.0).collect::<Vec<_>>()).fetch_one(&mut *conn).await.unwrap();assert_eq!(parents,0,"owner references do not turn source records into derived parents");
}

#[devkit::test]
async fn known_request_policy_and_response_changes_dirty_only_indexed_media_targets(db:devkit::pg::TestDatabase){
    let (mut conn,targets,policy)=world(&db).await;let request=requested(policy.id,&targets[0]);
    crate::query::set_grants(&mut conn,"").await.unwrap();
    submit(&mut conn,&batch(vec![captured(&request)]),&References).await;
    assert_eq!(dirty(&mut conn).await,[targets[0].id].into(),"normal Guarded owner references use indexed target invalidation under the private computational scope");
    assert_eq!(crate::query::current_grants(&mut conn).await.unwrap(),"");
    clear(&mut conn).await;crate::query::set_grants(&mut conn,"guarded").await.unwrap();
    let mut reread=request.clone();reread.requested_at=sinex_model::Timestamp::from_unix_secs(101).unwrap();
    submit(&mut conn,&batch(vec![captured(&reread)]),&References).await;
    assert_eq!(dirty(&mut conn).await,[targets[0].id].into(),"a normal replacement must defer its in-flight projection rather than globally invalidate");clear(&mut conn).await;
    let mut authorize=conn.begin().await.unwrap();authorize.model_call("operator",&Action::AuthorizePolicy{policy:policy.id,at:sinex_model::Timestamp::from_unix_secs(20).unwrap()},Capabilities::default()).await.unwrap();authorize.commit().await.unwrap();
    let output=json!({"text":"supplied fixture"});
    let response=captured(&RecordedResponse{request_id:request.request_id.clone(),request:request.request.clone(),output_hash:OutputHash::for_value(&output),output,
        actual:Charge{currency:"TEST".into(),unit:"fixture-unit".into(),units:0},provider_receipt:None,received_at:sinex_model::Timestamp::from_unix_secs(120).unwrap()});
    submit(&mut conn,&batch(vec![response.clone()]),&References).await;assert!(dirty(&mut conn).await.is_empty());
    let mut binding=request;binding.recorded_response=Some(response.id);submit(&mut conn,&batch(vec![captured(&binding)]),&References).await;
    assert_eq!(dirty(&mut conn).await,[targets[0].id].into());clear(&mut conn).await;
    for target in [response.id,policy.id]{
        let mut hide=batch(vec![]);hide.archive.push(ArchiveRequest::Hide{target,operation:OperationId::new()});submit(&mut conn,&hide,&References).await;
        assert_eq!(dirty(&mut conn).await,[targets[0].id].into());clear(&mut conn).await;
        let mut restore=batch(vec![]);restore.archive.push(ArchiveRequest::Unhide{target,operation:OperationId::new()});submit(&mut conn,&restore,&References).await;
        assert_eq!(dirty(&mut conn).await,[targets[0].id].into());clear(&mut conn).await;
    }
}

#[devkit::test]
async fn new_masked_request_invalidates_every_media_key_for_full_incremental_parity(db:devkit::pg::TestDatabase){
    let (mut conn,targets,policy)=world(&db).await;
    submit(&mut conn,&batch(vec![captured(&requested(policy.id,&targets[0]))]),&References).await;clear(&mut conn).await;
    crate::privacy::add_rule(&mut conn,&crate::privacy::NewRule{name:"new request selector".into(),source:"model.fixture".into(),event_type:Some(OWNER.into()),field:Some("/targets".into()),matcher:"pattern".into(),matcher_arg:Some(targets[1].id.to_string()),class:Class::Open,action:crate::privacy::Action::Mask,author:"fixture".into()}).await.unwrap();
    crate::privacy::projection::ensure_current(&mut conn).await.unwrap();
    submit(&mut conn,&batch(vec![captured(&requested(policy.id,&targets[1]))]),&References).await;
    assert_eq!(dirty(&mut conn).await,targets.iter().map(|target|target.id).collect(),"an opaque NEW request cannot leave an unrelated local fallback stale");
    let mut tx=conn.begin().await.unwrap();crate::query::read_one_snapshot(&mut tx).await.unwrap();let read=ReadMark::now(&mut tx).await.unwrap();let through=crate::query::current_seq(&mut tx).await.unwrap();
    assert!(matches!(scope(&mut tx,&descriptor(),read,through).await.unwrap(),ReferenceResult::Unavailable(SelectorUnavailable::RequiredPathTransformed)));
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn prior_current_request_version_cannot_prove_absence_from_current_owner_index(db:devkit::pg::TestDatabase){
    let (mut conn,targets,policy)=world(&db).await;let request=captured(&requested(policy.id,&targets[0]));
    let Origin::Record{location,output_index}=&request.origin else{panic!("source record")};
    let mut tx=conn.begin().await.unwrap();let position:i64=sqlx::query_scalar("UPDATE core.counters SET value=value+1 WHERE name='commit_seq' RETURNING value").fetch_one(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,output_index,ts_quality,commit_seq,payload,reading_order,record_hash,kind_version) VALUES($1,'model.request','model.fixture',$2,$3,$4,$5,'unknown',$6,$7,1,$8,1)")
        .bind(request.id.0).bind(sinex_model::RunId::new().0).bind(location.material.0).bind(location.anchor as i64).bind(*output_index as i64).bind(position).bind(&request.payload).bind(request.record_hash.as_slice()).execute(&mut *tx).await.unwrap();
    mark_changed(&mut tx,&[request.id.0],position,&References).await.unwrap();tx.commit().await.unwrap();
    assert_eq!(dirty(&mut conn).await,targets.iter().map(|target|target.id).collect());
    let mut tx=conn.begin().await.unwrap();crate::query::read_one_snapshot(&mut tx).await.unwrap();let read=ReadMark::now(&mut tx).await.unwrap();let through=crate::query::current_seq(&mut tx).await.unwrap();
    assert!(matches!(scope(&mut tx,&descriptor(),read,through).await.unwrap(),ReferenceResult::Unavailable(SelectorUnavailable::UndeclaredSelector)));tx.rollback().await.unwrap();
}

use super::*;
use crate::{AdmissionContext,NoDerivations};
use sinex_model::{Batch,BatchKind,MaterialId,RecordLocation,RecordOutcome,RunId,event_types::operator::LogEntry};
use privacy::{Action,NewRule};

fn document(source:&str,text:&str)->Event {
    Event::build(&LogEntry{written_at:None,label:Some("ordinary label".into()),text:text.into()},source,
      Origin::Record{location:RecordLocation{material:MaterialId::new(),anchor:0},output_index:0},ReadingOrder(1),*blake3::hash(text.as_bytes()).as_bytes(),Class::Open).unwrap()
}
async fn admit(conn:&mut AdmissionContext,events:Vec<Event>){
    let batch=Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},records:vec![RecordOutcome::Events{location:None,events}],archive:vec![]};
    let mut tx=conn.begin().await.unwrap();tx.admit(&batch,&NoDerivations).await.unwrap();tx.commit().await.unwrap();
}
fn rule()->NewRule{NewRule{name:"configured field".into(),source:"fixture.policy".into(),event_type:Some("operator_log.entry".into()),field:Some("/text".into()),matcher:"pattern".into(),matcher_arg:Some("leafsecret".into()),class:Class::Open,action:Action::Mask,author:"fixture".into()}}
async fn visible(conn:&mut PgConnection,id:Uuid)->Value{sqlx::query_scalar("SELECT payload FROM core.visible_events WHERE id=$1").bind(id).fetch_one(conn).await.unwrap()}

#[devkit::test]
async fn configured_epoch_transforms_encoded_sql_search_and_recovers_new_originals(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();
    let protected=document("fixture.policy","prefix leafsecret suffix");let other=document("fixture.unrelated","leafsecret unrelated");let id=protected.id.0;let other_id=other.id.0;
    let raw=protected.payload.clone();admit(&mut conn,vec![protected,other.clone()]).await;
    assert_eq!(visible(&mut conn,id).await,raw);assert_eq!(visible(&mut conn,other_id).await,other.payload);
    privacy::add_rule(&mut conn,&rule()).await.unwrap();
    assert!(!epoch(&mut conn).await.unwrap().ready());
    let error=sqlx::query_scalar::<_,Value>("SELECT payload FROM core.visible_events WHERE id=$1").bind(id).fetch_one(&mut *conn).await.unwrap_err();
    assert!(error.to_string().contains("privacy_projection_stale"),"{error}");
    ensure_current(&mut conn).await.unwrap();
    let output=visible(&mut conn,id).await;assert_eq!(output["text"],"prefix [masked] suffix");assert_eq!(output["label"],"ordinary label");
    assert_eq!(visible(&mut conn,other_id).await,other.payload);
    let encoded=format!("SELECT convert_from(decode(encode(convert_to(payload->>'text','UTF8'),'base64'),'base64'),'UTF8') AS value FROM core.visible_events WHERE id='{id}'::uuid");
    let rows=crate::query::read_sql::run(&mut conn,&encoded).await.unwrap();assert_eq!(rows[0]["value"],"prefix [masked] suffix");
    let search:i64=sqlx::query_scalar("SELECT count(*) FROM core.visible_search_text WHERE event_id=$1 AND text LIKE '%leafsecret%'").bind(id).fetch_one(&mut *conn).await.unwrap();assert_eq!(search,0);
    let original:Value=sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1").bind(id).fetch_one(&mut *conn).await.unwrap();assert_eq!(original,raw);
    for table in ["core.events","privacy.original_events","privacy.event_outputs"]{
        let encoded=format!("SELECT encode(convert_to(row_to_json(e)::text,'UTF8'),'base64') FROM {table} e");
        assert!(crate::query::read_sql::run(&mut conn,&encoded).await.is_err(),"private original table {table}");
    }
    privacy::end_rule(&mut conn,"configured field","fixture").await.unwrap();ensure_current(&mut conn).await.unwrap();
    assert_eq!(visible(&mut conn,id).await,raw);
    let search:i64=sqlx::query_scalar("SELECT count(*) FROM core.visible_search_text WHERE event_id=$1 AND text LIKE '%leafsecret%'").bind(id).fetch_one(&mut *conn).await.unwrap();assert_eq!(search,1);
}

#[devkit::test]
async fn lost_legacy_original_is_unavailable_and_failed_activation_remains_stale(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let event=document("fixture.policy","[masked]");let id=event.id.0;admit(&mut conn,vec![event]).await;
    // Removing the preservation marker simulates a predecessor row. Its
    // historical content is deliberately never promoted to a known original.
    sqlx::query("DELETE FROM privacy.original_events WHERE event_id=$1").bind(id).execute(&mut *conn).await.unwrap();
    privacy::add_rule(&mut conn,&rule()).await.unwrap();
    let mut outer=conn.begin().await.unwrap();ensure_current(&mut outer).await.unwrap();assert!(epoch(&mut outer).await.unwrap().ready());outer.rollback().await.unwrap();
    assert!(!epoch(&mut conn).await.unwrap().ready());ensure_current(&mut conn).await.unwrap();assert_eq!(visible(&mut conn,id).await["unavailable"],"unavailable_original");
    privacy::end_rule(&mut conn,"configured field","fixture").await.unwrap();ensure_current(&mut conn).await.unwrap();
    assert_eq!(visible(&mut conn,id).await["unavailable"],"unavailable_original");
    sqlx::query("DELETE FROM privacy.rules").execute(&mut *conn).await.unwrap();ensure_current(&mut conn).await.unwrap();
    assert_eq!(visible(&mut conn,id).await["unavailable"],"unavailable_original","privileged history deletion cannot create custody");
    let custody:bool=sqlx::query_scalar("SELECT recoverable FROM privacy.original_events WHERE event_id=$1").bind(id).fetch_one(&mut *conn).await.unwrap();assert!(!custody);
    let n:i64=sqlx::query_scalar("SELECT count(*) FROM core.visible_search_text WHERE event_id=$1").bind(id).fetch_one(&mut *conn).await.unwrap();assert_eq!(n,0);
}

#[devkit::test]
async fn new_admission_during_stale_epoch_keeps_original_and_activates_as_one_snapshot(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();privacy::add_rule(&mut conn,&rule()).await.unwrap();
    let event=document("fixture.policy","leafsecret");let id=event.id.0;let original=event.payload.clone();admit(&mut conn,vec![event]).await;
    assert!(!epoch(&mut conn).await.unwrap().ready());
    let raw:Value=sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1").bind(id).fetch_one(&mut *conn).await.unwrap();assert_eq!(raw,original);
    ensure_current(&mut conn).await.unwrap();assert_eq!(visible(&mut conn,id).await["text"],"[masked]");
}

#[devkit::test]
async fn bounded_pages_admit_new_readings_without_partial_epoch_activation(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();
    let events:Vec<_>=(0..1001).map(|n|document("fixture.policy",&format!("leafsecret {n}"))).collect();
    admit(&mut conn,events).await;privacy::add_rule(&mut conn,&rule()).await.unwrap();
    assert!(!build_page(&mut conn).await.unwrap(),"first transfer page cannot activate a partial prefix");
    assert!(!epoch(&mut conn).await.unwrap().ready());
    let late=document("fixture.policy","late leafsecret");let late_id=late.id.0;admit(&mut conn,vec![late]).await;
    ensure_current(&mut conn).await.unwrap();assert_eq!(visible(&mut conn,late_id).await["text"],"late [masked]");
    let (all,masked):(i64,i64)=sqlx::query_as("SELECT count(*),count(*) FILTER(WHERE payload->>'text' LIKE '%[masked]%') FROM privacy.event_outputs WHERE state='complete'").fetch_one(&mut *conn).await.unwrap();
    assert_eq!((all,masked),(1002,1002));
}

fn derived_command(parent:EventId,text:&str)->Event {
    use sinex_model::{Timestamp,event_types::CommandExecuted};
    Event::build(&CommandExecuted{native_id:"derived".into(),command:text.into(),cwd:"/fixture".into(),session:"fixture".into(),hostname:"fixture".into(),started_at:Timestamp::from_unix_secs(1).unwrap(),duration_ns:None,exit_code:None,deleted_at:None,author:None,intent:None,shell:None,extra:Default::default()},
        "fixture.derived",Origin::Derived{parents:vec![parent]},ReadingOrder(1),*blake3::hash(text.as_bytes()).as_bytes(),Class::Open).unwrap()
}
fn recomputation(parent:EventId,event:Event,read:crate::ReadMark,position:i64)->Batch {
    Batch{run:RunId::new(),kind:BatchKind::Derived{derivation:"fixture.derived".into(),scope:crate::DerivedScope::Position{inputs:vec![parent.0],position,read}.to_value()},records:vec![RecordOutcome::Events{location:None,events:vec![event]}],archive:vec![]}
}
#[devkit::test]
async fn repeated_identity_requires_exact_current_transformed_basis(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let parent=document("fixture.policy","leafsecret");let parent_id=parent.id;admit(&mut conn,vec![parent]).await;
    privacy::add_rule(&mut conn,&rule()).await.unwrap();ensure_current(&mut conn).await.unwrap();
    let child=derived_command(parent_id,"[masked]");let child_id=child.id;admit(&mut conn,vec![child.clone()]).await;
    assert_eq!(visible(&mut conn,child_id.0).await["unavailable"],"unavailable_dependency");
    let read=crate::ReadMark::now(&mut conn).await.unwrap();let head=crate::query::current_seq(&mut conn).await.unwrap();
    let mut forged=child.clone();forged.id=EventId::new();forged.payload["command"]=json!("different supplied output");
    let batch=recomputation(parent_id,forged,read,head);let mut tx=conn.begin().await.unwrap();let result=tx.admit(&batch,&NoDerivations).await.unwrap();assert_eq!(result.repeats,1);tx.commit().await.unwrap();
    assert_eq!(visible(&mut conn,child_id.0).await["unavailable"],"unavailable_dependency","same hash alone is insufficient evidence");
    let revision:i64=sqlx::query_scalar("SELECT value FROM core.counters WHERE name='privacy_outputs'").fetch_one(&mut *conn).await.unwrap();
    let mut same=child;same.id=EventId::new();let batch=recomputation(parent_id,same,read,head);let mut tx=conn.begin().await.unwrap();let result=tx.admit(&batch,&NoDerivations).await.unwrap();assert_eq!(result.repeats,1);tx.commit().await.unwrap();
    assert_eq!(visible(&mut conn,child_id.0).await["command"],"[masked]");
    let changed:i64=sqlx::query_scalar("SELECT value FROM core.counters WHERE name='privacy_outputs'").fetch_one(&mut *conn).await.unwrap();assert_eq!(changed,revision+1);
    let basis:Option<i64>=sqlx::query_scalar("SELECT basis_epoch FROM privacy.original_events WHERE event_id=$1").bind(child_id.0).fetch_one(&mut *conn).await.unwrap();assert_eq!(basis,read.policy_epoch);
    privacy::end_rule(&mut conn,"configured field","fixture").await.unwrap();ensure_current(&mut conn).await.unwrap();
    let mut stale=derived_command(parent_id,"leafsecret");stale.reading_order=ReadingOrder(2);let batch=recomputation(parent_id,stale,read,head);let mut tx=conn.begin().await.unwrap();let error=tx.admit(&batch,&NoDerivations).await.unwrap_err();assert!(error.to_string().contains("derivation_policy_epoch_changed"));tx.rollback().await.unwrap();
}


#[devkit::test]
async fn retained_history_proves_an_unmutated_legacy_row_before_future_rules(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let event=document("fixture.policy","leafsecret");let id=event.id.0;let original=event.payload.clone();admit(&mut conn,vec![event]).await;
    sqlx::query("DELETE FROM privacy.original_events WHERE event_id=$1").bind(id).execute(&mut *conn).await.unwrap();
    // First retained-history build has no configured historical rule or floor.
    ensure_current(&mut conn).await.unwrap();
    let custody:bool=sqlx::query_scalar("SELECT recoverable FROM privacy.original_events WHERE event_id=$1").bind(id).fetch_one(&mut *conn).await.unwrap();assert!(custody);
    privacy::add_rule(&mut conn,&rule()).await.unwrap();ensure_current(&mut conn).await.unwrap();assert_eq!(visible(&mut conn,id).await["text"],"[masked]");
    privacy::end_rule(&mut conn,"configured field","fixture").await.unwrap();ensure_current(&mut conn).await.unwrap();assert_eq!(visible(&mut conn,id).await,original);
}

#[devkit::test]
async fn transformed_source_key_is_not_resolved_from_the_private_original(db:devkit::pg::TestDatabase){
    use sinex_model::query::Reference;
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let parent=document("fixture.policy","ordinary");let parent_id=parent.id;admit(&mut conn,vec![parent]).await;
    let child=derived_command(parent_id,"ordinary command");let id=child.id;let original_key=child.equivalence_key.as_ref().unwrap().0.clone();admit(&mut conn,vec![child]).await;
    let original_ref=Reference::Key{source:"fixture.derived".into(),event_type:"command.executed".into(),key:original_key};
    assert_eq!(crate::query::resolve(&mut conn,&original_ref).await.unwrap(),id.0);
    privacy::add_rule(&mut conn,&NewRule{name:"configured key".into(),source:"fixture.derived".into(),event_type:Some("command.executed".into()),field:Some("/native_id".into()),matcher:"pattern".into(),matcher_arg:Some("derived".into()),class:Class::Guarded,action:Action::Mask,author:"fixture".into()}).await.unwrap();ensure_current(&mut conn).await.unwrap();
    assert!(crate::query::resolve(&mut conn,&original_ref).await.is_err());
    let shown=crate::query::show(&mut conn,&Reference::Event(id.0)).await.unwrap();assert_eq!(shown["payload"],json!({"withheld":"guarded"}));assert_eq!(shown["id"],json!(id));
    crate::query::set_grants(&mut conn,"guarded").await.unwrap();assert!(crate::query::resolve(&mut conn,&original_ref).await.is_err());
    let shown=crate::query::show(&mut conn,&Reference::Event(id.0)).await.unwrap();assert_eq!(shown["payload"]["native_id"],"[masked]");
    let projected_ref=Reference::Key{source:"fixture.derived".into(),event_type:"command.executed".into(),key:shown["key"].as_str().unwrap().into()};assert_eq!(crate::query::resolve(&mut conn,&projected_ref).await.unwrap(),id.0);
}

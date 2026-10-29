use super::*;
use sinex_protocol::{CoverageProof,CoverageRequest,CoverageStatus,InputReplayReply,ReplayContinuation,ReplayQuery,ReplayReply};
const RAW:&[&str]=&["input_observation","blob","session","message","document","event","link","background","repository","parse_progress"];
fn declaration(name:&str)->sinex_model::KindDeclaration{
    let mut kind=sinex_model::KindDeclaration::schemaless(sinex_model::KindRef{name:format!("polylogue.{name}"),version:1},"polylogue".into());
    kind.payload_schema=Some(json!({"type":"object","properties":{"id":{"type":"string"},"text":{"type":"string"}},"required":["id","text"],"additionalProperties":false}));
    kind.key_rule=Some(sinex_model::kinds::KeyRule::Fields{fields:vec!["id".into()]});kind.replace_rule=Some(sinex_model::ReplaceRule::ReplaceWhenNewer);kind
}
async fn setup(db:devkit::pg::TestDatabase)->(Harness,MaterialId,Vec<EventId>,sinex_model::KindDeclaration){
    let day=query_language::time::DaySetting::new(jiff::tz::TimeZone::get("UTC").unwrap(),jiff::civil::Time::constant(4,0,0,0)).unwrap();
    let config:crate::signals::Config=serde_json::from_value(json!({"interrupt_budget_per_hour":2,"quiet_hours":null,"notifications":null,"outlet_timeout_ms":1000})).unwrap();
    let provider=crate::public_context::Provider::new(day.clone().wire(),"test-host".into(),Some(&config)).unwrap();
    let now=Timestamp::now();let clock=i64::try_from(now.unix_ns()/1_000_000).unwrap()-1000;
    let checkpoint=sinex_store::signals::Checkpoint{settings_identity:provider.settings_identity.clone().unwrap(),evaluator:signals::Evaluator::new(clock,vec![],signals::Policy{private:vec![]},&signals::DayCalendar::new(day)).unwrap(),host_policy:signals::HostPolicy::new(signals::InterruptBudget{per_hour:2,handoffs:vec![]},None),notifications:Default::default(),forwarded:Default::default(),outbox:Default::default(),rule_events:Default::default(),rules:Default::default(),match_basis:Default::default(),current_inputs:Default::default(),rows:Default::default(),attempts:Default::default(),observed_seq:0,passive_count:0};
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();sinex_store::signals::save(&mut conn,"test-host",None,&checkpoint).await.unwrap();drop(conn);
    let raw=RAW.iter().map(|name|declaration(name)).collect::<Vec<_>>();let snapshot=declaration("work_snapshot");
    let mut write=test_write("polylogue",raw.clone());write.canonical_prefixes=vec!["polylogue.work_snapshot".into()];write.canonical_kinds=vec![snapshot.clone()];
    let grants=crate::ingress::fetch::Grants{public_context:true,kind_prefixes:vec!["polylogue".into()],classes:vec![sinex_model::Class::Open,sinex_model::Class::Guarded],write:Some(write.clone())};
    let mut narrow=crate::ingress::fetch::Grants{kind_prefixes:vec!["polylogue.session".into()],..grants.clone()};
    narrow.write.as_mut().unwrap().canonical_prefixes.clear();
    let h=Harness::start_with_policy(db,std::collections::BTreeMap::from([("polylogue".into(),grants),("narrow".into(),narrow)]),Some(provider)).await;
    assert!(h.send("polylogue",&Ingress::RegisterKinds{frame:"coverage-kinds".into(),declarations:raw.iter().cloned().chain([snapshot.clone()]).collect()}).await.refusal.is_none());
    let (material,frame)=h.material_for("polylogue","coverage-material",b"synthetic provider array\n");assert!(h.send("polylogue",&frame).await.refusal.is_none());
    let message=raw.iter().find(|kind|kind.kind.name=="polylogue.message").unwrap();
    let events=(0..257).map(|index|ProducerEvent{kind:message.kind.clone(),payload:json!({"id":format!("message-{index}"),"text":"synthetic"}),location:Some(RecordLocation{material,anchor:0}),output_index:index,parents:vec![],reading_order:Some(0),class:sinex_model::Class::Open}).collect();
    let receipt=h.send("polylogue",&Ingress::Events{frame:"coverage-inputs".into(),source:Some(test_source("polylogue")),coverage:None,events,failures:vec![]}).await;assert_eq!(receipt.refusal,None);
    (h,material,receipt.admitted,snapshot)
}
async fn page(h:&Harness,producer:&str,cache:&str,cursor:Option<String>)->InputReplayReply{
    let reply:ReplayReply=sinex_transport::replies::request_json(&h.client,h.subjects.replay_scope(producer),producer,&ReplayQuery::Inputs{cache:cache.into(),cursor},Duration::from_secs(20)).await.unwrap();
    let ReplayReply::Inputs{result}=reply else{panic!("input coverage reply")};result
}
async fn proof(h:&Harness,basis:CoverageProof,position:i64)->CoverageStatus{
    let context:sinex_protocol::PublicContext=sinex_transport::replies::request_json(&h.client,h.subjects.public_context("polylogue"),"polylogue",&sinex_protocol::PublicContextRequest{coverage:Some(CoverageRequest{basis:Some(basis),cache:"cache".into(),completed_position:position})},Duration::from_secs(20)).await.unwrap();
    let sinex_protocol::PublicContext::Available{coverage,..}=context else{panic!("configured public policy")};coverage
}
#[devkit::test]
async fn paged_owner_replay_survives_later_admission_and_lifecycle_but_stamps_current_policy(db:devkit::pg::TestDatabase){
    let (h,material,ids,snapshot)=setup(db).await;
    let first=page(&h,"polylogue","cache",None).await;
    let InputReplayReply::Page{position,inputs,continuation:ReplayContinuation::Next{cursor},..}=first else{panic!("first bounded page")};
    assert_eq!(inputs.len(),256);assert!(inputs.iter().all(|input|!input.archived&&input.lifecycle_position.is_none()));
    assert!(matches!(page(&h,"polylogue","other-cache",Some(cursor.clone())).await,InputReplayReply::Incomplete{..}));
    assert!(matches!(page(&h,"narrow","cache",None).await,InputReplayReply::Incomplete{..}));
    let replacement=ProducerEvent{kind:declaration("message").kind,payload:json!({"id":"message-0","text":"later reading"}),location:Some(RecordLocation{material,anchor:0}),output_index:0,parents:vec![],reading_order:Some(1),class:sinex_model::Class::Open};
    assert!(h.send("polylogue",&Ingress::Events{frame:"later-reading".into(),source:Some(test_source("polylogue")),coverage:None,events:vec![replacement],failures:vec![]}).await.refusal.is_none());
    let last=page(&h,"polylogue","cache",Some(cursor)).await;
    let InputReplayReply::Page{position:final_position,inputs,continuation:ReplayContinuation::Complete{basis},..}=last else{panic!("complete retained as-of snapshot")};assert_eq!(position,final_position);assert_eq!(inputs.len(),1);
    let CoverageStatus::Complete{proof:write_proof}=proof(&h,basis,position).await else{panic!("owner-signed complete basis")};
    let frozen=Ingress::Events{frame:"canonical-snapshot".into(),source:None,coverage:Some(write_proof),events:vec![ProducerEvent{kind:snapshot.kind,payload:json!({"id":"session","text":"complete snapshot"}),location:None,output_index:0,parents:vec![ids[1]],reading_order:Some(position),class:sinex_model::Class::Open}],failures:vec![]};
    let receipt=h.send("polylogue",&frozen).await;assert_eq!(receipt.refusal,None);
    let mut conn=PgConnection::connect(&h.url).await.unwrap();
    sinex_store::privacy::add_rule(&mut conn,&sinex_store::privacy::NewRule{name:"synthetic-required-text".into(),source:"polylogue".into(),event_type:Some("polylogue.message".into()),field:Some("/text".into()),matcher:"all".into(),matcher_arg:None,author:"synthetic-test".into(),class:sinex_model::Class::Guarded,action:sinex_store::privacy::Action::Mask}).await.unwrap();
    sinex_store::privacy::projection::ensure_current(&mut conn).await.unwrap();
    assert_eq!(h.send("polylogue",&frozen).await,receipt,"exact retry precedes proof freshness");
    let mut fresh_name=frozen;let Ingress::Events{frame,..}=&mut fresh_name else{unreachable!()};*frame="stale-proof".into();assert!(h.send("polylogue",&fresh_name).await.refusal.is_some());
    assert!(matches!(page(&h,"polylogue","cache",None).await,InputReplayReply::Incomplete{..}),"a transformed required fact never becomes complete from its E/hash alone");
}
#[devkit::test]
async fn pinned_excision_and_missing_basis_refuse_replay_authority(db:devkit::pg::TestDatabase){
    let (h,material,_,_)=setup(db).await;
    let context:sinex_protocol::PublicContext=sinex_transport::replies::request_json(&h.client,h.subjects.public_context("polylogue"),"polylogue",&sinex_protocol::PublicContextRequest{coverage:Some(CoverageRequest{cache:"cache".into(),completed_position:0,basis:None})},Duration::from_secs(20)).await.unwrap();
    assert!(matches!(context,sinex_protocol::PublicContext::Available{coverage:CoverageStatus::Incomplete{..},..}));
    let InputReplayReply::Page{continuation:ReplayContinuation::Next{cursor},..}=page(&h,"polylogue","cache",None).await else{panic!("first page")};
    let erased:ExciseReply=sinex_transport::replies::request_json(&h.client,h.subjects.excise("polylogue"),"polylogue",&Excise{materials:vec![material]},Duration::from_secs(20)).await.unwrap();assert_eq!(erased.refusal,None);
    assert!(matches!(page(&h,"polylogue","cache",Some(cursor)).await,InputReplayReply::Incomplete{..}));
}

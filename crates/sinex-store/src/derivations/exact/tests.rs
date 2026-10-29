use super::*;
use crate::{AdmissionContext,NoDerivations};
use sinex_model::{Batch,BatchKind,RecordOutcome,RunId,OperationId,ArchiveRequest,event_types::operator::LogEntry};
fn document(text:&str)->Event {
    Event::build(&LogEntry{written_at:None,label:None,text:text.into()},"fixture.exact",Origin::Record{location:RecordLocation{material:MaterialId::new(),anchor:0},output_index:0},ReadingOrder(1),*blake3::hash(text.as_bytes()).as_bytes(),Class::Open).unwrap()
}
#[devkit::test]
async fn exact_targets_are_paged_live_transformed_and_keep_unknown_times(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let events=[document("archived"),document("secret"),document("kept"),document("unrelated")];let ids:Vec<_>=events.iter().map(|event|event.id).collect();
    let batch=Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},records:vec![RecordOutcome::Events{location:None,events:events.to_vec()}],archive:vec![]};
    let mut tx=conn.begin().await.unwrap();tx.admit(&batch,&NoDerivations).await.unwrap();tx.commit().await.unwrap();
    let hide=Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},records:vec![],archive:vec![ArchiveRequest::Hide{target:ids[0],operation:OperationId::new()}]};let mut tx=conn.begin().await.unwrap();tx.admit(&hide,&NoDerivations).await.unwrap();tx.commit().await.unwrap();
    crate::privacy::add_rule(&mut conn,&crate::privacy::NewRule{name:"configured exact field".into(),source:"fixture.exact".into(),event_type:Some("operator_log.entry".into()),field:Some("/text".into()),matcher:"pattern".into(),matcher_arg:Some("secret".into()),class:Class::Open,action:crate::privacy::Action::Mask,author:"fixture".into()}).await.unwrap();crate::privacy::projection::ensure_current(&mut conn).await.unwrap();
    let mut reader=ExactInputs::new(vec![ids[0],ids[1],ids[1],ids[2],EventId::new()],1,1).unwrap();
    let first=reader.next(&mut conn).await.unwrap().unwrap();let second=reader.next(&mut conn).await.unwrap().unwrap();assert!(reader.next(&mut conn).await.unwrap().is_none());
    assert_eq!(first.len(),1);assert_eq!(second.len(),1);assert_eq!(first[0].event.id,ids[1]);assert_eq!(second[0].event.id,ids[2]);assert_eq!(first[0].event.payload["text"],"[masked]");assert!(first[0].event.ts_orig.at.is_none());assert!(second[0].event.ts_orig.at.is_none());
    assert_eq!(first[0].event.record_hash,events[1].record_hash,"canonical identity is retained separately from transformed values");
}

#[devkit::test]
async fn exact_targets_never_expand_to_unrequested_retained_position_siblings(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let events=[document("first requested"),document("unrequested sibling"),document("second requested")];
    let expected:std::collections::BTreeSet<_>=[events[0].id,events[2].id].into();
    // Normal admission assigns distinct positions. Insert an explicitly
    // synthetic retained tie without changing any immutable event.
    let mut tx=conn.begin().await.unwrap();
    for event in &events {
        let Origin::Record{location,output_index}=&event.origin else{panic!("record fixture")};
        sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,output_index,ts_quality,commit_seq,payload,reading_order,record_hash,kind_version) VALUES($1,$2,$3,$4,$5,$6,$7,'unknown',3,$8,1,$9,$10)")
            .bind(event.id.0).bind(&event.event_type).bind(&event.source).bind(RunId::new().0).bind(location.material.0).bind(location.anchor as i64).bind(*output_index as i64).bind(&event.payload).bind(event.record_hash.as_slice()).bind(event.kind_version as i64).execute(&mut *tx).await.unwrap();
    }
    sqlx::query("UPDATE core.counters SET value=3 WHERE name='commit_seq'").execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,event_type) VALUES(3,NULL,NULL)").execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();crate::privacy::projection::ensure_current(&mut conn).await.unwrap();
    let mut reader=ExactInputs::new(expected.iter().copied().collect(),1,1).unwrap();let mut seen=std::collections::BTreeSet::new();let mut pages=0;
    while let Some(page)=reader.next(&mut conn).await.unwrap(){assert_eq!(page.len(),1);assert!(expected.contains(&page[0].event.id));assert!(seen.insert(page[0].event.id));pages+=1;}
    assert_eq!(pages,2);assert_eq!(seen,expected);
}

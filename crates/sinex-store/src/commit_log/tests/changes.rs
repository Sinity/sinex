use super::*;
use crate::commit_log::{Change,ChangeCursor,changes_page};
use sinex_model::{ArchiveRequest,Batch,BatchKind,Class,Event,MaterialId,OperationId,Origin,ReadingOrder,RecordLocation,RecordOutcome,RunId,Timestamp};

fn event(command:&str,order:i64)->Event{
    let payload=sinex_model::event_types::CommandExecuted{native_id:"fixture-command".into(),command:command.into(),
        cwd:"/fixture".into(),session:"fixture-shell".into(),hostname:"fixture-host".into(),started_at:Timestamp::from_unix_secs(100).unwrap(),
        duration_ns:None,exit_code:Some(0),deleted_at:None,author:None,intent:None,shell:None,extra:Default::default()};
    Event::build(&payload,"fixture.commands",Origin::Record{location:RecordLocation{material:MaterialId::new(),anchor:0},output_index:0},
        ReadingOrder(order),*blake3::hash(command.as_bytes()).as_bytes(),Class::Open).unwrap()
}
async fn admit(conn:&mut crate::AdmissionContext,events:Vec<Event>,archive:Vec<ArchiveRequest>){
    let batch=Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},
        records:vec![RecordOutcome::Events{location:None,events}],archive};
    let mut tx=conn.begin().await.unwrap();tx.admit(&batch,&crate::NoDerivations).await.unwrap();tx.commit().await.unwrap();
}
fn tuple(entry:&crate::commit_log::CommittedChange)->Option<(i64,Uuid,&'static str)>{match &entry.change{
    Change::Event(event)=>Some((entry.position,event.id,"event")),
    Change::Archive(change)=>Some((entry.position,change.archive.event_id,if change.archive.archived{"archived"}else{"unhidden"})),
    Change::Excised(change)=>Some((entry.position,change.event_id,"excised")),Change::PositionComplete=>None}}
#[devkit::test]
async fn canonical_reader_preserves_event_and_visibility_order_across_tiny_pages(db:devkit::pg::TestDatabase){
    let mut conn=crate::AdmissionContext::connect(&db.url(None)).await.unwrap();
    let first=event("first",1);let first_id=first.id;admit(&mut conn,vec![first],vec![]).await;
    let second=event("second",2);let second_id=second.id;admit(&mut conn,vec![second],vec![]).await;
    admit(&mut conn,vec![],vec![ArchiveRequest::Hide{target:second_id,operation:OperationId::new()}]).await;
    admit(&mut conn,vec![],vec![ArchiveRequest::Unhide{target:second_id,operation:OperationId::new()}]).await;
    let mut cursor=ChangeCursor::after(0);let mut tuples=vec![];let mut completed=vec![];
    loop{let page=changes_page(&mut conn,cursor,1,1).await.unwrap();assert!(page.gap.is_none());if page.entries.is_empty(){break}
        assert_eq!(page.entries.len(),1);let entry=&page.entries[0];if let Some(tuple)=tuple(entry){tuples.push(tuple)}else{completed.push(entry.position)}
        cursor=page.next.unwrap();}
    assert_eq!(tuples,[(1,first_id.0,"event"),(2,second_id.0,"event"),(2,first_id.0,"archived"),(3,second_id.0,"archived"),(4,second_id.0,"unhidden")]);
    assert_eq!(completed,[1,2,3,4]);
}
async fn tombstone_fixture(conn:&mut PgConnection,n:i64){
    let mut tx=conn.begin().await.unwrap();
    sqlx::query("UPDATE core.counters SET value=$1 WHERE name='commit_seq'").bind(n+2).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id,commit_seq,original_commit_seq,class) SELECT md5(i::text)::uuid,'fixture','command.executed',$2,$1+2,i,'open' FROM generate_series(1,$1) i")
        .bind(n).bind(Uuid::now_v7()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason) SELECT i,md5(i::text)::uuid,$1+1,true,'hide' FROM generate_series(1,$1) i")
        .bind(n).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
}
#[devkit::test]
async fn pruning_retains_tombstone_order_and_unknown_history_is_a_named_gap(db:devkit::pg::TestDatabase){
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();tombstone_fixture(&mut conn,1).await;
    sqlx::query("DELETE FROM core.commit_log").execute(&mut conn).await.unwrap();
    let page=changes_page(&mut conn,ChangeCursor::after(0),100,100000).await.unwrap();
    assert!(page.gap.is_none());let id=Uuid::parse_str("c4ca4238-a0b9-2382-0dcc-509a6f75849b").unwrap();
    assert_eq!(page.entries.iter().filter_map(tuple).collect::<Vec<_>>(),[(2,id,"archived"),(3,id,"excised")]);
    assert_eq!(page.entries.iter().filter(|entry|matches!(entry.change,Change::PositionComplete)).map(|entry|entry.position).collect::<Vec<_>>(),[1,2,3]);
    sqlx::query("UPDATE core.event_tombstones SET original_commit_seq=NULL").execute(&mut conn).await.unwrap();
    let page=changes_page(&mut conn,ChangeCursor::after(0),100,100000).await.unwrap();assert!(page.entries.is_empty());
    assert!(page.gap.unwrap().reason.starts_with("commit_history_position_unknown:"));
}
#[devkit::test]
async fn one_hundred_thousand_sibling_changes_do_not_form_a_page_or_checkpoint_cap(db:devkit::pg::TestDatabase){
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();const N:i64=100_000;tombstone_fixture(&mut conn,N).await;
    let mut cursor=ChangeCursor::after(N);let mut seen=0;let mut pages=0;
    loop{let page=changes_page(&mut conn,cursor,8192,8*1024*1024).await.unwrap();assert!(page.gap.is_none());assert!(!page.entries.is_empty());pages+=1;
        let mut complete=false;for entry in &page.entries{if complete{break}assert_eq!(entry.position,N+1);match &entry.change{
            Change::Archive(_)=>seen+=1,Change::PositionComplete=>{assert_eq!(seen,N);complete=true},_=>panic!("only archive siblings before their barrier")}}
        cursor=page.next.unwrap();if complete{break}}
    assert!(pages>1);assert_eq!(seen,N);
}

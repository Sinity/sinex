use super::*;
use sinex_derivations::work::WorkActs;
use sinex_store::privacy::{self,Action,NewRule};

fn keys()->Vec<Box<dyn AnyKeyed>>{vec![Box::new(WorkActs)]}
async fn live(conn:&mut PgConnection,kind:&str)->Vec<Value>{
    sqlx::query_scalar("SELECT e.payload FROM core.derivation_events e WHERE event_type=$1 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").bind(kind).fetch_all(conn).await.unwrap()
}
fn rule(name:&str,field:&str)->NewRule{NewRule{name:name.into(),source:"polylogue".into(),event_type:Some("polylogue.work_snapshot".into()),field:Some(field.into()),matcher:"all".into(),matcher_arg:None,class:Class::Open,action:Action::Mask,author:"fixture".into()}}

#[devkit::test]
async fn required_snapshot_mask_retires_acts_while_unrelated_mask_and_loosening_preserve_them(db:devkit::pg::TestDatabase){
    let w=World::with_keys(db,keys()).await;external::register(&w.url).await;
    let mut payload=external::snapshot("codex:required","main",&[at(0)],1);
    payload["objectives"]=json!([{"kind":"task","repository":"synthetic/repo","reference":"[masked]"}]);
    payload["notes"]=json!("unrelated prose");
    let source=external::record("polylogue.work_snapshot",payload,location(0));let source_id=source.id;
    w.admit(vec![source]).await;
    let mut runner=w.driver_with(keys(),vec![]).await;runner.catch_up().await.unwrap();
    let mut conn=PgConnection::connect(&w.url).await.unwrap();
    let original=live(&mut conn,"work.act").await;assert_eq!(original.len(),1);
    let estimate=|rows:Vec<Value>|rows.into_iter().map(|row|json!({"at":row["at"],"agency":row["agency"],"objective":row["objective"],"action":row["action"]})).collect::<Vec<_>>();
    let original=estimate(original);
    privacy::add_rule(&mut conn,&rule("unrelated","/notes")).await.unwrap();privacy::projection::ensure_current(&mut conn).await.unwrap();
    runner.catch_up().await.unwrap();assert_eq!(estimate(live(&mut conn,"work.act").await),original,"unrelated transformed source fields do not invalidate estimates");
    privacy::add_rule(&mut conn,&rule("reference","/objectives/0/reference")).await.unwrap();privacy::projection::ensure_current(&mut conn).await.unwrap();
    runner.catch_up().await.unwrap();assert!(live(&mut conn,"work.act").await.is_empty(),"same-value masked reference is unavailable");
    let coverage=live(&mut conn,"work.coverage").await;assert_eq!(coverage.len(),1);assert_eq!(coverage[0]["state"],"unavailable");assert!(coverage[0]["eligible_entries"].is_null());
    let parents:Vec<uuid::Uuid>=sqlx::query_scalar("SELECT parents FROM core.events e WHERE event_type='work.coverage' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").fetch_one(&mut conn).await.unwrap();assert_eq!(parents,[source_id.0]);
    privacy::end_rule(&mut conn,"reference","fixture").await.unwrap();privacy::projection::ensure_current(&mut conn).await.unwrap();
    runner.catch_up().await.unwrap();assert_eq!(estimate(live(&mut conn,"work.act").await),original);
    assert_eq!(live(&mut conn,"work.coverage").await[0]["state"],"complete");
    runner.end_runs().await.unwrap();
}

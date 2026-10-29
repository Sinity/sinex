//! Supplied vectors traverse ordinary admission, the derivation runner and
//! rebuildable pgvector projection without constructing a provider.
use super::*;
use sinex_derivations::embedding::Embeddings;
use sinex_model::event_types::embedding::{EmbeddingSpace,SuppliedEmbedding,TextEmbedding};

#[devkit::test]
async fn supplied_vectors_derive_rank_and_recompute_with_exact_source_parents(db:devkit::pg::TestDatabase){
    let w=World::with_keys(db,vec![]).await;
    let target=Event::build(&sinex_model::event_types::operator::LogEntry{written_at:None,label:None,text:"neutral document".into()},
        "fixture.documents",location(0),ReadingOrder(1),[1;32],Class::Guarded).unwrap();let target_id=target.id;
    w.admit(vec![target]).await;
    let payload=SuppliedEmbedding{target:target_id,input_record:format!("sinex:ev/{target_id}"),input_hash:blake3::hash(b"neutral document").to_hex().to_string(),
        space:EmbeddingSpace{provider:"supplied".into(),model:"neutral".into(),revision:"1".into(),settings_hash:blake3::hash(b"settings").to_hex().to_string(),dimensions:3},
        vector:vec![1.0,0.0,0.0],embedded_at:None};
    let source=|payload:&SuppliedEmbedding,order|Event::build(payload,"fixture.supplied",Origin::Derived{parents:vec![target_id]},ReadingOrder(order),
        *blake3::hash(&serde_json::to_vec(payload).unwrap()).as_bytes(),Class::Open).unwrap();
    let supplied=source(&payload,1);let supplied_id=supplied.id;
    w.admit(vec![supplied]).await;
    let mut runner=w.driver_with(vec![],vec![Box::new(Embeddings)]).await;runner.catch_up().await.unwrap();
    let kept=w.live::<TextEmbedding>().await;assert_eq!(kept.len(),1);assert_eq!(kept[0].target,target_id);assert_eq!(kept[0].supplied,supplied_id);
    let mut conn=PgConnection::connect(&w.url).await.unwrap();
    let (reference,parents,class):(uuid::Uuid,Vec<uuid::Uuid>,String)=sqlx::query_as("SELECT id,parents,class FROM core.events WHERE event_type='text.embedding'").fetch_one(&mut conn).await.unwrap();
    assert_eq!(parents,[supplied_id.0]);assert_eq!(class,"guarded");
    sinex_store::vectors::initialize(&mut conn).await.unwrap();
    sinex_store::query::set_grants(&mut conn,"guarded").await.unwrap();
    let context=sinex_model::query::Context::utc("2026-09-15T00:00:00Z".parse().unwrap());
    let question=format!("events where event_type = operator_log.entry AND similar:sinex:ev/{reference} fields id");
    let answer=sinex_store::query::run(&mut conn,&question,None,&context,&sinex_store::query::Page::default()).await.unwrap();
    assert_eq!(answer.total,1);assert_eq!(answer.rows[0]["id"],json!(target_id));
    runner.catch_up().await.unwrap();assert_eq!(w.live::<TextEmbedding>().await,kept);
    let mut changed=payload;changed.vector=vec![0.0,1.0,0.0];let updated=source(&changed,2);let updated_id=updated.id;
    w.admit(vec![updated]).await;assert!(w.live::<TextEmbedding>().await.is_empty());
    runner.catch_up().await.unwrap();let outputs=w.live::<TextEmbedding>().await;assert_eq!(outputs.len(),1);
    assert_eq!(outputs[0].vector,changed.vector);assert_eq!(outputs[0].supplied,updated_id);
    assert_eq!(w.count("SELECT count(*) FROM core.events e JOIN core.event_children c ON c.child=e.id JOIN audit.archived a ON a.event_id=c.parent WHERE NOT EXISTS(SELECT 1 FROM audit.archived b WHERE b.event_id=e.id)").await,0);
}

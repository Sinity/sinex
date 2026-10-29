use super::*;
use crate::{AdmissionContext,NoDerivations};
use sinex_model::{Batch,BatchKind,Class,Event,EventId,MaterialId,Origin,ReadingOrder,RecordLocation,RecordOutcome,RunId};
use sinex_model::event_types::{embedding::{EmbeddingSpace,SuppliedEmbedding},operator::LogEntry};
use serde_json::json;

async fn admit(conn:&mut AdmissionContext,events:Vec<Event>){
    let batch=Batch{run:RunId::new(),kind:BatchKind::Api{actor:"fixture".into()},records:vec![RecordOutcome::Events{location:None,events}],archive:vec![]};
    let mut tx=conn.begin().await.unwrap();tx.admit(&batch,&NoDerivations).await.unwrap();tx.commit().await.unwrap();
}
fn target(label:&str,class:Class)->Event{
    Event::build(&LogEntry{written_at:None,label:Some(label.into()),text:label.into()},"fixture.documents",Origin::Record{location:RecordLocation{material:MaterialId::new(),anchor:0},output_index:0},ReadingOrder(1),*blake3::hash(label.as_bytes()).as_bytes(),class).unwrap()
}
fn space(dimensions:usize)->EmbeddingSpace{EmbeddingSpace{provider:"supplied".into(),model:"neutral".into(),revision:"1".into(),settings_hash:blake3::hash(b"settings").to_hex().to_string(),dimensions:dimensions as u64}}
async fn embedding(conn:&mut AdmissionContext,target:EventId,vector:Vec<f64>)->(EventId,TextEmbedding){
    let supplied=SuppliedEmbedding{target,input_record:format!("record:{target}"),input_hash:blake3::hash(target.to_string().as_bytes()).to_hex().to_string(),space:space(vector.len()),vector:vector.clone(),embedded_at:None};
    let event=Event::build(&supplied,"fixture.supplied",Origin::Derived{parents:vec![target]},ReadingOrder(1),[1;32],Class::Open).unwrap();let supplied_id=event.id;
    admit(conn,vec![event]).await;
    let payload=TextEmbedding{target,supplied:supplied_id,input_record:supplied.input_record,input_hash:supplied.input_hash,space:supplied.space,vector,embedded_at:None};
    let event=Event::build(&payload,"text.embeddings",Origin::Derived{parents:vec![supplied_id]},ReadingOrder(1),[2;32],Class::Open).unwrap();let id=event.id;
    admit(conn,vec![event]).await;(id,payload)
}
async fn rank(conn:&mut PgConnection,target:EventId,reference:EventId,position:i64)->Option<f64>{
    let mut expression=crate::query::sql::SqlFragment::text("SELECT ");
    let target=format!("'{}'::uuid",target.0);
    expression.append(rank_sql(&target,reference.0,position));
    let (sql,params)=expression.render();
    crate::query::sql::bind_scalar(sqlx::query_scalar(&sql),params).fetch_one(conn).await.unwrap()
}

#[devkit::test]
async fn pgvector_indexed_candidates_match_cosine_and_filtered_unembedded_rows_survive(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();
    let documents=[target("query",Class::Open),target("near",Class::Open),target("far",Class::Open),target("no vector",Class::Open)];
    let ids:Vec<_>=documents.iter().map(|event|event.id).collect();admit(&mut conn,documents.to_vec()).await;
    let (reference,q)=embedding(&mut conn,ids[0],vec![1.0,0.0,0.0]).await;
    let (_,near)=embedding(&mut conn,ids[1],vec![0.8,0.6,0.0]).await;
    let (_,far)=embedding(&mut conn,ids[2],vec![-1.0,0.0,0.0]).await;
    initialize(&mut conn).await.unwrap();let head=crate::query::current_seq(&mut conn).await.unwrap();
    validate_reference(&mut conn,reference.0,head).await.unwrap();
    let neighbors=indexed_candidates(&mut conn,reference.0,head,3,Some(&ids[..3].iter().map(|id|id.0).collect::<Vec<_>>())).await.unwrap();
    assert_eq!(neighbors.iter().map(|row|row.1).collect::<Vec<_>>(),ids[..3].iter().map(|id|id.0).collect::<Vec<_>>());
    for (target,vector)in [(ids[0],q.vector),(ids[1],near.vector),(ids[2],far.vector)]{
        let score=rank(&mut conn,target,reference,head).await.unwrap();
        assert!((score-cosine(&vector,&[1.0,0.0,0.0]).unwrap()).abs()<1e-6);
    }
    assert_eq!(rank(&mut conn,ids[3],reference,head).await,None);
    let context=sinex_model::query::Context::utc("2026-09-15T00:00:00Z".parse().unwrap());
    let question=format!("events where event_type = operator_log.entry AND similar:sinex:ev/{} fields id",reference.0);
    let answer=crate::query::run(&mut conn,&question,None,&context,&crate::query::Page{size:Some(10),cursor:None}).await.unwrap();
    assert_eq!(answer.total,4,"similarity ranks and preserves unembedded filtered rows");
    assert_eq!(answer.rows.iter().map(|row|row["id"].as_str().unwrap().to_owned()).collect::<Vec<_>>(),ids.iter().map(|id|id.to_string()).collect::<Vec<_>>());

    // Force only plan selection, never a timing threshold.
    sqlx::query("SET enable_seqscan=off").execute(&mut conn).await.unwrap();
    let statement=format!("EXPLAIN(FORMAT JSON) SELECT embedding_id FROM core.embedding_vectors WHERE space='{}' AND coverage='indexed' ORDER BY projected::vector(3) <=> '[1,0,0]'::vector(3) LIMIT 3",space(3).id());
    let plan:serde_json::Value=sqlx::query_scalar(&statement).fetch_one(&mut conn).await.unwrap();
    assert!(plan.to_string().contains("embedding_space_"),"{plan}");
}

#[devkit::test]
async fn arbitrary_original_vectors_use_explicit_fallback_and_rebuild_after_pruning(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();
    let a=target("huge",Class::Open);let b=target("wide",Class::Open);let c=target("zero",Class::Open);
    let ids=[a.id,b.id,c.id];admit(&mut conn,vec![a,b,c]).await;
    let (large,original)=embedding(&mut conn,ids[0],vec![f64::MAX,1e-250,0.0]).await;
    let (wide,_)=embedding(&mut conn,ids[1],vec![1.0;16_001]).await;
    let (zero,_)=embedding(&mut conn,ids[2],vec![0.0;3]).await;
    initialize(&mut conn).await.unwrap();let head=crate::query::current_seq(&mut conn).await.unwrap();
    let rows:Vec<(Uuid,String,Option<String>)>=sqlx::query_as("SELECT embedding_id,coverage,reason FROM core.embedding_vectors ORDER BY embedding_id").fetch_all(&mut conn).await.unwrap();
    assert!(rows.iter().all(|row|row.1=="fallback"));
    assert!(rows.iter().any(|row|row.0==wide.0&&row.2.as_deref()==Some("vector_type_dimension_limit")));
    assert_eq!(rank(&mut conn,ids[0],large,head).await,Some(1.0));
    assert_eq!(rank(&mut conn,ids[1],wide,head).await,Some(1.0));
    assert!(validate_reference(&mut conn,zero.0,head).await.unwrap_err().to_string().contains("direction_unknown"));
    let stored:serde_json::Value=sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1").bind(large.0).fetch_one(&mut conn).await.unwrap();
    assert_eq!(stored["vector"],json!(original.vector));
    sqlx::query("DELETE FROM core.commit_log").execute(&mut conn).await.unwrap();
    rebuild(&mut conn).await.unwrap();
    assert_eq!(rank(&mut conn,ids[0],large,head).await,Some(1.0));
    assert_eq!(crate::commit_log::consumer_position(&mut conn,CONSUMER).await.unwrap(),(head,0));
}

#[devkit::test]
async fn vector_grants_and_projection_rollback_follow_the_source_parent(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let document=target("guarded",Class::Guarded);let target=document.id;
    admit(&mut conn,vec![document]).await;let (reference,_)=embedding(&mut conn,target,vec![1.0,0.0]).await;
    initialize(&mut conn).await.unwrap();let head=crate::query::current_seq(&mut conn).await.unwrap();
    assert!(validate_reference(&mut conn,reference.0,head).await.is_err());
    crate::query::set_grants(&mut conn,"guarded").await.unwrap();
    validate_reference(&mut conn,reference.0,head).await.unwrap();
    assert_eq!(rank(&mut conn,target,reference,head).await,Some(1.0));
    let before:i64=sqlx::query_scalar("SELECT count(*) FROM core.embedding_vectors").fetch_one(&mut conn).await.unwrap();
    let mut tx=conn.begin().await.unwrap();sqlx::query("DELETE FROM core.embedding_vectors").execute(&mut *tx).await.unwrap();tx.rollback().await.unwrap();
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM core.embedding_vectors").fetch_one(&mut conn).await.unwrap(),before);
    crate::query::set_grants(&mut conn,"").await.unwrap();
    sqlx::query("SET ROLE sinex_reader").execute(&mut conn).await.unwrap();
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM core.visible_embedding_vectors").fetch_one(&mut conn).await.unwrap(),0);
    assert!(sqlx::query("SELECT projected FROM core.embedding_vectors").execute(&mut conn).await.is_err());
}

#[devkit::test]
async fn policy_epoch_withholds_changed_target_vectors_and_restores_exact_originals(db:devkit::pg::TestDatabase){
    let mut conn=AdmissionContext::connect(&db.url(None)).await.unwrap();let doc=target("vectorsecret",Class::Open);let id=doc.id;admit(&mut conn,vec![doc]).await;
    let (reference,original)=embedding(&mut conn,id,vec![0.8,0.6]).await;initialize(&mut conn).await.unwrap();
    let head=crate::query::current_seq(&mut conn).await.unwrap();validate_reference(&mut conn,reference.0,head).await.unwrap();
    crate::privacy::add_rule(&mut conn,&crate::privacy::NewRule{name:"vector field".into(),source:"fixture.documents".into(),event_type:Some("operator_log.entry".into()),field:Some("/text".into()),matcher:"pattern".into(),matcher_arg:Some("vectorsecret".into()),class:Class::Open,action:crate::privacy::Action::Mask,author:"fixture".into()}).await.unwrap();
    assert!(validate_reference(&mut conn,reference.0,head).await.is_err());
    crate::privacy::projection::ensure_current(&mut conn).await.unwrap();
    assert!(validate_reference(&mut conn,reference.0,head).await.is_err());
    let visible:i64=sqlx::query_scalar("SELECT count(*) FROM core.visible_embedding_vectors WHERE embedding_id=$1").bind(reference.0).fetch_one(&mut *conn).await.unwrap();assert_eq!(visible,0);
    let vector:serde_json::Value=sqlx::query_scalar("SELECT payload->'vector' FROM core.events WHERE id=$1").bind(reference.0).fetch_one(&mut *conn).await.unwrap();assert_eq!(vector,json!(original.vector));
    crate::privacy::end_rule(&mut conn,"vector field","fixture").await.unwrap();crate::privacy::projection::ensure_current(&mut conn).await.unwrap();
    validate_reference(&mut conn,reference.0,head).await.unwrap();assert_eq!(rank(&mut conn,id,reference,head).await,Some(1.0));
}

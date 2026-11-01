use super::*;
use crate::Blobs;
use sqlx::Connection;

async fn stored(conn: &mut PgConnection, blobs: &Blobs, bytes: &[u8]) -> String {
    let name=blobs.put(bytes).unwrap();
    let mut tx=conn.begin().await.unwrap();
    let receipt=blobs.commit_extents(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    blobs.acknowledge(receipt).unwrap();
    name
}

#[devkit::test]
async fn two_durable_holds_protect_a_prefix_across_restart_until_last_release(db: devkit::pg::TestDatabase) {
    let mut conn=db.connect().await.unwrap();
    let dir=tempfile::tempdir().unwrap();
    let blobs=Blobs::open(dir.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let bytes=b"independent original bytes, including the deleting extent";
    let name=stored(&mut conn,&blobs,bytes).await;
    let original=std::fs::read(dir.path().join("segments/current.hb")).unwrap();
    let one=OperationId::new(); let two=OperationId::new();
    let (first,created)=hold(&mut conn,one,"test").await.unwrap(); assert!(created);
    hold(&mut conn,two,"test").await.unwrap();
    assert_eq!(hold(&mut conn,one,"test").await.unwrap(),(first.clone(),false));
    assert!(hold(&mut conn,one,"forged").await.is_err());
    sqlx::query!("UPDATE core.blob_extents SET deleting=true WHERE hash=$1",name).execute(&mut conn).await.unwrap();
    assert!(!blobs.delete(&mut conn,&name).await.unwrap());
    drop(blobs);
    let reopened=Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(std::fs::read(dir.path().join("segments/current.hb")).unwrap(),original);
    release(&mut conn,one,OperationId::new(),"test").await.unwrap();
    assert!(!reopened.delete(&mut conn,&name).await.unwrap());
    release(&mut conn,two,OperationId::new(),"test").await.unwrap();
    assert!(reopened.delete(&mut conn,&name).await.unwrap());
    assert_ne!(std::fs::read(dir.path().join("segments/current.hb")).unwrap(),original);
    assert!(list(&mut conn,None,10).await.unwrap().is_empty());
    assert!(get(&mut conn,one).await.unwrap().unwrap().released_at.is_some());
}

#[devkit::test]
async fn a_prefix_hold_defers_orphan_cleanup_but_not_newer_appends(db: devkit::pg::TestDatabase) {
    let mut conn=db.connect().await.unwrap();
    let dir=tempfile::tempdir().unwrap();let blobs=Blobs::open(dir.path()).unwrap();blobs.load(&mut conn).await.unwrap();
    let old=stored(&mut conn,&blobs,b"old protected orphan").await;
    let id=OperationId::new();hold(&mut conn,id,"test").await.unwrap();
    let before=std::fs::read(dir.path().join("segments/current.hb")).unwrap();
    let new=stored(&mut conn,&blobs,b"new unprotected orphan").await;
    let removed=crate::blobs::delete_unreferenced_extents(&mut conn,&blobs).await.unwrap();
    assert_eq!(removed,vec![new]);
    assert_eq!(&std::fs::read(dir.path().join("segments/current.hb")).unwrap()[..before.len()],before);
    release(&mut conn,id,OperationId::new(),"test").await.unwrap();
    assert_eq!(crate::blobs::finish_deleting_extents(&mut conn,&blobs).await.unwrap(),vec![old]);
}

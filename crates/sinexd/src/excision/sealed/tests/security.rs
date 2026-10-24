use super::*;

async fn apply(ctx:&OneShot,target:Target,prepared:Prepared,authority:Authority,cancelled:bool)->Result<crate::writer::Excised,crate::writer::WriteError> {
    let guard=ctx.writer.excision_guard(zsh::SPEC.name.into()).await.unwrap();
    ctx.writer.excise(crate::writer::Excision { sealed:Some(prepared.sealed),authority:Some(authority),guard:Some(guard),
        cancelled:Arc::new(std::sync::atomic::AtomicBool::new(cancelled)),target,plan:prepared.plan,rewrites:prepared.rewrites,
        operation:OperationId::new(),actor:"synthetic".into(),blobs:ctx.blobs.clone() }).await
}

#[tokio::test]
async fn prepared_sealed_scope_rejects_reused_or_different_naming_secrets_cancel_and_new_epoch_before_any_mutation() {
    let (_db,url,_)=crate::testdb::fresh().await;let root=tempfile::tempdir().unwrap();
    let mut ctx=OneShot::open(&url,&root.path().join("store")).await.unwrap();
    let identity=crypto::Identity::generate();let recipient=identity.to_public();
    let authority=Authority::new(identity.clone(),Duration::from_secs(300)).unwrap();
    let raw=b": 1700000000:0;synthetic_removed\n: 1700000001:0;synthetic_retained\n";
    let material=capture(&mut ctx,root.path(),"epoch-a",raw,&recipient).await;
    let before=event_rows(&mut ctx).await;
    let target=Target::Event(EventId(before.iter().find(|e|e.2==0).unwrap().0));
    let old=authority.material_key(&mut ctx.read_conn,material).await.unwrap();
    for reused in [true,false] {
        let mut prepared=prepare(&mut ctx.read_conn,&ctx.blobs,target,&authority).await.unwrap();
        let forged=if reused { crypto::MaterialSecrets::from_data_key(&old) }
            else { crypto::MaterialSecrets::from_data_key(&crypto::new_key().unwrap()) };
        prepared.sealed.rotations[0].replacement.wrapped=crypto::wrap_secrets(&forged,&recipient).unwrap();
        let refused=apply(&ctx,target,prepared,authority.clone(),false).await.unwrap_err().to_string();
        assert!(refused.contains(if reused {"retiring data secret"}else{"record-naming capability"}),"{refused}");
        assert_eq!(event_rows(&mut ctx).await,before);
    }
    let prepared=prepare(&mut ctx.read_conn,&ctx.blobs,target,&authority).await.unwrap();
    assert!(apply(&ctx,target,prepared,authority.clone(),true).await.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(event_rows(&mut ctx).await,before);
    let prepared=prepare(&mut ctx.read_conn,&ctx.blobs,target,&authority).await.unwrap();
    authority.revoke();
    assert!(apply(&ctx,target,prepared,authority,false).await.unwrap_err().to_string().contains("revoked"));
    assert_eq!(event_rows(&mut ctx).await,before);
    let authority=Authority::new(identity,Duration::from_secs(300)).unwrap();
    let prepared=prepare(&mut ctx.read_conn,&ctx.blobs,target,&authority).await.unwrap();
    let added=capture(&mut ctx,root.path(),"epoch-b",raw,&recipient).await;
    let before_new_epoch=event_rows(&mut ctx).await;
    assert!(apply(&ctx,target,prepared,authority.clone(),false).await.unwrap_err().to_string().contains("changed after planning"));
    assert_eq!(event_rows(&mut ctx).await,before_new_epoch);
    assert!(crypto::wrapped_key(&mut ctx.read_conn,&old.key_ref).await.unwrap().is_some());
    remove(&ctx,&url,target,authority.clone()).await.unwrap();
    assert_eq!(event_rows(&mut ctx).await.len(),2);
    assert!(crypto::wrapped_key(&mut ctx.read_conn,&old.key_ref).await.unwrap().is_none());
    assert_eq!(crate::retained::open(&mut ctx.read_conn,&ctx.blobs,zsh::SPEC.name,added,Some(&authority)).await.unwrap().metadata.len(),1);
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn stale_pending_progress_cannot_restore_retired_envelopes_or_advance_the_purged_cursor() {
    let (_db,url,_)=crate::testdb::fresh().await;let root=tempfile::tempdir().unwrap();
    let mut ctx=OneShot::open(&url,&root.path().join("store")).await.unwrap();
    let identity=crypto::Identity::generate();let recipient=identity.to_public();
    let authority=Authority::new(identity,Duration::from_secs(300)).unwrap();
    let material=capture(&mut ctx,root.path(),"history",b": 1700000000:0;synthetic_removed\n: 1700000001:0;synthetic_retained\n",&recipient).await;
    let (binding,kind,locator,host,cursor,carry):(String,String,String,String,Value,Value)=sqlx::query_as("SELECT i.binding,i.kind,i.locator,i.host,p.cursor,p.carry FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id JOIN raw.positions p ON p.binding=i.binding WHERE m.id=$1")
        .bind(material.0).fetch_one(&mut ctx.read_conn).await.unwrap();
    let (key_ref,wrapped):(String,Vec<u8>)=sqlx::query_as("SELECT key_ref,wrapped_key FROM privacy.material_keys WHERE material_id=$1").bind(material.0).fetch_one(&mut ctx.read_conn).await.unwrap();
    let basis=excision::operations_basis(&mut ctx.read_conn,zsh::SPEC.name).await.unwrap();
    let target=Target::Event(EventId(event_rows(&mut ctx).await.iter().find(|e|e.2==0).unwrap().0));
    remove(&ctx,&url,target,authority.clone()).await.unwrap();
    let after=sinex_store::positions::load(&mut ctx.read_conn,&binding).await.unwrap().unwrap();
    let position:Value=sqlx::query_scalar("SELECT to_jsonb(p) FROM raw.positions p WHERE binding=$1").bind(&binding).fetch_one(&mut ctx.read_conn).await.unwrap();
    let mut progress=sinex_model::SourceProgress { native_pending:None,erased_replay:None,excision_basis:Some(basis),binding:binding.clone(),image:None,image_revision:None,
        material_keys:vec![sinex_model::WrappedMaterialKey {material,key_ref:key_ref.clone(),wrapped}],
        input:sinex_model::InputRef {kind,locator,host},observation:Default::default(),revision:sinex_model::RevisionChange::Same,material:None,cursor,carry };
    for current_basis in [false,true] {
        if current_basis { progress.excision_basis=Some(excision::operations_basis(&mut ctx.read_conn,zsh::SPEC.name).await.unwrap()); }
        let batch=sinex_model::Batch {run:sinex_model::RunId::new(),kind:sinex_model::BatchKind::Source(Box::new(progress.clone())),records:Vec::new(),archive:Vec::new()};
        assert!(ctx.writer.submit(crate::writer::Priority::Bulk,batch).await.is_err());
        assert_eq!(sinex_store::positions::load(&mut ctx.read_conn,&binding).await.unwrap().unwrap(),after);
        let actual:Value=sqlx::query_scalar("SELECT to_jsonb(p) FROM raw.positions p WHERE binding=$1").bind(&binding).fetch_one(&mut ctx.read_conn).await.unwrap();
        assert_eq!(actual,position);
        assert!(crypto::wrapped_key(&mut ctx.read_conn,&key_ref).await.unwrap().is_none());
    }
    ctx.close().await.unwrap();
}

#[tokio::test]
async fn a_missing_legacy_predecessor_cannot_reinterpret_a_later_duplicate_as_the_first_occurrence() {
    let (_db,url,_)=crate::testdb::fresh().await;let root=tempfile::tempdir().unwrap();
    let mut ctx=OneShot::open(&url,&root.path().join("store")).await.unwrap();
    let identity=crypto::Identity::generate();let recipient=identity.to_public();
    let authority=Authority::new(identity,Duration::from_secs(300)).unwrap();
    let first=capture(&mut ctx,root.path(),"history",b": 1700000000:0;synthetic_repeat\n",&recipient).await;
    let later=capture_scoped(&mut ctx,root.path(),"history",b": 1700000000:0;synthetic_repeat\n: 1700000000:0;synthetic_repeat\n",&recipient,Some(&authority)).await.unwrap();
    assert_ne!(first,later);
    let before=event_rows(&mut ctx).await;
    let event=before.iter().find(|row|row.1==later.0).unwrap();
    let key=authority.material_key(&mut ctx.read_conn,later).await.unwrap();
    assert_eq!(crypto::unseal_payload(&event.3["payload"],&key.key).unwrap()["n"],1);
    let target=Target::Event(EventId(event.0));
    let previous:Option<uuid::Uuid>=sqlx::query_scalar("SELECT previous_part_id FROM raw.materials WHERE id=$1").bind(later.0).fetch_one(&mut ctx.read_conn).await.unwrap();
    assert_eq!(previous,Some(first.0));
    sqlx::query("UPDATE raw.materials SET previous_part_id=NULL WHERE id=$1").bind(later.0).execute(&mut ctx.read_conn).await.unwrap();
    let refused=remove(&ctx,&url,target,authority.clone()).await.unwrap_err();
    assert!(refused.contains("retained predecessor evidence"),"{refused}");
    assert_eq!(event_rows(&mut ctx).await,before);
    assert!(crypto::wrapped_key(&mut ctx.read_conn,&key.key_ref).await.unwrap().is_some());
    sqlx::query("UPDATE raw.materials SET previous_part_id=$2 WHERE id=$1").bind(later.0).bind(previous).execute(&mut ctx.read_conn).await.unwrap();
    remove(&ctx,&url,target,authority).await.unwrap();
    let after=event_rows(&mut ctx).await;
    assert_eq!(after.len(),1);assert_eq!(after[0],before.iter().find(|row|row.1==first.0).unwrap().clone());
    ctx.close().await.unwrap();
}

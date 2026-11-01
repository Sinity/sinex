use super::*;
use sqlx::Connection;
fn claims(epoch:i64)->Claims{Claims{purpose:"write".into(),producer:"producer".into(),family:"family".into(),cache:"cache".into(),scope:"scope".into(),position:0,policy_epoch:epoch,instance:"instance".into(),catalog:"catalog".into(),host:"host".into(),settings_identity:"settings".into(),owner_revision:7,kinds:vec![],indexed_kinds:vec![],classes:vec!["open".into()],replay_root:Some("completed-page-chain".into())}}
#[devkit::test]
fn owner_proofs_reject_corruption_and_another_protocol_domain(){
    let key=[3;32];let claims=claims(4);let stamp=crate::owner_protocol::sign(&key,DOMAIN,&claims).unwrap();
    assert_eq!(crate::owner_protocol::open::<Claims>(&key,DOMAIN,&stamp).unwrap(),claims);
    assert!(crate::owner_protocol::open::<Claims>(&key,"another.protocol.v1",&stamp).is_err());
    let (body,tag)=stamp.split_once('.').unwrap();use base64::Engine;let mut value:serde_json::Value=serde_json::from_slice(&base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(body).unwrap()).unwrap();value["position"]=serde_json::json!(99);
    let forged=format!("{}.{}",base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).unwrap()),tag);
    assert!(crate::owner_protocol::open::<Claims>(&key,DOMAIN,&forged).is_err());
}
#[devkit::test]
async fn unchanged_settings_accept_newer_real_revisions_but_reject_stale_epoch_and_identity(db:devkit::pg::TestDatabase){
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();let epoch=crate::privacy::projection::epoch(&mut conn).await.unwrap();
    // Only the public projection fields consumed by this validator are seeded;
    // the host-policy projection itself has a separate typed checkpoint suite.
    sqlx::query("INSERT INTO core.signal_checkpoints(host,revision,state) VALUES('host',7,'{\"settings_identity\":\"settings\"}')").execute(&mut conn).await.unwrap();
    let claims=claims(epoch.active);current(&mut conn,&claims).await.unwrap();
    sqlx::query("UPDATE core.signal_checkpoints SET revision=8 WHERE host='host'").execute(&mut conn).await.unwrap();current(&mut conn,&claims).await.unwrap();
    sqlx::query("UPDATE core.signal_checkpoints SET state='{\"settings_identity\":\"changed\"}' WHERE host='host'").execute(&mut conn).await.unwrap();assert!(current(&mut conn,&claims).await.is_err());
    sqlx::query("UPDATE core.signal_checkpoints SET state='{\"settings_identity\":\"settings\"}' WHERE host='host'").execute(&mut conn).await.unwrap();
    sqlx::query("UPDATE privacy.policy_state SET desired_epoch=desired_epoch+1 WHERE id").execute(&mut conn).await.unwrap();assert!(current(&mut conn,&claims).await.is_err());
}

fn covered_frame(claims:&Claims,stamp:String,required:bool)->sinex_model::IngressFrame {
    sinex_model::IngressFrame {
        content_hash:None,receipt_number:None,
        coverage:Some(sinex_model::OwnerCoverage{required,stamp,position:claims.position,scope:claims.scope.clone(),catalog:claims.catalog.clone(),instance:claims.instance.clone(),host:claims.host.clone(),settings_identity:claims.settings_identity.clone()}),
        native_source:None,source_fingerprint:None,host:None,family:Some(claims.family.clone()),write_kind_prefixes:vec![],assign_orders:vec![],producer:claims.producer.clone(),frame:"frame".into(),material:None,refusal:None,declarations:vec![],refusal_kind:None,source:None,event_types:vec![],
    }
}

#[devkit::test]
async fn optional_emergency_stamp_loses_certification_without_relaxing_required_proofs(db:devkit::pg::TestDatabase){
    let mut conn=PgConnection::connect(&db.url(None)).await.unwrap();let epoch=crate::privacy::projection::epoch(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO core.signal_checkpoints(host,revision,state) VALUES('host',7,'{\"settings_identity\":\"settings\"}')").execute(&mut conn).await.unwrap();
    let claims=claims(epoch.active);let stamp=sign(&mut conn,&claims).await.unwrap();
    let mandatory=covered_frame(&claims,stamp.clone(),true);let optional=covered_frame(&claims,stamp,false);
    assert_eq!(validate(&mut conn,&mandatory).await.unwrap(),Some(epoch.active));
    assert_eq!(validate(&mut conn,&optional).await.unwrap(),Some(epoch.active));
    sqlx::query("UPDATE core.signal_checkpoints SET state='{\"settings_identity\":\"changed\"}' WHERE host='host'").execute(&mut conn).await.unwrap();
    assert!(validate(&mut conn,&mandatory).await.is_err());assert_eq!(validate(&mut conn,&optional).await.unwrap(),None);
    let malformed=covered_frame(&claims,"bad-stamp".into(),false);assert_eq!(validate(&mut conn,&malformed).await.unwrap(),None);
    let malformed=covered_frame(&claims,"bad-stamp".into(),true);assert!(validate(&mut conn,&malformed).await.is_err());
    let mut unavailable=conn.begin().await.unwrap();
    sqlx::query("DELETE FROM core.owner_protocol_key").execute(&mut *unavailable).await.unwrap();
    assert!(matches!(validate(&mut unavailable,&optional).await,Err(Error::Database(sqlx::Error::RowNotFound))),"optional certification cannot turn missing owner signing authority into a semantic refusal");
    unavailable.rollback().await.unwrap();
}

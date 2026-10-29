use super::*;
use sqlx::Connection;
fn claims(epoch: i64) -> Claims {
    Claims {
        purpose: "write".into(),
        producer: "producer".into(),
        family: "family".into(),
        cache: "cache".into(),
        scope: "scope".into(),
        position: 0,
        policy_epoch: epoch,
        instance: "instance".into(),
        catalog: "catalog".into(),
        host: "host".into(),
        settings_identity: "settings".into(),
        owner_revision: 7,
        kinds: vec![],
        replay_root: Some("completed-page-chain".into()),
    }
}
#[devkit::test]
fn owner_proofs_reject_corruption_and_another_protocol_domain() {
    let key = [3; 32];
    let claims = claims(4);
    let stamp = crate::owner_protocol::sign(&key, DOMAIN, &claims).unwrap();
    assert_eq!(
        crate::owner_protocol::open::<Claims>(&key, DOMAIN, &stamp).unwrap(),
        claims
    );
    assert!(crate::owner_protocol::open::<Claims>(&key, "another.protocol.v1", &stamp).is_err());
    let (body, tag) = stamp.split_once('.').unwrap();
    use base64::Engine;
    let mut value: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(body)
            .unwrap(),
    )
    .unwrap();
    value["position"] = serde_json::json!(99);
    let forged = format!(
        "{}.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&value).unwrap()),
        tag
    );
    assert!(crate::owner_protocol::open::<Claims>(&key, DOMAIN, &forged).is_err());
}
#[devkit::test]
async fn unchanged_settings_accept_newer_real_revisions_but_reject_stale_epoch_and_identity(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let epoch = crate::privacy::projection::epoch(&mut conn).await.unwrap();
    // Only the public projection fields consumed by this validator are seeded;
    // the host-policy projection itself has a separate typed checkpoint suite.
    sqlx::query("INSERT INTO core.signal_checkpoints(host,revision,state) VALUES('host',7,'{\"settings_identity\":\"settings\"}')").execute(&mut conn).await.unwrap();
    let claims = claims(epoch.active);
    current(&mut conn, &claims).await.unwrap();
    sqlx::query("UPDATE core.signal_checkpoints SET revision=8 WHERE host='host'")
        .execute(&mut conn)
        .await
        .unwrap();
    current(&mut conn, &claims).await.unwrap();
    sqlx::query("UPDATE core.signal_checkpoints SET state='{\"settings_identity\":\"changed\"}' WHERE host='host'").execute(&mut conn).await.unwrap();
    assert!(current(&mut conn, &claims).await.is_err());
    sqlx::query("UPDATE core.signal_checkpoints SET state='{\"settings_identity\":\"settings\"}' WHERE host='host'").execute(&mut conn).await.unwrap();
    sqlx::query("UPDATE privacy.policy_state SET desired_epoch=desired_epoch+1 WHERE id")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(current(&mut conn, &claims).await.is_err());
}

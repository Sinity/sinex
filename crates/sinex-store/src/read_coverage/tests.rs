use super::*;
use sqlx::Connection;
fn claims() -> Claims {
    Claims {
        purpose: "write".into(),
        producer: "producer".into(),
        family: "family".into(),
        cache: "cache".into(),
        scope: "scope".into(),
        position: 0,
        instance: "instance".into(),
        catalog: "catalog".into(),
        host: "host".into(),
        settings_identity: "settings".into(),
        owner_revision: 7,
        kinds: vec![],
        indexed_kinds: vec![],
        classes: vec!["open".into()],
        replay_root: Some("completed-page-chain".into()),
    }
}
#[devkit::test]
fn owner_proofs_reject_corruption_and_another_protocol_domain() {
    let key = [3; 32];
    let claims = claims();
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
async fn unchanged_settings_accept_newer_real_revisions_but_reject_changed_identity(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Only the public projection fields consumed by this validator are seeded;
    // the host-policy projection itself has a separate typed checkpoint suite.
    sqlx::query("INSERT INTO core.signal_checkpoints(host,revision,state) VALUES('host',7,'{\"settings_identity\":\"settings\"}')").execute(&mut conn).await.unwrap();
    let claims = claims();
    current(&mut conn, &claims).await.unwrap();
    sqlx::query("UPDATE core.signal_checkpoints SET revision=8 WHERE host='host'")
        .execute(&mut conn)
        .await
        .unwrap();
    current(&mut conn, &claims).await.unwrap();
    sqlx::query("UPDATE core.signal_checkpoints SET state='{\"settings_identity\":\"changed\"}' WHERE host='host'").execute(&mut conn).await.unwrap();
    assert!(current(&mut conn, &claims).await.is_err());
}

fn covered_frame(claims: &Claims, stamp: String) -> sinex_model::IngressFrame {
    sinex_model::IngressFrame {
        content_hash: None,
        receipt_number: None,
        coverage: Some(sinex_model::OwnerCoverage {
            stamp,
            position: claims.position,
            scope: claims.scope.clone(),
            catalog: claims.catalog.clone(),
            instance: claims.instance.clone(),
            host: claims.host.clone(),
            settings_identity: claims.settings_identity.clone(),
        }),
        native_source: None,
        source_fingerprint: None,
        host: None,
        family: Some(claims.family.clone()),
        write_kind_prefixes: vec![],
        assign_orders: vec![],
        producer: claims.producer.clone(),
        frame: "frame".into(),
        material: None,
        refusal: None,
        declarations: vec![],
        source: None,
        event_types: vec![],
    }
}

/// A coverage stamp certifies a frame only with its exact outer claims and
/// the owner's current settings; anything else is refused, and a missing or
/// malformed signing key is an error, never a silent loss of certification.
#[devkit::test]
async fn coverage_stamp_requires_its_exact_claims_and_current_settings(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query("INSERT INTO core.signal_checkpoints(host,revision,state) VALUES('host',7,'{\"settings_identity\":\"settings\"}')").execute(&mut conn).await.unwrap();
    let claims = claims();
    let stamp = sign(&mut conn, &claims).await.unwrap();
    let covered = covered_frame(&claims, stamp);
    assert!(validate(&mut conn, &covered).await.unwrap());
    let mut forged = covered.clone();
    forged.coverage.as_mut().unwrap().position += 1;
    assert!(
        validate(&mut conn, &forged).await.is_err(),
        "a valid stamp does not authenticate a changed outer position"
    );
    let malformed = covered_frame(&claims, "bad-stamp".into());
    assert!(validate(&mut conn, &malformed).await.is_err());
    let mut unavailable = conn.begin().await.unwrap();
    sqlx::query("DELETE FROM core.owner_protocol_key")
        .execute(&mut *unavailable)
        .await
        .unwrap();
    assert!(matches!(
        validate(&mut unavailable, &covered).await,
        Err(Error::Database(sqlx::Error::RowNotFound))
    ));
    unavailable.rollback().await.unwrap();
    let mut corrupt = conn.begin().await.unwrap();
    sqlx::query(
        "ALTER TABLE core.owner_protocol_key DROP CONSTRAINT owner_protocol_key_secret_check",
    )
    .execute(&mut *corrupt)
    .await
    .unwrap();
    sqlx::query("UPDATE core.owner_protocol_key SET secret=decode('00','hex')")
        .execute(&mut *corrupt)
        .await
        .unwrap();
    assert!(validate(&mut corrupt, &covered).await.is_err());
    corrupt.rollback().await.unwrap();
    sqlx::query("UPDATE core.signal_checkpoints SET state='{\"settings_identity\":\"changed\"}' WHERE host='host'").execute(&mut conn).await.unwrap();
    assert!(validate(&mut conn, &covered).await.is_err());
}

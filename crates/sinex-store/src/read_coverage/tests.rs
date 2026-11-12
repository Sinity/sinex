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
async fn projection_catalog_checks_do_not_reread_retained_event_history(
    db: devkit::pg::TestDatabase,
) {
    use crate::{AdmissionContext, NoDerivations};
    use sinex_model::{
        Batch, BatchKind, Class, Event, KindDeclaration, MaterialId, Origin, ReadingOrder,
        RecordLocation, RecordOutcome, RunId,
    };
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let declaration = KindDeclaration::schemaless(
        KindRef {
            name: "synthetic.projection".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    sinex_schema::kinds::register(&mut conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    let events = (0..8192)
        .map(|n| {
            Event::submitted(
                declaration.kind.clone(),
                serde_json::json!({"n":n}),
                "synthetic".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                *blake3::hash(n.to_string().as_bytes()).as_bytes(),
                if n == 0 { Class::Guarded } else { Class::Open },
            )
        })
        .collect();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(
        tx.admit(&batch, &NoDerivations).await.unwrap().admitted,
        8192
    );
    tx.commit().await.unwrap();
    let mut covered = claims();
    covered.position = crate::query::current_seq(&mut conn).await.unwrap();
    covered.indexed_kinds = vec![(declaration.kind.clone(), declaration.definition_hash())];
    covered.classes = vec!["open".into(), "guarded".into()];

    let mut probe = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = probe.begin().await.unwrap();
    sqlx::raw_sql(
        "SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL enable_indexonlyscan=off",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    let work = "SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN (SELECT show_chunks('core.events'))";
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    // Page continuations and status requests may check the same certificate
    // repeatedly. Its immutable catalog check must not revisit any event row.
    for _ in 0..8 {
        projection_catalog(&mut tx, &covered).await.unwrap();
    }
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert_eq!(
        after - before,
        0,
        "catalog checks revisited retained event tuples"
    );
    let mut changed = covered.clone();
    changed.indexed_kinds[0].1 = "0".repeat(64);
    assert!(
        matches!(projection_catalog(&mut tx, &changed).await, Err(AuthorityError::Invalidated(reason)) if reason == "projection_kind_catalog_unknown_or_changed")
    );
    changed.indexed_kinds[0].0.version = 2;
    assert!(projection_catalog(&mut tx, &changed).await.is_err());

    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let original: Vec<(String,i64,String)> = sqlx::query_as("SELECT DISTINCT e.event_type,e.kind_version,e.class FROM core.events e WHERE e.event_type=ANY($1) AND e.commit_seq<=$2")
        .bind(vec![declaration.kind.name.clone()]).bind(covered.position).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(original.len(), 2);
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before >= 8192,
        "original history check visited only {} tuples",
        after - before
    );
    let mut guarded = covered.clone();
    guarded.classes = vec!["guarded".into()];
    let selected = inputs(&mut tx, &guarded, None, 1).await.unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].class, "guarded");
    assert_eq!(selected[0].payload, serde_json::json!({"n":0}));
    tx.rollback().await.unwrap();
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

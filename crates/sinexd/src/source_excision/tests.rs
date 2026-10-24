use super::*;
use sinex_model::{
    Class, Event, Origin, ReadingOrder, RecordLocation, Timestamp, event_types::CommandExecuted,
};
use sinex_store::sealed;
use std::time::Duration;
fn event(material: MaterialId, native: &str, hash: [u8; 32]) -> Event {
    Event::build(
        &CommandExecuted {
            native_id: native.into(),
            command: "synthetic unrelated value".into(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1).unwrap(),
            duration_ns: None,
            exit_code: None,
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "s",
        Origin::Record {
            location: RecordLocation {
                material,
                anchor: 0,
            },
            ordinal: 0,
        },
        ReadingOrder(1),
        hash,
        Class::Sealed,
    )
    .unwrap()
}
#[tokio::test]
async fn old_naming_only_exclusions_match_new_epoch_keys_hashes_and_unparsed_bytes_after_restart() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let mut conn = sqlx::Connection::connect(&url).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let identity = sealed::Identity::generate();
    let old = sealed::new_key().unwrap();
    let material = MaterialId::new();
    let raw = b"synthetic erased private record\n";
    let original = event(material, "selected-native-id", [9; 32]);
    let names = MaterialSecrets::from_data_key(&old).naming_only();
    let wrapped = sealed::wrap_secrets(&names, &identity.to_public()).unwrap();
    let body = blobs.put_sealed(raw, &old).unwrap();
    let scope = metadata::Scope::Replay {
        source: "s".into(),
        material,
        parser: "p".into(),
        parser_version: 1,
        length: raw.len() as u64,
        whole: true,
    };
    let mut ciphertext = Vec::new();
    metadata::seal_replay(
        &mut ciphertext,
        &identity.to_public(),
        &scope,
        [metadata::ErasedRecord {
            start: 0,
            end: raw.len() as u64,
            raw_digest: names.name_raw_record(raw),
            transition: None,
        }],
        || Ok(()),
    )
    .unwrap();
    let replay = blobs.put(&ciphertext).unwrap();
    for (blob, length) in [(&body, raw.len()), (&replay, ciphertext.len())] {
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
            .bind(blob)
            .bind(length as i64)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO raw.materials(id,blob,durable_bytes,manifest,parser,parser_version,erased_replay_blob) VALUES($1,$2,$3,'{}','p',1,$4)").bind(material.0).bind(&body).bind(raw.len() as i64).bind(&replay).execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO privacy.material_keys(material_id,key_ref,class,wrapped_key,role) VALUES($1,'names-only','sealed',$2,'record-naming-only')").bind(material.0).bind(&wrapped).execute(&mut conn).await.unwrap();
    let operation = Uuid::now_v7();
    let scope = serde_json::json!({"sources":["s"],"record_domains_version":1,"record_domains":[{"material":material,"source":"s","naming":"sealed"}]});
    sqlx::query(
        "INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'excise','synthetic',$2)",
    )
    .bind(operation)
    .bind(scope)
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::query("INSERT INTO core.hidden_occurrences(source,event_type,equivalence_key,kind,operation_id) VALUES('s',$1,$2,'excise',$3)").bind(original.event_type).bind(names.name_equivalence(&original.equivalence_key.as_ref().unwrap().0).0).bind(operation).execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO core.excised_digests(source,event_type,record_hash,operation_id) VALUES('s',$1,$2,$3)").bind(original.event_type).bind(names.name_record_hash(&original.record_hash).as_slice()).bind(operation).execute(&mut conn).await.unwrap();
    assert!(sealed::unwrap(&wrapped, &identity).is_err());
    drop(old);
    let authority = Authority::new(identity, Duration::from_secs(60)).unwrap();
    assert!(
        AcquisitionPolicy::load(&mut conn, &blobs, "s", None)
            .await
            .is_err()
    );
    let policy = AcquisitionPolicy::load(&mut conn, &blobs, "s", Some(&authority))
        .await
        .unwrap();
    let fresh = MaterialId::new();
    assert!(
        policy.excludes(fresh, 0, raw, &[]).unwrap(),
        "failed/no-event exact bytes remain excluded"
    );
    assert!(
        policy
            .excludes(
                fresh,
                0,
                b"changed bytes",
                &[event(fresh, "selected-native-id", [99; 32])]
            )
            .unwrap(),
        "native keys cross fresh data-key epochs"
    );
    assert!(
        policy
            .excludes(
                fresh,
                0,
                b"changed bytes",
                &[event(fresh, "different-native-id", [9; 32])]
            )
            .unwrap()
    );
    assert!(
        !policy
            .excludes(
                fresh,
                0,
                b"unrelated bytes",
                &[event(fresh, "different-native-id", [88; 32])]
            )
            .unwrap()
    );
    let opened = crate::retained::open(&mut conn, &blobs, "s", material, Some(&authority))
        .await
        .unwrap();
    assert!(opened.body.is_none());
    let backing = opened.records();
    assert!(backing.bodyless);
    assert!(backing.bytes.read_exact_at(0, &mut [0; 1]).is_err());
    assert!(opened.transitions(0).unwrap().is_some());
    let restarted = AcquisitionPolicy::load(&mut conn, &blobs, "s", Some(&authority))
        .await
        .unwrap();
    assert!(restarted.excludes(fresh, 0, raw, &[]).unwrap());
    sqlx::query("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'excise','synthetic','{\"sources\":[\"s\"]}')").bind(Uuid::now_v7()).execute(&mut conn).await.unwrap();
    assert!(policy.revalidate(&mut conn).await.is_err());
    assert!(
        AcquisitionPolicy::load(&mut conn, &blobs, "s", Some(&authority))
            .await
            .is_err(),
        "missing legacy domain evidence refuses"
    );
    authority.revoke();
    assert!(restarted.excludes(fresh, 0, raw, &[]).is_err());
}
